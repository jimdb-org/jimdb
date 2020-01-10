// Copyright 2019 The JimDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

#include "range.h"

#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

#include "base/fs_util.h"
#include "common/server_config.h"

#include "master/client.h"
#include "server/run_status.h"
#include "storage/meta_store.h"

#include "range_logger.h"

namespace jimkv {
namespace ds {
namespace range {

static const int kDownPeerThresholdSecs = 50;

Range::Range(const RangeOptions& opt, RangeContext* context, const basepb::Range &meta) :
    opt_(opt),
    context_(context),
    node_id_(context_->GetNodeID()),
    id_(meta.id()),
    start_key_(meta.start_key()),
    meta_(meta, context_->GetTimerQueue()) {
}

Range::~Range() {
}

std::string Range::raftLogPath(uint64_t table_id, uint64_t range_id) {
    return JoinFilePath({std::string(ds_config.raft_config.log_path),
                         std::to_string(table_id),
                         std::to_string(range_id)});
}

// start raft
Status Range::startRaft(uint64_t leader) {
    assert(store_ != nullptr);

    raft::RaftOptions options;
    options.id = id_;
    options.leader = leader;
    options.applied = store_->PersistApplied();
    options.statemachine = shared_from_this();
    if (ds_config.raft_config.in_memory_log) {
        options.use_memory_storage = true;
    }
    options.log_file_size = ds_config.raft_config.log_file_size;
    options.max_log_files = ds_config.raft_config.max_log_files;
    options.allow_log_corrupt = ds_config.raft_config.allow_log_corrupt > 0;
    options.storage_path = raftLogPath(meta_.GetTableID(), id_);

    // init raft peers
    auto peers = meta_.GetAllPeers();
    if (peers.empty()) {
        return Status(Status::kInvalidArgument, "invalid peer size", "0");
    }
    for (const auto&peer : peers) {
        raft::Peer raft_peer;
        raft_peer.type = peer.type() == basepb::PeerType_Learner ? raft::PeerType::kLearner
            : raft::PeerType::kNormal;
        raft_peer.node_id = peer.node_id();
        raft_peer.peer_id = peer.id();
        options.peers.push_back(raft_peer);
    }

    if (leader != 0) {
        options.term = 1;
    }

    // create raft group
    auto s = context_->RaftServer()->CreateRaft(options, &raft_);
    if (!s.ok()) {
        return Status(Status::kInvalidArgument, "create raft", s.ToString());
    }

    return Status::OK();
}

Status Range::Initialize(std::unique_ptr<db::DB> db, uint64_t leader) {
    // create store
    assert(store_ == nullptr);
    if (db == nullptr) {
        auto s = context_->DBManager()->CreateDB(id_, start_key_, meta_.GetEndKey(), db);
        if (!s.ok()) {
            RLOG_ERROR("open db failed: {}", s.ToString());
            return s;
        }
    }
    assert(db != nullptr);
    store_.reset(new storage::Store(meta_.Get(), std::move(db)));
    apply_index_ = store_->PersistApplied();

    auto s = startRaft(leader);
    if (!s.ok()) {
        RLOG_ERROR("start raft failed: {}", s.ToString());
        return s;
    }

    recovering_ = false;

    if (leader == node_id_) {
        is_leader_ = true;
        context_->Statistics()->ReportLeader(id_, true);
        scheduleHeartbeat(false);
    }

    return Status::OK();
}

Status Range::Shutdown() {
    valid_ = false;

    context_->Statistics()->ReportLeader(id_, false);

    auto s = context_->RaftServer()->RemoveRaft(id_);
    if (!s.ok()) {
        RLOG_WARN("remove raft failed: {}", s.ToString());
    }
    raft_.reset();

    clearExpiredContext();
    return Status::OK();
}

void Range::Dispatch(RPCRequestPtr rpc, dspb::RangeRequest& request) {
    // common verify
    ErrorPtr err;
    do {
        if (!verifyEpoch(request.header().range_epoch(), &err)) {
            break;
        }
        if (!verifyLeader(&err)) {
            break;
        }
    } while (false);

    if (err != nullptr) {
        RLOG_ERROR("Dispacth request error, msgid: {}, type: {}, err: {}",
                rpc->MsgID(), request.req_case(), err->ShortDebugString());

        dspb::RangeResponse resp;
        sendResponse(rpc, request.header(), resp, std::move(err));
        return;
    }

    // dispatch
    switch (request.req_case()) {
        case dspb::RangeRequest::kPrepare:
            txnPrepare(std::move(rpc), request);
            break;
        case dspb::RangeRequest::kDecide:
            txnDecide(std::move(rpc), request);
            break;
        case dspb::RangeRequest::kClearUp:
            txnClearup(std::move(rpc), request);
            break;
        case dspb::RangeRequest::kGetLockInfo:
            txnGetLockInfo(std::move(rpc), request);
            break;
        case dspb::RangeRequest::kSelect:
            txnSelect(std::move(rpc), request);
            break;
        case dspb::RangeRequest::kScan:
            txnScan(std::move(rpc), request);
            break;
        case dspb::RangeRequest::kSelectFlow:
            txnSelectFlow(std::move(rpc), request);
            break;

        // kv
        case dspb::RangeRequest::kKvGet:
            kvGet(std::move(rpc), request);
            break;
        case dspb::RangeRequest::kKvPut:
            kvPut(std::move(rpc), request);
            break;
        case dspb::RangeRequest::kKvDelete:
            kvDelete(std::move(rpc), request);
            break;
        case dspb::RangeRequest::kKvDelRange:
            kvDelRange(std::move(rpc), request);
            break;
        case dspb::RangeRequest::kKvScan:
            kvScan(std::move(rpc), request);
            break;

        // index statistic 
        case dspb::RangeRequest::kIndexStats:
            idxStatsGet(std::move(rpc), request);
            break;
        // cols statistic 
        case dspb::RangeRequest::kColumnsStats:
            colsStatsGet(std::move(rpc), request);
            break;

        default: {
            err.reset(new dspb::Error);
            err->mutable_server_error()->set_msg("unsupported range request type : " +
                                                 std::to_string(request.req_case()));
            dspb::RangeResponse resp;
            sendResponse(rpc, request.header(), resp, std::move(err));
            break;
        }
    }
}

void Range::sendResponse(RPCRequestPtr& rpc, const dspb::RangeRequest_Header& req_header,
                  dspb::RangeResponse& resp, ErrorPtr err) {
    auto taken_usec = NowMicros() - rpc->begin_time;
    if (taken_usec > opt_.request_warn_usecs) {
        RLOG_WARN("{} takes too long({} ms), from={}, msgid={}",
                  rpc->FuncName(), taken_usec / 1000, rpc->ctx.remote_addr, rpc->MsgID());
    }
    SetResponseHeader(resp.mutable_header(), req_header, std::move(err));
    rpc->Reply(resp);
}

uint64_t Range::PersistApplied() {
    return store_->PersistApplied();
}

class HeartbeatTimer : public Timer {
public:
    explicit HeartbeatTimer(const std::shared_ptr<Range>& r) : wp_rng_(r) {}

    void OnTimeout() override {
        auto r = wp_rng_.lock();
        if (r) {
            r->doHeartbeat();
            r->clearExpiredContext();
        }
    }

private:
    std::weak_ptr<Range> wp_rng_;
};

void Range::scheduleHeartbeat(bool delay) {
    if (!is_leader_) {
        return;
    }

    std::shared_ptr<HeartbeatTimer> timer(new HeartbeatTimer(shared_from_this()));
    if (delay) {
        context_->GetTimerQueue()->Push(timer, opt_.heartbeat_interval_msec);
    } else {
        context_->GetTimerQueue()->Push(timer, 0);
    }
}

void Range::doHeartbeat() {
    if (!is_leader_ || !valid_ || raft_ == nullptr || raft_->IsStopped()) {
        return;
    }

    RLOG_DEBUG("Range heartbeat. epoch[{} : {}], key range[{} - {}]",
            meta_.GetVersion(), meta_.GetConfVer(), EncodeToHex(start_key_),
            EncodeToHex(meta_.GetEndKey()));

    mspb::RangeHeartbeatRequest req;

    meta_.Get(req.mutable_range());

    raft::RaftStatus rs;
    raft_->GetStatus(&rs);
    if (rs.leader != node_id_) {
        RLOG_ERROR("heartbeat raft say not leader, leader={}", rs.leader);
        return;
    }
    req.set_term(rs.term);
    req.mutable_range()->set_leader(node_id_);

    auto leader_applied = apply_index_;
    int leader_index = 0;
    for (const auto &pr : rs.replicas) {
        auto peer_status = req.add_peers_status();
        if (pr.first == node_id_) {
            leader_index = req.peers_status_size() - 1;
        }
        RLOG_DEBUG("Range node_id:{} raft replicas node_id:{} ReplicaStatus:{} "
            , node_id_, pr.first, pr.second.ToString());
        auto peer = peer_status->mutable_peer();
        if (!meta_.FindPeerByNodeID(pr.first, peer)) {
            RLOG_ERROR("heartbeat not found peer: {}", pr.first);
            continue;
        }

        peer_status->set_index(pr.second.match);
        peer_status->set_commit(pr.second.commit);
        peer_status->set_applied(std::min(pr.second.commit, leader_applied));
        if (pr.second.inactive_seconds > kDownPeerThresholdSecs) {
            peer_status->set_down_seconds(
                    static_cast<uint64_t>(pr.second.inactive_seconds));
        }
        peer_status->set_snapshotting(pr.second.snapshotting);
    }

    if (leader_index != 0) {
        req.mutable_peers_status()->SwapElements(0, leader_index);
    }

    mspb::RangeHeartbeatResponse resp;
    auto s = context_->MasterClient()->RangeHeartbeat(req, resp);
    if (!s.ok()) {
        RLOG_ERROR("heartbeat error: {}", s.ToString());
    }

    // schedule next heartbeat
    if (is_leader_) {
        scheduleHeartbeat(true);
    }
}

void Range::GetRangeInfo(dspb::RangeInfo *rangeInfo) {
    meta_.Get(rangeInfo->mutable_range());

    raft::RaftStatus rs;
    raft_->GetStatus(&rs);
    rangeInfo->mutable_range()->set_leader(rs.leader);
    rangeInfo->set_term(rs.term);

    // metric stats
    auto stats = rangeInfo->mutable_stats();
    stats->set_approximate_size(real_size_);
    stats->set_kv_count(kv_count_);

    storage::MetricStat store_stat;
    store_->CollectMetric(&store_stat);
    stats->set_read_keys_per_sec(store_stat.keys_read_per_sec);
    stats->set_read_bytess_per_sec(store_stat.bytes_read_per_sec);
    stats->set_write_keys_per_sec(store_stat.keys_write_per_sec);
    stats->set_write_bytes_per_sec(store_stat.bytes_write_per_sec);

    if (rs.leader != node_id_) {
        return;
    }

    auto leader_applied = apply_index_;
    int leader_index = 0;
    for (const auto &pr : rs.replicas) {
        auto peer_status = rangeInfo->add_peers_status();
        if (pr.first == node_id_) {
            leader_index = rangeInfo->peers_status_size() - 1;
        }
        RLOG_DEBUG("Range node_id:{} raft replicas node_id:{} ReplicaStatus:{} "
            , node_id_, pr.first, pr.second.ToString());
        auto peer = peer_status->mutable_peer();
        if (!meta_.FindPeerByNodeID(pr.first, peer)) {
            RLOG_ERROR("heartbeat not found peer: {}", pr.first);
            continue;
        }

        peer_status->set_index(pr.second.match);
        peer_status->set_commit(pr.second.commit);
        peer_status->set_applied(std::min(pr.second.commit, leader_applied));
        if (pr.second.inactive_seconds > kDownPeerThresholdSecs) {
            peer_status->set_down_seconds(
                    static_cast<uint64_t>(pr.second.inactive_seconds));
        }
        peer_status->set_snapshotting(pr.second.snapshotting);
    }

    if (leader_index != 0) {
        rangeInfo->mutable_peers_status()->SwapElements(0, leader_index);
    }
}

Status Range::apply(const dspb::Command &cmd, uint64_t index) {
    if (!hasSpaceLeft(LimitType::kHard)) {
        return Status(Status::kIOError, "no left space", "apply");
    }

    switch (cmd.cmd_type()) {
        case dspb::CmdType::KvPut:
            return applyKvPut(cmd, index);
        case dspb::CmdType::KvDelete:
            return applyKvDelete(cmd, index);
        case dspb::CmdType::KvDelRange:
            return applyKvDelRange(cmd, index);
        case dspb::CmdType::TxnPrepare:
            return applyTxnPrepare(cmd, index);
        case dspb::CmdType::TxnDecide:
            return applyTxnDecide(cmd, index);
        case dspb::CmdType::TxnClearup:
            return applyTxnClearup(cmd, index);
        default:
            RLOG_ERROR("Apply cmd type error {}", CmdType_Name(cmd.cmd_type()));
            return Status(Status::kNotSupported, "cmd type not supported", "");
    }
}

Status Range::ApplyReadIndex(const std::string& cmd, uint16_t verify_result) {
    ErrorPtr err = nullptr;
    dspb::RangeResponse resp;
    if (!valid_) {
        RLOG_ERROR("is invalid!");
        return Status(Status::kInvalid, "range is invalid", "");
    }
    auto start = std::chrono::system_clock::now();

    dspb::Command raft_cmd;
    google::protobuf::io::ArrayInputStream input(cmd.data(), static_cast<int>(cmd.size()));
    if(!raft_cmd.ParseFromZeroCopyStream(&input)) {
        RLOG_ERROR("parse raft command failed");
        return Status(Status::kCorruption, "parse raft command", EncodeToHex(cmd.data()));
    }

    if (verify_result == jimkv::raft::READ_FAILURE) {
        err = noLeaderError();
    }

    switch (raft_cmd.cmd_type()) {
    case dspb::CmdType::KvGet:
    {
        RLOG_DEBUG("kv get key: {}", raft_cmd.kv_get_req().key());
        auto kv_resp = resp.mutable_kv_get();
        auto ret = store_->Get(raft_cmd.kv_get_req().key(), kv_resp->mutable_value());
        kv_resp->set_code(static_cast<int>(ret.code()));
        auto btime = NowMicros();
        replySubmit(raft_cmd, resp, std::move(err), btime);
    }
    break;
    case dspb::CmdType::TxnSelect:
    {
        RLOG_DEBUG("select {}", raft_cmd.txn_select_req().DebugString());
        auto select_resp = resp.mutable_select();
        auto ret = store_->TxnSelect(raft_cmd.txn_select_req(), select_resp);
        if (!ret.ok()) {
            RLOG_ERROR("TxnSelect from store error: {}", ret.ToString());
            select_resp->set_code(static_cast<int>(ret.code()));
        }
        auto btime = NowMicros();
        replySubmit(raft_cmd, resp, std::move(err), btime);
    }
    break;
    case dspb::CmdType::TxnScan:
    {
        auto scan_resp = resp.mutable_scan();
        auto ret = store_->TxnScan(raft_cmd.txn_scan_req(), scan_resp);

        if (!ret.ok()) {
            RLOG_ERROR("TxnScan from store error: {}", ret.ToString());
            scan_resp->set_code(static_cast<int>(ret.code()));
            break;
        }

        auto btime = NowMicros();
        replySubmit(raft_cmd, resp, std::move(err), btime);
    }
    break;
    case dspb::CmdType::TxnSelectFlow:
    {
        auto select_flow_resp = resp.mutable_select_flow();
        auto ret = store_->TxnSelectFlow(raft_cmd.txn_select_flow_req(), select_flow_resp);

        if (!ret.ok()) {
            RLOG_ERROR("TxnSelectFlow from store error: {}", ret.ToString());
            select_flow_resp->set_code(static_cast<int>(ret.code()));
            break;
        }

        auto btime = NowMicros();
        replySubmit(raft_cmd, resp, std::move(err), btime);
    }
    break;
    case dspb::CmdType::IdxStatsGet:
    {
        auto ret = store_->IdxStatsGet(raft_cmd.idx_stats_req(), *(resp.mutable_idx_stats()));

        if (!ret.ok()) {
            RLOG_ERROR("index stat get from store error: {}", ret.ToString());
            break;
        }

        auto btime = NowMicros();
        replySubmit(raft_cmd, resp, std::move(err), btime);
    }
    break;
    case dspb::CmdType::ColsStatsGet:
        {
            auto ret = store_->ColsStatsGet(raft_cmd.cols_stats_req(), *(resp.mutable_cols_stats()));

            if (!ret.ok()) {
                RLOG_ERROR("cols stat get from store error: {}", ret.ToString());
                break;
            }

            auto btime = NowMicros();
            replySubmit(raft_cmd, resp, std::move(err), btime);
        }
        break;
    default:
        RLOG_DEBUG("xxxxxxxxxxxxxxxxxxxx");
    }

    auto end = std::chrono::system_clock::now();
    auto elapsed_usec =
        std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    if (elapsed_usec > opt_.request_warn_usecs) {
        RLOG_WARN("apply takes too long({} ms), type: {}.", elapsed_usec / 1000,
                dspb::CmdType_Name(raft_cmd.cmd_type()));
    }

    return Status::OK();
}

Status Range::Apply(const std::string &cmd, uint64_t index) {
    if (!valid_) {
        RLOG_ERROR("is invalid!");
        return Status(Status::kInvalid, "range is invalid", "");
    }

    auto start = std::chrono::system_clock::now();

    dspb::Command raft_cmd;
    google::protobuf::io::ArrayInputStream input(cmd.data(), static_cast<int>(cmd.size()));
    if(!raft_cmd.ParseFromZeroCopyStream(&input)) {
        RLOG_ERROR("parse raft command failed");
        return Status(Status::kCorruption, "parse raft command", EncodeToHex(cmd.data()));
    }

    Status ret;
    if (raft_cmd.cmd_type() == dspb::CmdType::AdminSplit) {
        ret = applySplit(raft_cmd, index);
    } else if (raft_cmd.cmd_type() == dspb::CmdType::UpdateStats) {
        ret = applyUpdateStats(raft_cmd, index);
    } else {
        ret = apply(raft_cmd, index);
        if (!ret.ok() && ret.code() != Status::kIOError) {
            ret = Status::OK();
        }
    }
    if (!ret.ok()) {
        return ret;
    }

    apply_index_ = index;

    auto end = std::chrono::system_clock::now();
    auto elapsed_usec =
        std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    if (elapsed_usec > opt_.request_warn_usecs) {
        RLOG_WARN("apply takes too long({} ms), type: {}.", elapsed_usec / 1000,
                dspb::CmdType_Name(raft_cmd.cmd_type()));
    }

    return Status::OK();
}

Status Range::submit(const dspb::Command &cmd, uint16_t rw_flag) {
    if (!ds_config.raft_config.disabled) {
        if (!is_leader_) {
            return Status(Status::kNotLeader, "Not Leader", "");
        }
        auto str_cmd = cmd.SerializeAsString();
        if (rw_flag == jimkv::raft::WRITE_FLAG) {
            return raft_->Propose(str_cmd, cmd.cmd_flags());
        } else {
            return raft_->ReadIndex(str_cmd);
        }
    } else {
        static std::atomic<uint64_t> fake_index = {0};
        apply(cmd, ++fake_index);
        return Status::OK();
    }
}

void Range::submitCmd(RPCRequestPtr rpc, dspb::RangeRequest_Header& header, uint16_t rw_flag,
               const std::function<void(dspb::Command &cmd)> &init) {
    dspb::Command cmd;
    init(cmd);
    // set verify epoch
    cmd.set_allocated_verify_epoch(header.release_range_epoch());
    // add to queue
    // todo exsist conflict when application restart
    auto seq = submit_queue_.Add(std::move(rpc), cmd.cmd_type(), std::move(header));
    cmd.mutable_cmd_id()->set_node_id(node_id_);
    cmd.mutable_cmd_id()->set_seq(seq);
    auto ret = submit(cmd, rw_flag);
    if (!ret.ok()) {
        RLOG_ERROR("raft submit failed: {}", ret.ToString());
        auto ctx = submit_queue_.Remove(seq);
        if (ctx != nullptr) {
            ctx->SendError(raftFailError());
        }
    }
    // check if range is in destroying
    if (!valid_) {
        auto ctx = submit_queue_.Remove(seq);
        if (ctx != nullptr) {
            ctx->SendError(newRangeNotFoundErr(id_));
        }
    }
}

void Range::replySubmit(const dspb::Command& cmd, dspb::RangeResponse& resp, ErrorPtr err, int64_t apply_time) {
    auto ctx = submit_queue_.Remove(cmd.cmd_id().seq());
    if (ctx != nullptr) {
        context_->Statistics()->PushTime(HistogramType::kRaft, apply_time - ctx->SubmitTime());
        ctx->CheckExecuteTime(id_, opt_.request_warn_usecs);
        ctx->SendResponse(resp, std::move(err));
    } else {
        if (!recovering_) {
            RLOG_WARN("Apply cmd id {} not found", cmd.cmd_id().seq());
        }
    }
}

void Range::OnReplicateError(const std::string &cmd, const Status &status) {
    assert(!status.ok());

    dspb::Command raft_cmd;
    google::protobuf::io::ArrayInputStream input(cmd.data(), static_cast<int>(cmd.size()));
    if(!raft_cmd.ParseFromZeroCopyStream(&input)) {
        RLOG_ERROR("parse raft command failed");
        return;
    }

    if (raft_cmd.cmd_id().seq() == kPassbySubmitQueueCmdSeq || raft_cmd.cmd_id().node_id() != node_id_) {
        return;
    }

    auto ctx = submit_queue_.Remove(raft_cmd.cmd_id().seq());
    if (!ctx) {
        return;
    }

    RLOG_WARN("raft replicate {} {} failed: {}, msgid: {}", raft_cmd.cmd_id().ShortDebugString(),
              dspb::CmdType_Name(raft_cmd.cmd_type()), status.ToString(), ctx->MsgID());

    ErrorPtr err;
    if (status.code() == Status::kNotLeader) {
        uint64_t leader, term;
        raft_->GetLeaderTerm(&leader, &term);
        basepb::Peer peer;
        if (leader == 0 || !meta_.FindPeerByNodeID(leader, &peer)) {
            err = noLeaderError();
        } else {
            err = notLeaderError(std::move(peer));
        }
    } else {
        err = raftFailError();
    }
    ctx->SendError(std::move(err));
}

void Range::OnLeaderChange(uint64_t leader, uint64_t term) {
    RLOG_INFO("Leader Change to Node {}", leader);

    if (!valid_) {
        RLOG_ERROR("is invalid!");
        return;
    }

    bool prev_is_leader = is_leader_;
    is_leader_ = (leader == node_id_);
    if (is_leader_) {
        if (!prev_is_leader) {
            store_->ResetMetric();
        }
        scheduleHeartbeat(false);
    }
    context_->Statistics()->ReportLeader(id_, is_leader_.load());
}

std::shared_ptr<raft::Snapshot> Range::GetSnapshot() {
    dspb::SnapshotContext ctx;
    meta_.Get(ctx.mutable_meta());
    std::string ctx_str;
    if (!ctx.SerializeToString(&ctx_str)) {
        RLOG_ERROR("serialize snapshot context failed!");
        return nullptr;
    }

    std::shared_ptr<raft::Snapshot> snapshot;
    auto s = store_->GetSnapshot(apply_index_, std::move(ctx_str), &snapshot);
    if (!s.ok()) {
        RLOG_ERROR("get snapshot failed: {}", s.ToString());
        return nullptr;
    }
    return snapshot;
}

Status Range::ApplySnapshotStart(const std::string &context, uint64_t raft_index) {
    RLOG_INFO("apply snapshot begin");

    if (!valid_) {
        RLOG_ERROR("is invalid!");
        return Status(Status::kInvalid, "range is invalid", "");
    }

    apply_index_ = 0;

    auto s = store_->ApplySnapshotStart(raft_index);
    if (!s.ok()) {
        return s;
    }

    dspb::SnapshotContext ctx;
    if (!ctx.ParseFromString(context)) {
        return Status(Status::kCorruption, "parse snapshot context", "pb return false");
    }

    meta_.Set(ctx.meta());
    s = saveMeta(ctx.meta()) ;
    if (!s.ok()) {
        RLOG_ERROR("save snapshot meta failed: {}", s.ToString());
        return Status(Status::kIOError, "save range meta", "");
    }

    RLOG_INFO("meta update to {}", ctx.meta().ShortDebugString());

    return Status::OK();
}

Status Range::ApplySnapshotData(const std::vector<std::string> &datas) {
    if (!valid_) {
        RLOG_ERROR("is invalid!");
        return Status(Status::kInvalid, "range is invalid", "");
    }

    if (!hasSpaceLeft(LimitType::kHard)) {
        RLOG_ERROR("apply snapshot failed: no left space");
        return Status(Status::kNoLeftSpace);
    }

    return store_->ApplySnapshotData(datas);
}

Status Range::ApplySnapshotFinish(uint64_t index) {
    RLOG_INFO("finish apply snapshot. index: {}", index);

    if (!valid_) {
        RLOG_ERROR("is invalid!");
        return Status(Status::kInvalid, "range is invalid", "");
    }
    apply_index_ = index;
    return store_->ApplySnapshotFinish(index);
}

Status Range::saveMeta(const basepb::Range &meta) {
    return context_->MetaStore()->AddRange(meta);
}

Status Range::Destroy() {
    valid_ = false;

    // reply error to all pending requests
    auto pending_ctxs = submit_queue_.ClearAll();
    for (auto& ctx: pending_ctxs) {
        if (ctx) {
            ctx->SendError(newRangeNotFoundErr(id_));
        }
    }

    context_->Statistics()->ReportLeader(id_, false);

    auto s = context_->RaftServer()->DestroyRaft(id_, false);
    if (!s.ok()) {
        RLOG_WARN("destroy raft failed: {}", s.ToString());
    }
    raft_.reset();

    s = store_->Destroy();
    if (!s.ok()) {
        RLOG_ERROR("destroy store fail: {}", s.ToString());
        return s;
    }
    return s;
}

void Range::TransferLeader() {
    if (!valid_) {
        RLOG_ERROR("is invalid!");
        return;
    }

    RLOG_INFO("receive TransferLeader, try to leader.");

    auto s = raft_->TryToLeader();
    if (!s.ok()) {
        RLOG_ERROR("TransferLeader fail, {}", s.ToString());
    }
}

void Range::GetPeerInfo(raft::RaftStatus *raft_status) {
    raft_->GetStatus(raft_status);
}

void Range::GetReplica(basepb::Replica *rep) {
    rep->set_range_id(id_);
    rep->set_start_key(start_key_);
    rep->set_end_key(meta_.GetEndKey());
    auto peer = new basepb::Peer;
    meta_.FindPeerByNodeID(node_id_, peer);
    rep->set_allocated_peer(peer);
}

static ErrorPtr timeoutError() {
    ErrorPtr err(new dspb::Error);
    err->mutable_server_error()->set_msg("request timeout");
    return err;
}

void Range::clearExpiredContext() {
    submit_queue_.Shrink();

    auto expired_seqs = submit_queue_.GetExpired();
    for (auto seq: expired_seqs) {
        auto ctx = submit_queue_.Remove(seq);
        if (ctx) {
            ctx->SendError(timeoutError());
        }
    }
}

}  // namespace range
}  // namespace ds
}  // namespace jimkv
