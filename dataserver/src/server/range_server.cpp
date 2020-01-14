// Copyright 2019 The JIMDB Authors.
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

#include "range_server.h"

#include <chrono>
#include <future>
#include <thread>

#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <common/server_config.h>

#include "base/fs_util.h"

#include "common/ds_encoding.h"
#include "common/logger.h"
#include "common/server_config.h"
#include "common/masstree_env.h"
#include "common/statistics.h"

#include "dspb/api.pb.h"
#include "basepb/basepb.pb.h"
#include "dspb/function.pb.h"

#include "range_context_impl.h"
#include "server.h"

namespace jim {
namespace ds {
namespace server {

using namespace jim::ds::range;

static RangeOptions makeRangeOptions(basepb::RangeType range_type) {
    RangeOptions opt;
    opt.enable_split = ds_config.range_config.enable_split;
    opt.check_size = ds_config.range_config.check_size;
    opt.split_size = ds_config.range_config.split_size;
    opt.max_size = ds_config.range_config.max_size;
    if (ds_config.b_test) {
        opt.heartbeat_interval_msec = 1000*10000UL;
    } else {
        opt.heartbeat_interval_msec = ds_config.cluster_config.range_interval_secs * 1000;
    }
    if (range_type == basepb::RNG_Index) {
        opt.check_size /= ds_config.range_config.index_split_ratio;
        opt.split_size /= ds_config.range_config.index_split_ratio;
        opt.max_size /= ds_config.range_config.index_split_ratio;
    }
    return opt;
}

Status RangeServer::createTestRange(ContextServer *context) {
    if (ds_config.test_config.nodes.size() == 0) {
        FLOG_WARN("there is no raft peers, please check");
        return Status::OK();
    }
    std::string raft_str = ds_config.test_config.nodes[context->node_id];
    if (raft_str.empty()) {
        return Status(Status::kInvalidArgument, "no raft peer in test config", "");
    }
    basepb::Range rng;
    rng.set_start_key(ds_config.test_config.start_key);
    rng.set_end_key(ds_config.test_config.end_key);
    rng.mutable_range_epoch()->set_conf_ver(ds_config.test_config.conf_id);
    rng.mutable_range_epoch()->set_version(ds_config.test_config.version);
    rng.set_id(ds_config.test_config.range_id);
    for (auto it = ds_config.test_config.nodes.begin(); it != ds_config.test_config.nodes.end(); it++) {
        auto peer = rng.add_peers();
        peer->set_id(it->first);
        peer->set_node_id(it->first);
        peer->set_type(basepb::PeerType::PeerType_Normal);
    }
    auto key_schema = rng.mutable_key_schema();
    auto key_cols = key_schema->add_key_cols();
    key_cols->set_id(0);
    key_cols->set_type(basepb::Varchar);
    key_cols->set_unsigned_(false);

    std::shared_ptr<range::Range> existed;
    Status ret = createRange(rng, 0, existed);
    if (!ret.ok()) {
        FLOG_ERROR("node_id: {} create range: {} failed, error: {}", context_->node_id, rng.id(), ret.ToString());
        return ret;
    }
    return Status::OK();
}

int RangeServer::Init(ContextServer *context) {
    FLOG_INFO("RangeServer Init begin ...");

    context_ = context;

    // open meta db
    meta_store_ = new storage::MetaStore(ds_config.meta_path);
    auto ret = meta_store_->Open();
    if (!ret.ok()) {
        FLOG_ERROR("open meta store failed({}), path={}", ret.ToString(), ds_config.meta_path);
        return -1;
    }
    assert(context_->node_id != 0);
    ret = meta_store_->SaveNodeID(context_->node_id);
    if (!ret.ok()) {
        FLOG_ERROR("save node id to meta failed({})", ret.ToString());
        return -1;
    } else {
        FLOG_DEBUG("save node_id ({}) to meta.", context_->node_id);
    }
    context_->meta_store = meta_store_;

    if (openDB() != 0) {
        FLOG_ERROR("RangeServer Init error ...");
        return -1;
    }
    context_->db_manager = db_manager_;

    timer_queue_.reset(new TimerQueue("range_hb"));
    range_context_.reset(new RangeContextImpl(context_, timer_queue_.get()));

    std::vector<basepb::Range> range_metas;
    ret = meta_store_->GetAllRange(&range_metas);
    if (!ret.ok()) {
        FLOG_ERROR("load range metas failed({})", ret.ToString());
        return -1;
    }
    if (recover(range_metas) != 0) {
        FLOG_ERROR("load local range meta failed");
        return -1;
    }

    // create test ranges if run in test mode
    if (ds_config.b_test && ranges_.Size() == 0) {
        ret = createTestRange(context);
        if (!ret.ok()) {
            FLOG_ERROR("create test ranges failed: {}", ret.ToString());
            return  -1;
        }
    }

    FLOG_INFO("RangeServer Init end ...");

    return 0;
}

int RangeServer::Start() {
    FLOG_INFO("RangeServer Start begin ...");

    char name[32] = {'\0'};
    for (int i = 0; i < ds_config.range_config.worker_threads; i++) {
        worker_.emplace_back([this] {
            uint64_t range_id = 0;
            while (running_) {
                {
                    std::unique_lock<std::mutex> lock(statis_mutex_);
                    if (statis_queue_.empty()) {
                        statis_cond_.wait_for(lock, std::chrono::seconds(5));
                        continue;
                    }

                    range_id = statis_queue_.front();
                    statis_queue_.pop();
                }

                auto range = Find(range_id);
                if (range == nullptr) {
                    FLOG_ERROR("RawPut request not found range_id {} failed", range_id);
                    continue;
                }
                range->ResetStatisSize();
            }

            FLOG_INFO("StatisSize worker thread exit...");
        });

        auto handle = worker_[i].native_handle();
        snprintf(name, 32, "statis:%d", i);
        AnnotateThread(handle, name);
    }

    FLOG_INFO("RangeServer Start end ...");
    return 0;
}

void RangeServer::Stop() {
    FLOG_INFO("RangeServer Stop begin ...");

    running_ = false;
    statis_cond_.notify_all();

    for (auto &work : worker_) {
        if (work.joinable()) {
            work.join();
        }
    }
    auto all_ranges = ranges_.GetAll();
    for (auto rng: all_ranges) {
        rng->Shutdown();
    }

    closeDB();

    if (meta_store_ != nullptr) {
        delete meta_store_;
        meta_store_ = nullptr;
    }

    FLOG_INFO("RangeServer Stop end ...");
}

int RangeServer::openDB() {
    std::unique_ptr<db::DBManager> manager;
    auto ret = db::NewDBManager(manager);
    if (!ret.ok()) {
        FLOG_ERROR("OpenDB failed: {}", ret.ToString());
        return -1;
    } else {
        assert(db_manager_ == nullptr);
        db_manager_ = manager.release();
        return 0;
    }
}

void RangeServer::closeDB() {
    if (db_manager_ != nullptr) {
        delete db_manager_;
        db_manager_ = nullptr;
    }
}

void RangeServer::Clear() {
    FLOG_WARN("clear range data!");

    RemoveDirAll(ds_config.raft_config.log_path.c_str());
    RemoveDirAll(ds_config.rocksdb_config.path.c_str());
}

void RangeServer::DealTask(RPCRequestPtr rpc) {
    context_->run_status->PushTime(HistogramType::kQWait, NowMicros() - rpc->begin_time);

    const auto &header = rpc->msg->head;

    FLOG_DEBUG("server start deal {} task from {}, msgid={}",
               dspb::FunctionID_Name(static_cast<dspb::FunctionID>(header.func_id)),
               rpc->ctx.remote_addr, rpc->MsgID());

    if (rpc->range_req) {
        forwardToRange(rpc);
    } else if (rpc->sch_req) {
        dispatchSchedule(rpc);
    } else {
        FLOG_WARN("unknown func id: {} from {}, msgid: {}", header.func_id, rpc->ctx.remote_addr, rpc->MsgID());
    }
}

size_t RangeServer::GetRangesCount() const {
    return ranges_.Size();
}

std::shared_ptr<range::Range> RangeServer::Find(uint64_t range_id) {
    auto rng = ranges_.Find(range_id);
    if (rng == nullptr) {
        return nullptr;
    }

    if (rng->Valid()) {
        return rng;
    } else {
        FLOG_WARN("RangeHeartbeat range_id {} is invalid", range_id);
        return nullptr;
    }
}

Status RangeServer::SplitRange(uint64_t old_range_id, const dspb::SplitCommand& req,
                               uint64_t raft_index) {
    auto rng = Find(old_range_id);
    if (rng == nullptr) {
        return Status(Status::kNotFound, "range not found", "");
    }

    // insert new range
    RangePtr existed_rng;
    auto ret = ranges_.Insert(req.new_range().id(), [=](RangePtr& new_range) {
        return rng->Split(req, raft_index, new_range);
    }, existed_rng);

    bool is_exist = false;
    if (ret.code() == Status::kExisted) {
        FLOG_WARN("range[{}] ApplySplit(new range: {}) already exist.",
                  old_range_id, req.new_range().id());
        ret = Status::OK();
        is_exist = true;
    } else if (!ret.ok()) {
        return ret;
    }

    basepb::Range meta = rng->GetMeta();
    meta.set_end_key(req.split_key());
    meta.mutable_range_epoch()->set_version(req.epoch().version());

    std::vector<basepb::Range> batch_ranges{meta};
    if (!is_exist) {
        batch_ranges.push_back(req.new_range());
    }
    ret = meta_store_->BatchAddRange(batch_ranges);
    if (!ret.ok()) {
        if (!is_exist) {
            deleteRange(req.new_range().id());
        }
    }
    return ret;
}

Status RangeServer::recover(const basepb::Range &meta) {
    auto rng = std::make_shared<range::Range>(makeRangeOptions(meta.range_type()), range_context_.get(), meta);
    auto s = rng->Initialize();
    if (!s.ok()) {
        return s;
    }

    std::string str_stats;
    s = meta_store_->LoadRangeStats(meta.id(), str_stats);
    if (s.ok()) {

        dspb::UpdateStatsCommand cmd;
        google::protobuf::io::ArrayInputStream input(str_stats.data(), static_cast<int>(str_stats.size()));
        if(!cmd.ParseFromZeroCopyStream(&input)) {
            FLOG_WARN("recover parse raft stats command failed, range_id: {}", meta.id());
        } else {
            rng->SetKvCount(cmd.kv_count());
            rng->SetRealSize(cmd.real_size());
            FLOG_INFO("recover range_id:{} set stats info kv_count:{}, real_size:{}",
                    meta.id(), cmd.kv_count(), cmd.real_size());
        }
    } else {
        FLOG_WARN("recover range_id:{} load range stats failed", meta.id());
    }

    // insert to range tree
    RangePtr existed;
    s = ranges_.Insert(meta.id(), [=](RangePtr& new_rng) {
        new_rng = rng;
        return Status::OK();
    }, existed);

    return s;
}

int RangeServer::recover(const std::vector<basepb::Range> &metas) {
    assert(ds_config.range_config.recover_concurrency > 0);
    auto actual_concurrency = std::min(metas.size() / 4 + 1,
                                       static_cast<size_t>(ds_config.range_config.recover_concurrency));
    if (actual_concurrency > 50) {
        actual_concurrency = 50;
    }

    std::vector<std::future<Status>> recover_futures;
    std::vector<uint64_t> failed_ranges;
    std::mutex failed_mu;
    std::atomic<size_t> recover_pos = {0};
    std::atomic<size_t> success_counter = {0};

    FLOG_INFO("Start to recovery ranges. total ranges={}, concurrency={}, skip_fail={}",
              metas.size(), actual_concurrency, ds_config.range_config.recover_skip_fail);

    auto begin = std::chrono::system_clock::now();

    for (size_t i = 0; i < actual_concurrency; ++i) {
        auto f = std::async(std::launch::async, [&, this] {
            Status s;
            while (true) {
                auto pos = recover_pos.fetch_add(1);
                if (pos >= metas.size()) {
                    break;
                }
                const auto &meta = metas[pos];
                FLOG_DEBUG("Start Recover range id={}", meta.id());
                s = recover(meta);
                if (s.ok()) {
                    ++success_counter;
                } else {
                    FLOG_ERROR("Recovery range[{}] failed: {}", meta.id(), s.ToString());
                    {
                        std::lock_guard<std::mutex> lock(failed_mu);
                        failed_ranges.push_back(meta.id());
                    }
                    if (ds_config.range_config.recover_skip_fail) { // allow failed
                        continue;
                    } else {
                        recover_pos = metas.size(); // failed, let other threads exit
                        break;
                    }
                }
            }
            // free all rcu items during recover
            if (ds_config.engine_type == EngineType::kMassTree ||
                (ds_config.engine_type == EngineType::kRocksdb &&
                    ds_config.rocksdb_config.enable_txn_cache)) {
                RunRCUFree(true);
            }
            return s;
        });
        recover_futures.push_back(std::move(f));
    }

    Status last_error;
    for (auto &f : recover_futures) {
        auto s = f.get();
        if (!s.ok())
            last_error = s;
    }

    auto took_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::system_clock::now() - begin)
                       .count();

    if (!failed_ranges.empty()) {
        std::string failed_str;
        for (std::size_t i = 0; i < failed_ranges.size(); ++i)
            failed_str += std::to_string(failed_ranges[i]) + ", ";
        FLOG_ERROR("Range recovery failed ranges: [{}]", failed_str);
    }

    if (!last_error.ok()) {
        FLOG_ERROR("Range recovery abort, last status: {}. failed={}",
                   last_error.ToString(), failed_ranges.size());
        return -1;
    } else {
        FLOG_INFO("Range recovery finished. success={}, failed={}, time used={}s {}ms",
                  success_counter.load(), failed_ranges.size(), took_ms / 1000, took_ms % 1000);
        return 0;
    }
}

static void timeOut(const dspb::RangeRequest_Header& req,
                    dspb::RangeResponse_Header* resp) {
    ErrorPtr err(new dspb::Error);
    err->mutable_server_error()->set_msg("timeout");
    SetResponseHeader(resp, req, std::move(err));
}

static void rangeNotFound(const dspb::RangeRequest_Header& req,
                          dspb::RangeResponse_Header* resp) {
    ErrorPtr err(new dspb::Error);
    err->mutable_range_not_found()->set_range_id(req.range_id());
    err->set_detail("range not found");
    SetResponseHeader(resp, req, std::move(err));
}

static ErrorPtr newClusterMismatchErr(uint64_t request, uint64_t actual) {
    ErrorPtr err(new dspb::Error);
    err->mutable_cluster_mismatch()->set_request_cluster(request);
    err->mutable_cluster_mismatch()->set_actual_cluster(actual);
    return err;
}

void RangeServer::forwardToRange(RPCRequestPtr &rpc) {
    assert(rpc->range_req);

    dspb::RangeRequest& request = *(rpc->range_req);

    FLOG_DEBUG("{} from {} called. req: {}", rpc->FuncName(), rpc->ctx.remote_addr, request.DebugString());

    // check cluster id
    if (request.header().cluster_id() != 0 &&
        request.header().cluster_id() != ds_config.cluster_config.cluster_id) {
        FLOG_ERROR("mismatch cluster id {} (self: {}) from {}, msg id={}",
                request.header().cluster_id(), ds_config.cluster_config.cluster_id,
                rpc->ctx.remote_addr, rpc->MsgID());

        dspb::RangeResponse response;
        response.mutable_header()->set_trace_id(request.header().trace_id());
        auto err = newClusterMismatchErr(request.header().cluster_id(), ds_config.cluster_config.cluster_id);
        response.mutable_header()->set_allocated_error(err.release());
        rpc->Reply(response);
        return;
    }

    // check timeout
    if (rpc->expire_time != 0 && rpc->expire_time < NowMilliSeconds()) {
        FLOG_WARN("{} request timeout from {}", rpc->FuncName(), rpc->ctx.remote_addr);
        dspb::RangeResponse response;
        timeOut(request.header(), response.mutable_header());
        rpc->Reply(response);
        return;
    }

    auto range = Find(request.header().range_id());
    if (range != nullptr) {
        range->Dispatch(std::move(rpc), request);
        return;
    } else {
        FLOG_ERROR("{} request not found range_id {} failed", rpc->FuncName(), request.header().range_id());
        dspb::RangeResponse response;
        rangeNotFound(request.header(), response.mutable_header());
        rpc->Reply(response);
        return;
    }
}

static ErrorPtr newServerError(const std::string& msg) {
    ErrorPtr err(new dspb::Error);
    err->mutable_server_error()->set_msg(msg);
    return err;
}

static ErrorPtr newRangeNotFoundErr(uint64_t range_id) {
    ErrorPtr err(new dspb::Error);
    err->mutable_range_not_found()->set_range_id(range_id);
    return err;
}

void RangeServer::dispatchSchedule(RPCRequestPtr& rpc) {
    assert(rpc->sch_req);
    dspb::SchRequest& request = *(rpc->sch_req);

    dspb::SchResponse response;
    response.mutable_header()->set_cluster_id(request.header().cluster_id());
    // check cluster id
    if (request.header().cluster_id() != 0 &&
            request.header().cluster_id() != ds_config.cluster_config.cluster_id) {
        FLOG_ERROR("mismatch cluster id {} (self: {}) from {}, msg id={}",
                   request.header().cluster_id(), ds_config.cluster_config.cluster_id,
                   rpc->ctx.remote_addr, rpc->MsgID());

        auto err = newClusterMismatchErr(request.header().cluster_id(), ds_config.cluster_config.cluster_id);
        response.mutable_header()->set_allocated_error(err.release());
        rpc->Reply(response);
        return;
    }

    ErrorPtr err;
    switch (request.req_case()) {
        case dspb::SchRequest::kCreateRange:
            err = createRange(request.create_range(), response.mutable_create_range());
            break;
        case dspb::SchRequest::kDeleteRange:
            err = deleteRange(request.delete_range(), response.mutable_delete_range());
            break;
        case dspb::SchRequest::kTransferRangeLeader:
            err = transferLeader(request.transfer_range_leader(), response.mutable_transfer_range_leader());
            break;
        case dspb::SchRequest::kGetPeerInfo:
            err = getPeerInfo(request.get_peer_info(), response.mutable_get_peer_info());
            break;
        case dspb::SchRequest::kIsAlive:
            err = isAlive(request.is_alive(), response.mutable_is_alive());
            break;
        case dspb::SchRequest::kNodeInfo:
            err = nodeInfo(request.node_info(), response.mutable_node_info());
            break;
        case dspb::SchRequest::kChangeRaftMember:
            err = changeRaftMember(request.change_raft_member(), response.mutable_change_raft_member());
            break;
        default:
            err = newServerError("unsupported request type: " + std::to_string(request.req_case()));
            break;
    }

    if (err != nullptr) {
        response.mutable_header()->set_allocated_error(err.release());
        response.mutable_header()->set_cluster_id(request.header().cluster_id());
    }
    rpc->Reply(response);
}

Status RangeServer::createRange(const basepb::Range &range, uint64_t leader,
        std::shared_ptr<range::Range>& existed) {
    FLOG_DEBUG("new range: id={}, start={}, end={},"
               " version={}, conf_ver={}",
               range.id(), EncodeToHexString(range.start_key()),
               EncodeToHexString(range.end_key()), range.range_epoch().version(),
               range.range_epoch().conf_ver());

    auto ret = ranges_.Insert(range.id(), [&](RangePtr& new_rng) {
        auto s = meta_store_->AddRange(range);
        if (!s.ok()) {
            return s;
        }
        new_rng = std::make_shared<range::Range>(makeRangeOptions(range.range_type()), range_context_.get(), range);
        s = new_rng->Initialize(nullptr, leader);
        if (!s.ok()) {
            meta_store_->DelRange(range.id());
            return s;
        }
        return Status::OK();
    }, existed);

    if (!ret.ok()) {
        FLOG_ERROR("create range[{}] failed: {}", range.id(), ret.ToString());
        return ret;
    } else {
        FLOG_INFO("create new range[{}] success.", range.id());
    };

    return ret;
}

ErrorPtr RangeServer::createRange(const dspb::CreateRangeRequest& req, dspb::CreateRangeResponse* resp) {
    FLOG_INFO("range[{}] recv create range from master: {}", req.range().id(), req.ShortDebugString());

    ErrorPtr err;
    std::shared_ptr<range::Range> existed;
    auto ret = createRange(req.range(), req.leader(), existed);
    if (ret.code() == Status::kExisted && existed) {
        if (!existed->VerifyEpoch(req.range().range_epoch(), err)) {
            err->set_detail("range already exist but epoch no equal");
        } else {
            FLOG_WARN("create range {} already exist", req.range().id());
        }
    } else if (!ret.ok()) {
        err = newServerError(ret.ToString());
    }

    if (err != nullptr) {
        FLOG_ERROR("create range {} failed: {}", req.range().id(), err->ShortDebugString());
    } else {
        FLOG_INFO("create range {} successfully", req.range().id());
    }

    return err;
}

Status RangeServer::deleteRange(uint64_t range_id, uint64_t peer_id) {
    std::shared_ptr<range::Range> rng;
    auto ret = ranges_.RemoveIf(range_id, [=](const RangePtr& rng) {
        auto local_peer_id = rng->GetPeerID();
        // local_peer_id == 0 maybe removed
        if (peer_id != 0 && local_peer_id != 0 && local_peer_id != peer_id) {
            return Status(Status::kStaleRange, std::to_string(local_peer_id), std::to_string(peer_id));
        }

        auto s = meta_store_->DelRange(range_id);
        if (!s.ok()) {
            return s;
        }

        s = meta_store_->DelRangeStats(range_id);
        if (s.ok()) {
            return s;
        }
        return Status::OK();
    }, rng);

    if (ret.code() == Status::kStaleRange) {
        // consider mismatch as success
        FLOG_WARN("range[{}] delete failed: {}", range_id, ret.ToString());
        ret = Status(Status::kStaleRange, ret.ToString(), std::to_string(range_id));
    } else if (ret.code() == Status::kNotFound) {
        FLOG_WARN("range[{}] delete not found", range_id);
        ret = Status(Status::kNotFound, ret.ToString(), std::to_string(range_id));
    } else if (ret.ok()) {
        assert(rng != nullptr);
        ret = rng->Destroy();
    }

    if (!ret.ok()) {
        FLOG_ERROR("delete range[{}] failed: {}", range_id, ret.ToString());
    } else {
        FLOG_INFO("delete range[{}] success.", range_id);
    }
    return ret;

    return Status::OK();
}

ErrorPtr RangeServer::deleteRange(const dspb::DeleteRangeRequest& req, dspb::DeleteRangeResponse* resp) {
    FLOG_WARN("range[{}] recv delete range from master. peer_id={}", req.range_id(), req.peer_id());

    auto s = deleteRange(req.range_id(), req.peer_id());
    if (s.code() == Status::kNotFound) {
        return newRangeNotFoundErr(req.range_id());
    } else if (!s.ok()) {
        FLOG_ERROR("range[{}] delete failed: {}", req.range_id(), s.ToString());
        return newServerError(s.ToString());
    } else {
        return nullptr;
    }
}

ErrorPtr RangeServer::transferLeader(const dspb::TransferRangeLeaderRequest& req,
        dspb::TransferRangeLeaderResponse* resp) {
    FLOG_INFO("range[{}] recv transfer leader from master", req.range_id());

    auto range = Find(req.range_id());
    if (range == nullptr) {
        FLOG_ERROR("TransferLeade request not found range_id {} failed", req.range_id());
        return newRangeNotFoundErr(req.range_id());
    } else {
        range->TransferLeader();
        return nullptr;
    }
}

ErrorPtr RangeServer::getPeerInfo(const dspb::GetPeerInfoRequest& req, dspb::GetPeerInfoResponse* resp) {
    raft::RaftStatus peer_info;
    auto range = Find(req.range_id());
    if (range == nullptr) {
        FLOG_ERROR("TransferLeade request not found range_id {} failed", req.range_id());
        return newRangeNotFoundErr(req.range_id());
    } else {
        range->GetPeerInfo(&peer_info);
        resp->set_index(peer_info.index);
        resp->set_term(peer_info.term);
        resp->set_commit(peer_info.commit);

        auto replica = resp->mutable_replica();
        range->GetReplica(replica);
    }
    return nullptr;
}

ErrorPtr RangeServer::isAlive(const dspb::IsAliveRequest& req, dspb::IsAliveResponse* resp) {
    resp->set_alive(true);
    return nullptr;
}

ErrorPtr RangeServer::nodeInfo(const dspb::NodeInfoRequest& req, dspb::NodeInfoResponse* resp) {
    FLOG_DEBUG("nodeInfo, req: {}", req.DebugString());

    resp->set_node_id(context_->node_id);

    auto stats = resp->mutable_stats();
    stats->set_range_count(GetRangesCount());
    stats->set_range_leader_count(context_->run_status->GetLeaderCount());
    stats->set_range_split_count(context_->run_status->GetSplitCount());

    raft::ServerStatus rss;
    context_->raft_server->GetStatus(&rss);
    stats->set_snap_sending_count(rss.total_snap_sending);
    stats->set_snap_applying_count(rss.total_snap_applying);

    // collect db usage
    db::DBUsage db_usage;
    auto s = context_->run_status->GetDBUsage(db_manager_, db_usage);
    if (s.ok()) {
        stats->set_capacity(db_usage.total_size);
        stats->set_used_size(db_usage.used_size);
    } else {
        FLOG_ERROR("get db usage failed: {}", s.ToString());
    }

   // collect storage metric
    storage::MetricStat mstat;
    storage::Metric::CollectAll(&mstat);
    stats->set_read_keys_per_sec(mstat.keys_read_per_sec);
    stats->set_read_bytess_per_sec(mstat.bytes_read_per_sec);
    stats->set_write_keys_per_sec(mstat.keys_write_per_sec);
    stats->set_write_bytes_per_sec(mstat.bytes_write_per_sec);

    auto all_ranges = ranges_.GetAll();
    for (auto rng: all_ranges) {
        if (rng && rng->Valid()) {
            rng->GetRangeInfo(resp->add_range_infos());
        }
    }

    FLOG_DEBUG("nodeInfo, resp: {}", resp->DebugString());
    return nullptr;
}

ErrorPtr RangeServer::changeRaftMember(const dspb::ChangeRaftMemberRequest& req,
        dspb::ChangeRaftMemberResponse* resp) {
    FLOG_INFO("range[{}] recv change member from master: {}", req.range_id(), req.ShortDebugString());

    auto rng = Find(req.range_id());
    if (rng == nullptr) {
        return newRangeNotFoundErr(req.range_id());
    }

    ErrorPtr err;
    if (!rng->VerifyEpoch(req.range_epoch(), err)) {
        return err;
    }

    switch (req.change_type()) {
        case dspb::ChangeRaftMemberRequest_ChangeType_CT_ADD:
            rng->AddPeer(req.target_peer());
            break;
        case dspb::ChangeRaftMemberRequest_ChangeType_CT_REMOVE:
            rng->DelPeer(req.target_peer());
            break;
        default:
            err = newServerError("unsupported change type: " + std::to_string(req.change_type()));
            break;
    }
    return err;
}

void RangeServer::StatisPush(uint64_t range_id) {
    std::lock_guard<std::mutex> lock(statis_mutex_);
    statis_queue_.push(range_id);
    statis_cond_.notify_all();
}

} // namespace server
} // namespace ds
} // namespace jim
