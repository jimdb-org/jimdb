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

#include "range.h"

#include <sstream>

#include "master/client.h"
#include "base/util.h"
#include "storage/meta_store.h"

#include "range_logger.h"

namespace jim {
namespace ds {
namespace range {

void Range::checkSplit(uint64_t size) {
    statis_size_ += size;

    if (!statis_flag_ && statis_size_ > opt_.check_size && !recovering_) {
        statis_flag_ = true;
        context_->ScheduleCheckSize(id_);
    }
}

void Range::ResetStatisSize() {
    ResetStatisSize(opt_.split_size, opt_.max_size);
}

void Range::ResetStatisSize(uint64_t split_size, uint64_t max_size) {
    // split size is split size, not half of split size
    // amicable sequence writing and random writing
    // const static uint64_t split_size = ds_config.range_config.split_size >>
    // 1;
    auto meta = meta_.Get();

    uint64_t total_size = 0;
    uint64_t kv_count_1 = 0;
    uint64_t kv_count_2 = 0;
    std::string split_key;
    auto s = store_->StatSize(split_size, &total_size, &split_key, &kv_count_1, &kv_count_2);
    statis_flag_ = false;
    statis_size_ = 0;
    if (!s.ok()) {
        if (s.code() == Status::kUnexpected) {
            RLOG_INFO("StatSize failed: {}", s.ToString());
        } else {
            RLOG_ERROR("StatSize failed: {}", s.ToString());
            return;
        }
    }

    if (!split_key.empty()) {
        if (split_key <= meta.start_key() || split_key >= meta.end_key()) {
            RLOG_ERROR("StatSize invalid split key: {} vs scope[{}-{}]",
                       EncodeToHex(split_key),
                       EncodeToHex(meta.start_key()),
                       EncodeToHex(meta.end_key()));
            return ;
        }
    }

    RLOG_INFO("StatSize total size: {}, split key: {}, kv count : {}",
            total_size, EncodeToHex(split_key), kv_count_1+kv_count_2);

    if (!verifyEpoch(meta.range_epoch())) {
        RLOG_WARN("StatSize epoch is changed");
        return;
    }

    updateStats(total_size, kv_count_1+kv_count_2);

    // when real size >= max size, we need split with split size
    if (opt_.enable_split && total_size >= max_size) {

        dspb::SplitCommand_StatsInfo st_info;
        st_info.set_left_count(kv_count_1);
        st_info.set_right_count(kv_count_2);
        st_info.set_left_size(split_size);
        st_info.set_right_size(total_size - split_size);

        return askSplit(std::move(split_key), std::move(meta), std::move(st_info), false);
    }
}

void Range::askSplit(std::string &&key, basepb::Range&& meta, dspb::SplitCommand_StatsInfo&& info, bool force) {
    assert(!key.empty());
    assert(key >= meta.start_key());
    assert(key < meta.end_key());

    RLOG_INFO("AskSplit, version: {}, key: {}", meta.range_epoch().version(), EncodeToHex(key));

    mspb::AskSplitRequest ask;
    ask.set_allocated_range(new basepb::Range(std::move(meta)));
    ask.set_split_key(std::move(key));
    ask.set_force(force);
    mspb::AskSplitResponse resp;
    auto s = context_->MasterClient()->AskSplit(ask, resp);
    if (!s.ok()) {
        RLOG_WARN("AskSplit failed: {}", s.ToString());
    } else {
        RLOG_INFO("AskSplit success: {}", resp.ShortDebugString());
        startSplit(resp, std::move(info));
    }
}

void Range::startSplit(mspb::AskSplitResponse &resp, dspb::SplitCommand_StatsInfo &&info) {
    if (!valid_) {
        return;
    }

    if (!verifyEpoch(resp.range().range_epoch())) {
        RLOG_WARN("StartSplit epoch is changed");
        return;
    }

    auto &split_key = resp.split_key();

    RLOG_INFO("StartSplit new_range_id: {} split_key: {}",
            resp.new_range_id(), EncodeToHex(split_key));

    dspb::Command cmd;
    // set cmd_id
    cmd.mutable_cmd_id()->set_node_id(node_id_);
    cmd.mutable_cmd_id()->set_seq(kPassbySubmitQueueCmdSeq);
    // set cmd_type
    cmd.set_cmd_type(dspb::CmdType::AdminSplit);

    // set cmd admin_split_req
    auto split_req = cmd.mutable_split_cmd();

    split_req->set_split_key(split_key);

    auto range = resp.release_range();

    auto psize = range->peers_size();
    auto rsize = resp.new_peer_ids_size();
    if (psize != rsize) {
        RLOG_WARN("StartSplit peers_size no equal");
        return;
    }

    auto epoch = range->mutable_range_epoch();

    // set verify epoch
    cmd.set_allocated_verify_epoch(new basepb::RangeEpoch(*epoch));
    // force rotate
    cmd.set_cmd_flags(raft::EntryFlags::kForceRotate);

    // set leader
    split_req->set_leader(node_id_);
    // set epoch
    split_req->mutable_epoch()->set_version(epoch->version() + 1);

    // no need set con_ver;con_ver is member change
    // split_req->mutable_epoch()->set_version(epoch->conf_ver());

    // set range id
    range->set_id(resp.new_range_id());
    // set parent range id
    range->set_parent_range_id(id_);
    // set range start_key
    range->set_start_key(split_key);
    // range end_key doesn't need to change.

    // set range_epoch
    epoch->set_conf_ver(1);
    epoch->set_version(1);

    auto p0 = range->mutable_peers(0);
    // set range peers
    for (int i = 0; i < psize; i++) {
        auto px = range->mutable_peers(i);
        px->set_id(resp.new_peer_ids(i));

        // Don't consider the role
        if (i > 0 && px->node_id() == node_id_) {
            p0->Swap(px);
        }
    }

    split_req->set_allocated_new_range(range);
    split_req->set_allocated_info(new dspb::SplitCommand_StatsInfo(std::move(info)));

    auto ret = submit(cmd, jim::raft::WRITE_FLAG);
    if (!ret.ok()) {
        RLOG_ERROR("Split raft submit error: {}", ret.ToString());
    }
}

Status Range::Split(const dspb::SplitCommand& req, uint64_t raft_index,
        std::shared_ptr<Range>& split_range) {
    auto split_range_id = req.new_range().id();
    auto split_raft_path = raftLogPath(meta_.GetTableID(), split_range_id);
    auto s = raft_->InheritLog(split_raft_path, raft_index, true);
    if (!s.ok()) {
        RLOG_ERROR("Split(new range: {}) clone raft logs failed: {}", req.new_range().id(), s.ToString());
        return s;
    }
    split_range = std::make_shared<Range>(opt_, context_, req.new_range());
    std::unique_ptr<db::DB> split_db;
    auto ret = store_->Split(req.new_range().id(), req.split_key(), raft_index, split_db);
    if (!ret.ok()) {
        RLOG_ERROR("Split(new range: {}) split new db failed: {}", req.new_range().id(), ret.ToString());
        return ret;
    }
    return split_range->Initialize(std::move(split_db), req.leader());
}

Status Range::applySplit(const dspb::Command &cmd, uint64_t index) {
    RLOG_INFO("ApplySplit Begin, version: {}, index: {}", meta_.GetVersion(), index);

    const auto& req = cmd.split_cmd();
    auto ret = meta_.CheckSplit(req.split_key(), cmd.verify_epoch().version());
    if (ret.code() == Status::kStaleEpoch) {
        RLOG_WARN("ApplySplit(new range: {}) check failed: {}",
                req.new_range().id(), ret.ToString());
        return Status::OK();
    } else if (ret.code() == Status::kOutOfBound) {
        // invalid split key, ignore split request
        RLOG_ERROR("ApplySplit(new range: {}) check failed: {}",
                req.new_range().id(), ret.ToString());
        return Status::OK();
    } else if (!ret.ok()) {
        RLOG_ERROR("ApplySplit(new range: {}) check failed: {}",
                req.new_range().id(), ret.ToString());
        return ret;
    }

    context_->Statistics()->IncrSplitCount();

    ret = context_->SplitRange(id_, req, index);
    if (!ret.ok()) {
        RLOG_ERROR("ApplySplit(new range: {}) create failed: {}",
                req.new_range().id(), ret.ToString());
        return ret;
    }

    meta_.Split(req.split_key(), req.epoch().version());
    ret = store_->ApplySplit(req.split_key(), index);
    if (!ret.ok()) {
        return Status(Status::kIOError, "apply split", ret.ToString());
    }

    scheduleHeartbeat(false);

    split_range_id_ = req.new_range().id();

    auto rng = context_->FindRange(split_range_id_);
    if (rng != nullptr) {
        // new range report heartbeat
        if (req.leader() == node_id_) {
            rng->scheduleHeartbeat(false);
        }
        rng->SetStatsInfo(req.info().right_count(), req.info().right_size());
    }

    SetStatsInfo(req.info().left_count(), req.info().left_size());

    context_->Statistics()->DecrSplitCount();
    RLOG_INFO("ApplySplit(new range: {}) End. version:{}",
            req.new_range().id(), meta_.GetVersion());

    return ret;
}

Status Range::ForceSplit(uint64_t version, std::string *result_split_key) {
    auto meta = meta_.Get();
    // check version when version ne zero
    if (version != 0 && meta.range_epoch().version() != version) {
        std::ostringstream ss;
        ss << "request version: " << version << ", ";
        ss << "current version: " << meta.range_epoch().version();
        return Status(Status::kStaleEpoch, "force split", ss.str());
    }

    std::string split_key;
    split_key = FindMiddle(meta.start_key(), meta.end_key());
    if (split_key.empty() || split_key <= meta.start_key() || split_key >= meta.end_key()) {
        std::ostringstream ss;
        ss << "begin key: " << EncodeToHex(meta.start_key()) << ", ";
        ss << "end key: " << EncodeToHex(meta.end_key()) << ", ";
        ss << "split key: " << EncodeToHex(split_key);
        return Status(Status::kUnknown, "could not find split key", ss.str());
    }

    RLOG_INFO("Force split, start AskSplit. split key: [{}-{}]-{}, version: {}",
            EncodeToHex(meta.start_key()), EncodeToHex(meta.end_key()),
            EncodeToHex(split_key), meta.range_epoch().version());

    *result_split_key = split_key;

    uint64_t left_count = 0;
    uint64_t left_size = 0;
    uint64_t right_count = 0;
    uint64_t right_size = 0;

    auto s = store_->StatSize(split_key, &left_count, &left_size, &right_count, &right_size);
    if (!s.ok()) {
        if ( s.code() == Status::kUnexpected) {
            RLOG_INFO("Force split StatSize failed: {}", s.ToString());
        } else {
            RLOG_INFO("Force split StatSize failed: {}", s.ToString());
            return s;
        }
    }

    dspb::SplitCommand_StatsInfo st_info;
    st_info.set_left_count(left_count);
    st_info.set_right_count(right_count);
    st_info.set_left_size(left_size);
    st_info.set_right_size(right_size);

    askSplit(std::move(split_key), std::move(meta), std::move(st_info), true);

    return Status::OK();
}

void Range::updateStats(const uint64_t total_size, const uint64_t kv_count) {
    RLOG_INFO("startStats total_size:{}, kv_count:{}", total_size, kv_count);

    if (!valid_) {
        return ;
    }

    dspb::Command cmd;
    // set cmd_id
    cmd.mutable_cmd_id()->set_node_id(node_id_);
    cmd.mutable_cmd_id()->set_seq(kPassbySubmitQueueCmdSeq);
    // set cmd_type
    cmd.set_cmd_type(dspb::CmdType::UpdateStats);
    cmd.set_allocated_verify_epoch(new basepb::RangeEpoch(GetMeta().range_epoch()));

    auto stats_req = cmd.mutable_update_stats_cmd();

    stats_req->set_real_size(total_size);
    stats_req->set_kv_count(kv_count);

    auto ret = submit(cmd, jim::raft::WRITE_FLAG);
    if (!ret.ok()) {
        RLOG_ERROR("stats raft submit error: {}", ret.ToString());
    }
}

Status Range::applyUpdateStats( const dspb::Command &cmd, uint64_t index) {
    RLOG_INFO("ApplyUpdateStats Begin, version: {}, index: {}", meta_.GetVersion(), index);

    const auto& req = cmd.update_stats_cmd();
    auto ret = meta_.verifyVersion(cmd.verify_epoch().version());
    if (ret.code() == Status::kStaleEpoch) {
        RLOG_WARN("ApplyUpdateStats check failed: {}", ret.ToString());
        return Status::OK();
    } else if (!ret.ok()) {
        RLOG_ERROR("ApplyUpdateStats check failed: {}", ret.ToString());
        return ret;
    }

    SetStatsInfo(req.kv_count(), req.real_size());

    RLOG_INFO("ApplyUpdateStats End, version: {}, index: {}, kv_count: {}, real_size: {}",
            meta_.GetVersion(), index, req.kv_count(), req.real_size());
    return ret;

}

void Range::SetStatsInfo( const uint64_t kv_count, const uint64_t real_size){
    RLOG_INFO("SetStatsInfo kv_count:{}, real_size:{}", kv_count, real_size);

    SetKvCount(kv_count);
    SetRealSize(real_size);

    dspb::UpdateStatsCommand cmd;
    cmd.set_real_size(real_size);
    cmd.set_kv_count(kv_count);

    auto str_cmd = cmd.SerializeAsString();
    auto ret = context_->MetaStore()->SaveRangeStats(id_, str_cmd);
    if (!ret.ok()) {
        RLOG_WARN("save range stats fail info:", ret.ToString());
    }
}

}  // namespace range
}  // namespace ds
}  // namespace jim
