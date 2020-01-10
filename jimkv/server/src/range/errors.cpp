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

#include "base/util.h"
#include "range_logger.h"

namespace jimkv {
namespace ds {
namespace range {

// percentage
static const uint64_t kDBUsagePercentSoftLimit = 85;
static const uint64_t kDBUsagePercentHardLimit = 92;

// return to client the newest route info (include meta and leader inf0)
basepb::Range Range::getRouteInfo() const {
    auto meta = meta_.Get();
    // trim unnecessary info
    meta.clear_key_schema();
    meta.clear_peers();
    meta.clear_db_id();

    // fetch newest leader
    auto raft = raft_;
    if (raft) {
        uint64_t leader = 0, term = 0;
        raft->GetLeaderTerm(&leader, &term);
        meta.set_leader(leader);
    }
    return meta;
}

ErrorPtr Range::staleEpochErr() {
    ErrorPtr err(new dspb::Error);

    auto current_meta = getRouteInfo();
    auto stale_epoch = err->mutable_stale_epoch();
    stale_epoch->set_allocated_old_range(new basepb::Range(std::move(current_meta)));

    if (split_range_id_ > 0) {
        auto split_range = context_->FindRange(split_range_id_);
        if (split_range) {
            auto split_meta = split_range->getRouteInfo();
            stale_epoch->set_allocated_new_range(new basepb::Range(std::move(split_meta)));
        }
    }

    return err;
}

bool Range::VerifyEpoch(const basepb::RangeEpoch& epoch, ErrorPtr& err) {
    return verifyEpoch(epoch, &err);
}

bool Range::verifyEpoch(const basepb::RangeEpoch& epoch, ErrorPtr* err) {
    if (meta_.EpochIsEqual(epoch)) {
        return true;
    }

    auto meta = meta_.Get();
    if (meta.range_epoch().version() == epoch.version() &&
        meta.range_epoch().conf_ver() == epoch.conf_ver()) {
        return true;
    } else {
        if (err != nullptr) {
            *err = staleEpochErr();
            (*err)->set_detail("request epoch: " + epoch.ShortDebugString());
        }
        return false;
    }
}

ErrorPtr Range::newRangeNotFoundErr(uint64_t range_id) {
    ErrorPtr err(new dspb::Error);
    err->mutable_range_not_found()->set_range_id(range_id);
    err->set_detail("range not found");
    return err;
}

ErrorPtr Range::raftFailError() {
    ErrorPtr err(new dspb::Error);
    err->set_detail("raft submit fail");
    err->mutable_raft_fail();
    return err;
}

ErrorPtr Range::noLeaderError() {
    ErrorPtr err(new dspb::Error);
    err->set_detail("no leader");
    err->mutable_not_leader()->set_range_id(id_);
    return err;
}

ErrorPtr Range::notLeaderError(basepb::Peer &&peer) {
    ErrorPtr err(new dspb::Error);
    err->set_detail("not leader");
    err->mutable_not_leader()->set_range_id(id_);
    err->mutable_not_leader()->set_allocated_leader(new basepb::Peer(std::move(peer)));
    meta_.GetEpoch(err->mutable_not_leader()->mutable_epoch());
    return err;
}

bool Range::verifyLeader(ErrorPtr* err) {
    if (raft_->IsLeader()) {
        return true;
    }

    uint64_t leader, term;
    raft_->GetLeaderTerm(&leader, &term);
    // we are leader
    if (leader == node_id_) return true;

    basepb::Peer peer;
    if (leader == 0 || !meta_.FindPeerByNodeID(leader, &peer)) {
        if (err != nullptr) {
            *err = noLeaderError();
        }
        return false;
    } else {
        if (err != nullptr) {
            *err = notLeaderError(std::move(peer));
        }
        return false;
    }
}

static ErrorPtr noLeftSpaceErr() {
    ErrorPtr err(new dspb::Error);
    err->mutable_no_left_space();
    err->set_detail("no left space");
    return err;
}

bool Range::hasSpaceLeft(LimitType type, ErrorPtr* err) {
    auto percent = context_->GetDBUsagePercent();
    bool reach_limit = percent >= kDBUsagePercentHardLimit ||
                       (type == LimitType::kSoft && percent >= kDBUsagePercentSoftLimit);
    if (!reach_limit) {
        return true;
    } else {
        RLOG_ERROR("db usage percent({} > {}) limit reached, reject write request", percent,
                   (type == LimitType::kSoft ? kDBUsagePercentSoftLimit : kDBUsagePercentHardLimit));

        if (err != nullptr) {
            *err = noLeftSpaceErr();
        }
        return false;
    }
}

ErrorPtr Range::keyOutOfBoundErr(const std::string &key) {
    ErrorPtr err(new dspb::Error);
    err->mutable_out_of_bound()->set_key(key);
    err->mutable_out_of_bound()->set_range_id(id_);
    err->mutable_out_of_bound()->set_range_start(start_key_);
    // end_key change at range split time
    err->mutable_out_of_bound()->set_range_limit(meta_.GetEndKey());
    return err;
}

bool Range::verifyKeyInBound(const std::string &key, ErrorPtr* err) {
    if (key < start_key_) {
        if (!recovering_) {
            RLOG_WARN("key: {} less than start_key: {}, out of range",
                      EncodeToHex(key), EncodeToHex(start_key_));
        }
        if (err != nullptr)  {
            *err = keyOutOfBoundErr(key);
        }
        return false;
    }

    auto end_key = meta_.GetEndKey();
    if (key >= end_key) {
        if (!recovering_) {
            RLOG_WARN("key: {} greater than end_key: {}, out of range",
                      EncodeToHex(key), EncodeToHex(end_key));
        }
        if (err != nullptr)  {
            *err = keyOutOfBoundErr(key);
        }
        return false;
    }
    return true;
}

bool Range::verifyKeyInBound(const dspb::PrepareRequest& req, ErrorPtr* err) {
    for (const auto& intent: req.intents()) {
        if (!verifyKeyInBound(intent.key(), err)) {
            return false;
        }
    }
    return true;
}

bool Range::verifyKeyInBound(const dspb::DecideRequest& req, ErrorPtr* err) {
    for (const auto& key: req.keys()) {
        if (!verifyKeyInBound(key, err)) {
            return false;
        }
    }
    return true;
}

}  // namespace range
}  // namespace ds
}  // namespace jimkv
