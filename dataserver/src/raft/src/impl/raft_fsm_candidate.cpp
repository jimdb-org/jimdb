// Copyright 2015 The etcd Authors
// Portions Copyright 2019 The JIMDB Authors.
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

#include "raft_fsm.h"

#include <functional>
#include "common/logger.h"
#include "raft_exception.h"

namespace jim {
namespace raft {
namespace impl {

void RaftFsm::becomeCandidate() {
    if (state_ == FsmState::kLeader) {
        throw RaftException(
            "[raft->becomeCandidate] invalid transition [leader -> candidate]");
    }

    step_func_ = std::bind(&RaftFsm::stepCandidate, this, std::placeholders::_1);
    reset(term_ + 1, false);
    tick_func_ = std::bind(&RaftFsm::tickElection, this);
    vote_for_ = node_id_;
    state_ = FsmState::kCandidate;

    FLOG_INFO("node_id: {} raft[{}] become candidate at term {}", node_id_, id_, term_);
}

void RaftFsm::becomePreCandidate() {
    if (state_ == FsmState::kLeader) {
        throw RaftException("invalid transition[leader -> pre-candidate]");
    }

    step_func_ = std::bind(&RaftFsm::stepCandidate, this, std::placeholders::_1);
    tick_func_ = std::bind(&RaftFsm::tickElection, this);
    state_ = FsmState::kPreCandidate;
    votes_.clear();

    FLOG_INFO("node_id: {} raft[{}] become pre-candidate at term {}", node_id_, id_, term_);
}

void RaftFsm::stepCandidate(MessagePtr& msg) {
    switch (msg->type()) {
        case pb::LOCAL_MSG_PROP:
            FLOG_DEBUG("node_id: {} raft[{}] no leader at term {}; dropping proposal.", node_id_, id_, term_);
            return;

        case pb::LOCAL_MSG_READ_INDEX:
            FLOG_DEBUG("node_id: {} raft[{}] no leader at term {}; dropping read request.", node_id_, id_, term_);
            return;

        case pb::APPEND_ENTRIES_REQUEST:
            becomeFollower(term_, msg->from());
            handleAppendEntries(msg);
            return;

        case pb::HEARTBEAT_REQUEST:
            becomeFollower(term_, msg->from());
            return;

        case pb::VOTE_RESPONSE:
        case pb::PRE_VOTE_RESPONSE: {
            bool pre = false;
            if (msg->type() == pb::PRE_VOTE_RESPONSE) {
                if (state_ != FsmState::kPreCandidate) {
                    FLOG_WARN("node_id: {}, raft[{}], term: {} is not pre candidate", node_id_, id_, term_);
                    return;
                }
                pre = true;
            } else {
                if (state_ != FsmState::kCandidate) {
                    FLOG_WARN("node_id: {}, raft[{}], term: {} is not candidate", node_id_, id_, term_);
                    return;
                }
            }

            int grants = poll(pre, msg->from(), !msg->reject());
            int rejects = votes_.size() - grants;

            FLOG_INFO("node_id: {} raft[{}] {} quorum: {} has received {} votes and {} vote rejections",
                      node_id_, id_, (pre ? "pre-campaign" : "campaign"), quorum(), grants, rejects);

            if (grants == quorum()) {
                if (pre) {
                    campaign(false);
                } else {
                    becomeLeader();
                    bcastAppend();
                }
            } else if (rejects == quorum()) {
                becomeFollower(term_, 0);
            }
            return;
        }
        default:
            FLOG_WARN("node_id: {} raft[{}], term: {} invalid message type: {}", node_id_, id_, term_, msg->type());
        return;
    }
}

void RaftFsm::campaign(bool pre) {
    if (pre) {
        becomePreCandidate();
    } else {
        becomeCandidate();
    }

    if (poll(pre, node_id_, true) == quorum()) {
        if (pre) {
            campaign(false);
        } else {
            becomeLeader();
        }
        return;
    }

    uint64_t li = 0, lt = 0;
    raft_log_->lastIndexAndTerm(&li, &lt);
    for (const auto& r : replicas_) {
        if (r.first == node_id_) continue;

        FLOG_INFO("node_id: {} raft[{}] {}: [logterm: {}, index: {}] sent vote request to {} at term {}.",
                  node_id_, id_, (pre ? "pre-campaign" : "campaign"), lt, li, r.first, term_);

        MessagePtr msg(new pb::Message);
        msg->set_type(pre ? pb::PRE_VOTE_REQUEST : pb::VOTE_REQUEST);
        msg->set_term(pre ? (term_ + 1) : term_);
        msg->set_to(r.first);
        msg->set_log_index(li);
        msg->set_log_term(lt);
        send(msg);
    }
}

int RaftFsm::poll(bool pre, uint64_t node_id, bool vote) {
    FLOG_INFO("node_id: {} raft[{}] received {} {} from {} at term {}",
              node_id_, id_, (pre ? "pre-vote" : "vote"), (vote ? "granted" : "rejected"),
              node_id, term_);

    votes_.emplace(node_id, vote);

    int grants = 0;
    for (const auto& kv : votes_) {
        if (kv.second) ++grants;
    }
    return grants;
}

} /* namespace impl */
} /* namespace raft */
} /* namespace jim */
