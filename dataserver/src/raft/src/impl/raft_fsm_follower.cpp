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

#include "common/logger.h"
#include "storage/storage.h"
#include "snapshot/apply_task.h"

namespace jim {
namespace raft {
namespace impl {

void RaftFsm::becomeFollower(uint64_t term, uint64_t leader) {
    step_func_ = std::bind(&RaftFsm::stepFollower, this, std::placeholders::_1);
    reset(term, false);
    tick_func_ = std::bind(&RaftFsm::tickElection, this);
    leader_ = leader;
    state_ = FsmState::kFollower;

    FLOG_INFO("node_id: {}, raft[{}] become follwer at term {}", node_id_, id_, term_);
}

void RaftFsm::stepFollower(MessagePtr& msg) {
    switch (msg->type()) {
        case pb::LOCAL_MSG_PROP:
            if (leader_ == 0) {
                FLOG_DEBUG("raft[{}] no leader at term {}; dropping proposal.", id_, term_);
            } else {
                FLOG_WARN("node_id: {}, raft[{}], term: {} is not leader, retransmit to leader: {}", node_id_, id_, term_, leader_);
                msg->set_to(leader_);
                send(msg);
            }
            return;

        case pb::APPEND_ENTRIES_REQUEST:
            election_elapsed_ = 0;
            leader_ = msg->from();
            handleAppendEntries(msg);
            return;
        case pb::READ_INDEX_REQUEST:
            election_elapsed_ = 0;
            handleReadRequest(msg);
            return;
        case pb::HEARTBEAT_REQUEST:
            election_elapsed_ = 0;
            leader_ = msg->from();
            return;

        case pb::SNAPSHOT_REQUEST:
            election_elapsed_ = 0;
            leader_ = msg->from();
            handleSnapshot(msg);
            return;

        case pb::LOCAL_SNAPSHOT_STATUS:
            if (!applying_snap_ || applying_snap_->GetContext().uuid != msg->snapshot().uuid()) {
                return;
            }

            if (!msg->reject()) {
                auto s = restore(applying_meta_);
                if (!s.ok()) {
                    FLOG_ERROR("node_id: {}, raft[{}] restore snapshot[{}] failed({}) at term {}.",
                               node_id_, id_, applying_snap_->GetContext().uuid, s.ToString(), term_);
                } else {
                    MessagePtr resp(new pb::Message);
                    resp->set_type(pb::APPEND_ENTRIES_RESPONSE);
                    resp->set_to(msg->from());
                    resp->set_log_index(raft_log_->lastIndex());
                    resp->set_commit(raft_log_->committed());
                    send(resp);
                }
            }
            applying_snap_.reset();
            return;

        default:
            return;
    }
}

void RaftFsm::tickElection() {
    if (!electable()) {
        election_elapsed_ = 0;
        return;
    }

    ++election_elapsed_;
    if (pastElectionTimeout() ) {
        election_elapsed_ = 0;
        MessagePtr msg(new pb::Message);
        msg->set_type(pb::LOCAL_MSG_HUP);
        msg->set_from(node_id_);
        Step(msg);
    }
}

void RaftFsm::handleAppendEntries(MessagePtr& msg) {
    MessagePtr resp_msg(new pb::Message);
    resp_msg->set_type(pb::APPEND_ENTRIES_RESPONSE);
    resp_msg->set_to(msg->from());
    resp_msg->set_reject(false);

    if (msg->log_index() < raft_log_->committed()) {
        resp_msg->set_log_index(raft_log_->committed());
        resp_msg->set_commit(raft_log_->committed());
        send(resp_msg);
        return;
    }

    std::vector<EntryPtr> ents;
    takeEntries(msg, ents);
    uint64_t last_index = 0;
    if (raft_log_->maybeAppend(msg->log_index(), msg->log_term(), msg->commit(), ents,
                               &last_index)) {
        resp_msg->set_log_index(last_index);
        resp_msg->set_commit(raft_log_->committed());
        send(resp_msg);
    } else {
        FLOG_DEBUG("node_id: {}, raft[{}] [logterm:{}, index:{}] rejected msgApp from {}[logterm:{}, index:{}]",
                   node_id_, id_, raft_log_->lastTerm(), raft_log_->lastIndex(), msg->from(),
                   msg->log_term(), msg->log_index());

        resp_msg->set_log_index(msg->log_index());
        resp_msg->set_commit(raft_log_->committed());
        resp_msg->set_reject(true);
        resp_msg->set_reject_hint(raft_log_->lastIndex());
        send(resp_msg);
    }
}

void RaftFsm::handleReadRequest(MessagePtr& msg) {
    bool reject = false;
    uint64_t leader;
    uint64_t term;
    if (leader_ != msg->from() || term_ != msg->term()) {
        reject = true;
    }
    MessagePtr resp_msg(new pb::Message);
    resp_msg->set_type(pb::READ_INDEX_RESPONSE);
    resp_msg->set_to(msg->from());
    resp_msg->set_reject(reject);
    if (reject) {
        resp_msg->set_reject_hint(leader_);
    }
    resp_msg->set_read_sequence(msg->read_sequence());
    resp_msg->set_log_index(msg->log_index());
    resp_msg->set_commit(raft_log_->committed());
    send(resp_msg);
}

void RaftFsm::handleSnapshot(MessagePtr& msg) {
    auto s = applySnapshot(msg);
    if (!s.ok()) {
        FLOG_ERROR("node_id: {}, raft[{}] apply snapshot[{}:{}] from {} at term {} failed: {}",
                   node_id_, id_, msg->snapshot().uuid(), msg->snapshot().seq(), msg->from(), term_, s.ToString());

        MessagePtr resp(new pb::Message);
        resp->set_type(pb::SNAPSHOT_ACK);
        resp->set_to(msg->from());
        resp->set_reject(true);
        resp->mutable_snapshot()->set_uuid(msg->snapshot().uuid());
        resp->mutable_snapshot()->set_seq(msg->snapshot().seq());
        send(resp);
    }
}

Status RaftFsm::applySnapshot(MessagePtr& msg) {
    if (applying_snap_) {
        bool reject = false;
        if (applying_snap_->GetContext().uuid != msg->snapshot().uuid()) {
            return Status(Status::kExisted, "othre snapshot is applying", std::to_string(applying_snap_->GetContext().uuid));
        } else {
            auto s = applying_snap_->RecvData(msg);
            if (!s.ok()) {
                return s;
            }
        }
        return Status::OK();
    }

    const auto& snapshot = msg->snapshot();
    if (snapshot.uuid() == 0) {
        return Status(Status::kInvalidArgument, "uuid", "0");
    }
    if (!snapshot.has_meta()) {
        return Status(Status::kInvalidArgument, "meta", "missing");
    }
    if (snapshot.seq() != 1) {
        return Status(Status::kInvalidArgument, "seq", "invalid first seq");
    }

    if (!checkSnapshot(snapshot.meta())) {
        FLOG_WARN("node_id: {}, raft[{}] [commit: {}] ignored snapshot [index: {}, term: {}, uuid: {}].",
                  node_id_, id_, raft_log_->committed(), snapshot.meta().index(),
                  snapshot.meta().term(), snapshot.uuid());

        MessagePtr resp(new pb::Message);
        resp->set_type(pb::APPEND_ENTRIES_RESPONSE);
        resp->set_to(msg->from());
        resp->set_log_index(raft_log_->lastIndex());
        resp->set_commit(raft_log_->committed());
        send(resp);
        return Status::OK();
    }

    SnapContext ctx;
    ctx.uuid = snapshot.uuid();
    ctx.from = msg->from();
    ctx.id = id_;
    ctx.term = term_;
    ctx.to = node_id_;

    FLOG_INFO("node_id: {}, raft[{}] [commit: {}] start to apply snapshot [index: {}, term: {}, uuid: {}].",
              node_id_, id_, raft_log_->committed(), snapshot.meta().index(),
              snapshot.meta().term(), snapshot.uuid());

    // new apply task
    applying_snap_ = std::make_shared<ApplySnapTask>(ctx, sm_);
    applying_meta_ = snapshot.meta();
    return applying_snap_->RecvData(msg);
}

bool RaftFsm::checkSnapshot(const pb::SnapshotMeta& meta) {
    if (meta.index() <= raft_log_->committed()) {
        return false;
    } else if (raft_log_->matchTerm(meta.index(), meta.term())) {
        raft_log_->commitTo(meta.index());
        return false;
    } else {
        return true;
    }
}

Status RaftFsm::restore(const pb::SnapshotMeta& meta) {
    FLOG_WARN("node_id: {}, raft[{}] [commit:{}, lastindex:{}, lastterm:{}] starts "
              "to restore snapshot [index:{}, term:{}], peers: {}",
              node_id_, id_, raft_log_->committed(), raft_log_->lastIndex(), raft_log_->lastTerm(),
              meta.index(), meta.term(), meta.peers_size());

    auto s = storage_->ApplySnapshot(meta);
    if (!s.ok()) return s;

    raft_log_->restore(meta.index());

    // restore all replicas
    replicas_.clear();
    learners_.clear();
    for (int i = 0; i < meta.peers_size(); ++i) {
        Peer peer;
        auto s = DecodePeer(meta.peers(i), &peer);
        if (!s.ok()) {
            return s;
        }

        if (peer.node_id == node_id_) {
            if (!is_learner_ && peer.type == PeerType::kLearner) {
                return Status(Status::kInvalidArgument, "restore meta",
                              "counld degrade from a normal peer to leadrner");
            }
            // promote self from snapshot
            if (is_learner_ && peer.type != PeerType::kLearner) {
                FLOG_INFO("node_id: {}, raft[{}] promote self from snapshot", node_id_, id_);
                is_learner_ = false;
            }
        }

        if (peer.type == PeerType::kLearner) {
            learners_.emplace(peer.node_id, newReplica(peer, false));
        } else {
            replicas_.emplace(peer.node_id, newReplica(peer, false));
        }
    }
    return Status::OK();
}

} /* namespace impl */
} /* namespace raft */
} /* namespace jim */
