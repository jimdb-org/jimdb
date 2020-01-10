// Copyright 2015 The etcd Authors
// Portions Copyright 2019 The JimDB Authors.
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

#include <algorithm>
#include <sstream>
#include "common/logger.h"
#include "raft_exception.h"
#include "snapshot/send_task.h"

namespace jimkv {
namespace raft {
namespace impl {

static const size_t kMaxPendingEntries = 10000;

void RaftFsm::becomeLeader() {
    if (state_ == FsmState::kFollower) {
        throw RaftException(
            "[raft->becomeLeader]invalid transition [follower -> leader].");
    }

    step_func_ = std::bind(&RaftFsm::stepLeader, this, std::placeholders::_1);
    reset(term_, true);
    tick_func_ = std::bind(&RaftFsm::tickHeartbeat, this);
    leader_ = node_id_;
    state_ = FsmState::kLeader;

    std::vector<EntryPtr> ents;
    Status s = raft_log_->entries(raft_log_->committed() + 1, kNoLimit, &ents);
    if (!s.ok()) {
        throw RaftException(std::string("[raft->becomeLeader] unexpected error "
                                        "getting uncommitted entries:") +
                            s.ToString());
    }
    int nconf = numOfPendingConf(ents);
    if (nconf > 1) {
        throw RaftException("[raft->becomeLeader]unexpected double uncommitted config "
                            "entry");
    } else if (nconf == 1) {
        pending_conf_ = true;
    }

    EntryPtr entry(new pb::Entry);
    entry->set_type(pb::ENTRY_NORMAL);
    entry->set_term(term_);
    entry->set_index(raft_log_->lastIndex() + 1);
    appendEntry(std::vector<EntryPtr>{entry});

    FLOG_INFO("node_id: {}, raft[{}] become leader at term {}", node_id_, id_, term_);
}

void RaftFsm::stepReadIndex(MessagePtr& msg) {
    FLOG_DEBUG("node_id: {}, raft: {} recv read request", node_id_, id_);
    if (msg->entries_size() == 0) {
        return;
    }
    for (int i = 0; i < msg->entries_size(); ++i) {
        msg->mutable_entries(i)->set_index(++read_index_seq_);
    }

    std::vector<EntryPtr> ents;
    takeEntries(msg, ents);
    auto commit_index = raft_log_->committed();
    auto apply_index = raft_log_->applied();
    if (sops_.read_option == LEASE_ONLY) {
        FLOG_DEBUG("node_id: {}, raft: {} lease only", node_id_, id_);
        if (commit_index == apply_index) {
            FLOG_DEBUG("node_id: {}, raft: {} commit_index equal apply_index", node_id_, id_);
            //commit_index is equal to apply_index, directly read and return client
            for (auto& entry: ents) {
                FLOG_DEBUG("node_id: {}, raft: {} precess uinque_seq: {}", node_id_, id_, entry->index());
                auto s = ReadProcess(entry, jimkv::raft::READ_SUCCESS);
                if (!s.ok()) {
                    //todo implements
                    throw RaftException(std::string("statemachine read entry[") +
                                        std::to_string(entry->index()) + "] error: " + s.ToString());
                }
            }
        } else {
            //commit_index gt apply_index, wait apply to commit index, then return client
            FLOG_DEBUG("node_id: {}, raft: {} commit_index greater than apply_index", node_id_, id_);
            for (auto& entry: ents) {
                FLOG_DEBUG("node_id: {}, raft: {} precess uinque_seq: {}", node_id_, id_, entry->index());
                read_context_->AddReadIndex(entry->index(), commit_index, entry);
                //self is set active
                read_context_->SetVerifyResult(node_id_, entry->index(), true);
            }
        }
    } else {
        //consensus read
        if (replicas_.size() == 1) { //only one replicate
            if (commit_index == apply_index) {
                //commit_index is equal to apply_index, directly read and return client
                FLOG_DEBUG("node_id: {}, raft: {} commit_index equal apply_index", node_id_, id_);
                for (auto& entry: ents) {
                    FLOG_DEBUG("node_id: {}, raft: {} precess uinque_seq: {}", node_id_, id_, entry->index());
                    auto s = ReadProcess(entry, jimkv::raft::READ_SUCCESS);
                    if (!s.ok()) {
                        //todo implements
                        throw RaftException(std::string("statemachine read entry[") +
                                            std::to_string(entry->index()) + "] error: " + s.ToString());
                    }
                }
            } else {
                //commit_index gt apply_index, wait apply to commit index, then return client
                FLOG_DEBUG("node_id: {}, raft: {} commit_index greater than apply_index", node_id_, id_);
                for (auto& entry: ents) {
                    FLOG_DEBUG("node_id: {}, raft: {} precess uinque_seq: {}", node_id_, id_, entry->index());
                    read_context_->AddReadIndex(entry->index(), commit_index, entry);
                    //self is set active
                    read_context_->SetVerifyResult(node_id_, entry->index(), true);
                }
            }
        } else {
            for (auto& entry: ents) {
                FLOG_DEBUG("node_id: {}, raft: {} precess uinque_seq: {}", node_id_, id_, entry->index());
                read_context_->AddReadIndex(entry->index(), commit_index, entry);
                //self is set active
                read_context_->SetVerifyResult(node_id_, entry->index(), true);
            }
            FLOG_DEBUG("node_id: {}, raft: {} consensus read and replicate more than 1, so broadcast to follower", node_id_, id_);
            //broadcast read request to follower
            bcastReadAppend();
        }
    }
}

void RaftFsm::stepLeader(MessagePtr& msg) {
    if (msg->type() == pb::LOCAL_MSG_PROP) {
        if (replicas_.find(node_id_) != replicas_.end() && msg->entries_size() > 0) {
            std::vector<EntryPtr> ents;
            takeEntries(msg, ents);
            auto li = raft_log_->lastIndex();
            for (auto& entry : ents) {
                entry->set_index(++li);
                entry->set_term(term_);
                if (entry->type() == pb::ENTRY_CONF_CHANGE) {
                    if (pending_conf_) {
                        entry->set_type(pb::ENTRY_NORMAL);
                        entry->clear_data();
                    }
                    pending_conf_ = true;
                } else if (entry->type() == pb::ENTRY_NORMAL && !entry->data().empty() &&
                        pending_entries_.size() < kMaxPendingEntries) {
                    pending_entries_.push_back(entry);
                    // TODO: discard entries if beyond pendings queue size?
                }
            }
            appendEntry(ents);
            bcastAppend();
        }
        return;
    }

    if (msg->type() == pb::LOCAL_MSG_READ_INDEX) {
        stepReadIndex(msg);
        return;
    }

    auto replica = getReplica(msg->from());
    if (replica == nullptr) {
        FLOG_WARN("node_id: {}, raft[{}] stepLeader no progress available for {}", node_id_, id_, msg->from());
        return;
    }

    Replica& pr = *replica;
    pr.set_active();
    pr.setAlive(true);

    switch (msg->type()) {
        case pb::APPEND_ENTRIES_RESPONSE:
            if (msg->reject()) {
                FLOG_DEBUG("node_id: {}, raft[{}] received msgApp rejection(lastindex:{}) from {} for index {}",
                           node_id_, id_, msg->reject_hint(), msg->from(), msg->log_index());

                if (pr.maybeDecrTo(msg->log_index(), msg->reject_hint(), msg->commit())) {
                    if (pr.state() == ReplicaState::kReplicate) {
                        pr.becomeProbe();
                    }
                    sendAppend(msg->from(), pr);
                }
            } else {
                bool old_paused = pr.isPaused();
                if (pr.maybeUpdate(msg->log_index(), msg->commit())) {
                    switch (pr.state()) {
                        case ReplicaState::kProbe:
                            pr.becomeReplicate();
                            break;
                        case ReplicaState::kReplicate:
                            pr.inflight().freeTo(msg->log_index());
                            break;
                        case ReplicaState::kSnapshot:
                            if (pr.needSnapshotAbort()) {
                                FLOG_INFO("node_id: {}, raft[{}] snapshot aborted, resumed sending replication to {}",
                                          node_id_, id_, msg->from());
                                if (sending_snap_ &&
                                    sending_snap_->GetContext().to == msg->from()) {
                                    sending_snap_->Cancel();
                                    sending_snap_.reset();
                                }
                                pr.becomeProbe();
                            }
                            break;
                    }
                    if (maybeCommit()) {
                        bcastAppend();
                    } else if (old_paused) {
                        sendAppend(msg->from(), pr);
                    }
                }
            }
            return;
        case pb::READ_INDEX_RESPONSE:
            //implements process read index response
            if (msg->reject()) {
                FLOG_DEBUG("raft: {}, seq: {}, node_id: {} is rejected", id_, msg->read_sequence(), msg->from());
                read_context_->SetVerifyResult(msg->from(), msg->read_sequence(), false);
                return;
            } else {
                FLOG_DEBUG("raft: {}, seq: {}, node_id: {} is accepted", id_, msg->read_sequence(), msg->from());
                pr.set_read_match(msg->read_sequence());
                read_context_->SetVerifyResult(msg->from(), msg->read_sequence(), true);
                // try to process read request
                read_context_->ProcessBySequence(msg->read_sequence(), raft_log_->applied(), GetPeers().size());
            }
            return;
        case pb::HEARTBEAT_RESPONSE:
            pr.resume();
            if (pr.state() == ReplicaState::kReplicate && pr.inflight().full()) {
                pr.inflight().freeFirstOne();
            }

            if (pr.match() < raft_log_->lastIndex() ||
                pr.committed() < raft_log_->committed()) {
                sendAppend(msg->from(), pr);
            }
            //send pending read index request to follower
            if (read_context_->ReadRequestSize() > 0) {
                if (pr.read_match() != read_context_->LastSeq()) {
                    sendReadAppend(msg->from(), pr);
                }
            }
            return;

        case pb::SNAPSHOT_ACK:
            if (sending_snap_ &&
                sending_snap_->GetContext().uuid == msg->snapshot().uuid()) {
                FLOG_DEBUG("node_id: {}, raft[{}] recv snapshot: {}, ack seq: {}, reject: {}",
                           node_id_, id_, msg->snapshot().uuid(), msg->snapshot().seq(), msg->reject());

                auto s = sending_snap_->RecvAck(msg);
                if (!s.ok()) {
                    FLOG_WARN("node_id: {}, raft[{}] ack seq: {}, reject: {}, from: {} snapshot: {} failed: {}",
                              node_id_, id_, msg->snapshot().seq(), msg->reject(), msg->from(),
                              msg->snapshot().uuid(), s.ToString());
                }
            }

            return;

        case pb::LOCAL_SNAPSHOT_STATUS:
            if (!sending_snap_ ||
                sending_snap_->GetContext().uuid != msg->snapshot().uuid()) {
                return;
            }
            sending_snap_.reset();

            if (pr.state() != ReplicaState::kSnapshot) {
                return;
            }

            if (msg->reject()) {
                FLOG_WARN("node_id: {}, raft[{}] send snapshot to [{}] failed.", node_id_, id_, msg->from());
                pr.snapshotFailure();
                pr.becomeProbe();
            } else {
                pr.becomeProbe();
                FLOG_WARN("node_id: {}, raft[{}] send snapshot to [{}] succeed, resumed replication [{}]",
                          node_id_, id_, msg->from(), pr.ToString());
            }

            pr.pause();
            return;
        default:
            return;
    }
}

bool RaftFsm::activeQuorum() const {
    int active_num = 0;
    for (auto it = replicas_.begin(); it != replicas_.end(); it++) {
        if (it->first == node_id_) {
            active_num++;
            continue;
        }

        if (it->second->alive()) {
            active_num++;
        }
    }
    return active_num >= quorum();
}

void RaftFsm::tickHeartbeat() {
    ++heartbeat_elapsed_;
    ++election_elapsed_;
    if (state_ != FsmState::kLeader) {
        return;
    }

    if (election_elapsed_ >= sops_.election_tick) {
        election_elapsed_ = 0;
        if (!activeQuorum()) {
            FLOG_WARN("node: {}, raft: {}, network exception, change to follower", node_id_, id_);
            becomeFollower(term_, 0);
            return;
        }
        resetAlive();
    }

    traverseReplicas([this](uint64_t node, Replica& pr) {
        if (node != node_id_) {
            pr.incr_inactive_tick();
        }
    });

    if (heartbeat_elapsed_ >= sops_.heartbeat_tick) {
        heartbeat_elapsed_ = 0;

        if (sops_.auto_promote_learner && !learners_.empty() && !pending_conf_) {
            checkCaughtUp();
        }
    }
}

bool RaftFsm::maybeCommit() {
    std::vector<uint64_t> matches;
    matches.reserve(replicas_.size());
    for (const auto& r : replicas_) {
        matches.push_back(r.second->match());
    }
    std::sort(matches.begin(), matches.end(), std::greater<uint64_t>());

    uint64_t mid = matches[quorum() - 1];
    bool is_commit = raft_log_->maybeCommit(mid, term_);
    if (state_ == FsmState::kLeader) {
        auto it = replicas_.find(node_id_);
        if (it != replicas_.end()) {
            it->second->set_committed(raft_log_->committed());
        }
    }
    return is_commit;
}

void RaftFsm::bcastAppend() {
    traverseReplicas([this](uint64_t node, Replica& pr) {
        if (node != node_id_) this->sendAppend(node, pr);
    });
}

void RaftFsm::bcastReadAppend() {
    traverseReplicas([this](uint64_t node, Replica& pr) {
        if (node != node_id_) this->sendReadAppend(node, pr);
    });
}

void RaftFsm::sendAppend(uint64_t to, Replica& pr) {
    assert(to == pr.peer().node_id);

    if (pr.isPaused()) {
        return;
    }

    if (pr.inactive_ticks() > sops_.inactive_tick) {
        pr.becomeProbe();
        pr.pause();
        return;
    }

    Status ts, es;
    uint64_t term = 0;
    std::vector<EntryPtr> ents;

    uint64_t fi = raft_log_->firstIndex();
    if (pr.next() >= fi) {
        ts = raft_log_->term(pr.next() - 1, &term);
        es = raft_log_->entries(pr.next(), sops_.max_size_per_msg, &ents);
    }

    // check if replica has no history entries, issue a snapshot
    auto bare = pr.next() <= 1 && pr.next() < raft_log_->committed();
    if (bare || pr.next() < fi || !ts.ok() || !es.ok()) {
        FLOG_INFO("node_id: {}, raft[{}] need snapshot to {}[next:{}], fi:{}, committed:{}, log error:{}-{}",
                  node_id_, id_, to, pr.next(), fi, raft_log_->committed(), ts.ToString(), es.ToString());

        if (sending_snap_) {
            FLOG_WARN("node_id: {}, raft[{}] sendAppend could not send snapshot to {}(other snapshot[{}] is sending)",
                      node_id_, id_, to, sending_snap_->GetContext().uuid);
        } else {
            uint64_t snap_index = 0;
            sending_snap_ = newSendSnapTask(to, &snap_index);
            pr.becomeSnapshot(snap_index);
        }
    } else {
        MessagePtr msg(new pb::Message);
        msg->set_type(pb::APPEND_ENTRIES_REQUEST);
        msg->set_to(to);
        msg->set_log_index(pr.next() - 1);  // prev log index
        msg->set_log_term(term);            // prev log term
        msg->set_commit(raft_log_->committed());
        putEntries(msg, ents);

        if (msg->entries_size() > 0) {
            switch (pr.state()) {
                case ReplicaState::kReplicate: {
                    uint64_t last = msg->entries(msg->entries_size() - 1).index();
                    pr.update(last);
                    pr.inflight().add(last);
                    break;
                }
                case ReplicaState::kProbe:
                    pr.pause();
                    break;
                case ReplicaState::kSnapshot:
                    throw RaftException(
                        std::string("[repl->sendAppend][%v] is sending append "
                                    "in unhandled state ") +
                        ReplicateStateName(pr.state()));
            }
        }
        send(msg);
    }
}

void RaftFsm::sendReadAppend(uint64_t to, Replica& pr) {
    assert(to == pr.peer().node_id);

    if (pr.isPaused()) {
        return;
    }

    if (read_context_->ReadRequestSize() == 0) {
        return;
    }
    if (pr.inactive_ticks() > sops_.inactive_tick) {
        pr.becomeProbe();
        pr.pause();
        return;
    }
    if (pr.state() != ReplicaState::kReplicate) {
        FLOG_WARN("raft: {}, replica: {} state is not kReplicate, don't send read index", id_, to);
        return;
    }

    uint64_t last_seq = read_context_->LastSeq();
    auto read_state = read_context_->GetReadIndexState(last_seq);
    if (read_state == nullptr) {
        FLOG_CRIT("don't get read state: {} by read context, not expected", last_seq);
        return;
    }

    MessagePtr msg(new pb::Message);
    msg->set_type(pb::READ_INDEX_REQUEST);
    msg->set_to(to);
    msg->set_read_sequence(read_state->Seq());
    msg->set_log_index(pr.next() - 1);  // prev log index
    msg->set_log_term(term_);            // prev log term
    msg->set_commit(read_state->CommitIndex());
    send(msg);
}

void RaftFsm::appendEntry(const std::vector<EntryPtr>& ents) {
    FLOG_DEBUG("node_id: {}, raft[{}] append log entry. index: {}, size: {}",
               node_id_, id_, ents[0]->index(), ents.size());

    raft_log_->append(ents);
    replicas_[node_id_]->maybeUpdate(raft_log_->lastIndex(), raft_log_->committed());
    maybeCommit();
}

static uint64_t unixNano() {
    auto now = std::chrono::system_clock::now();
    auto count = std::chrono::time_point_cast<std::chrono::nanoseconds>(now).time_since_epoch().count();
    return static_cast<uint64_t>(count);
}

std::shared_ptr<SendSnapTask> RaftFsm::newSendSnapTask(uint64_t to,
                                                       uint64_t* snap_index) {
    SnapContext snap_ctx;
    snap_ctx.id = id_;
    snap_ctx.to = to;
    snap_ctx.from = node_id_;
    snap_ctx.term = term_;
    snap_ctx.uuid = unixNano();

    pb::SnapshotMeta snap_meta;
    traverseReplicas([&snap_meta](uint64_t node, const Replica& pr) {
        auto p = snap_meta.add_peers();
        auto s = EncodePeer(pr.peer(), p);
        if (!s.ok()) {
            throw RaftException(std::string("create snapshot failed: ") + s.ToString());
        }
    });

    auto snap_data = sm_->GetSnapshot();
    if (snap_data == nullptr) {
        throw RaftException("raft->sendAppend failed to send snapshot, because "
                            "snapshot is unavailable");
    } else if (snap_data->ApplyIndex() < raft_log_->firstIndex() - 1) {
        std::ostringstream ss;
        ss << "raft->sendAppend[" << id_
           << "]failed to send snapshot, because snapshot is invalid(apply="
           << snap_data->ApplyIndex() << ", first=" << raft_log_->firstIndex() << ").";
        throw RaftException(ss.str());
    }

    // set meta index
    snap_meta.set_index(snap_data->ApplyIndex());
    // set meta term
    uint64_t snap_term = 0;
    auto status = raft_log_->term(snap_data->ApplyIndex(), &snap_term);
    if (!status.ok()) {
        std::ostringstream ss;
        ss << "[raft->sendAppend][" << id_ << "] failed to send snapshot to " << to
           << " because snapshot is unavailable, error is: " << status.ToString();
        throw RaftException(ss.str());
    }
    snap_meta.set_term(snap_term);

    // set user context
    std::string context;
    auto s = snap_data->Context(&context);
    if (!s.ok()) {
        throw RaftException(std::string("get snapshot user context failed: ") +
                            s.ToString());
    }
    snap_meta.set_context(std::move(context));

    *snap_index = snap_data->ApplyIndex();

    FLOG_DEBUG("node_id: {}, raft[{}] sendAppend [firstindex: {}, commit: {}] sent "
               "snapshot[index: {}, term: {}] to [{}]",
               node_id_, id_, raft_log_->firstIndex(), raft_log_->committed(),
               snap_data->ApplyIndex(), snap_term, to);

    return std::make_shared<SendSnapTask>(snap_ctx, std::move(snap_meta), snap_data);
}

void RaftFsm::checkCaughtUp() {
    Peer max_peer;
    uint64_t max_match = 0;
    for (const auto& p : learners_) {
        const auto& pr = *p.second;
        if (pr.match() >= max_match) {
            max_match = pr.match();
            max_peer = pr.peer();
        }
    }

    auto lasti = raft_log_->lastIndex();
    auto precent_threshold = (sops_.promote_gap_percent * lasti) / 100;
    uint64_t final_threshold = std::max(sops_.promote_gap_threshold, precent_threshold);

    assert(lasti >= max_match);
    if (lasti - max_match < final_threshold) {
        FLOG_INFO("node_id: {}, raft[{}] start promote learner {} [match:{}, lasti:{}] at term {}.",
                  node_id_, id_, max_peer.ToString(), max_match, lasti, term_);

        max_peer.type = PeerType::kNormal;
        ConfChange cc;
        cc.type = ConfChangeType::kPromote;
        cc.peer = max_peer;
        std::string str;
        auto s = EncodeConfChange(cc, &str);
        if (!s.ok()) {
            throw RaftException(std::string("promote caughtup learner failed:") +
                                s.ToString());
        }

        auto msg = std::make_shared<pb::Message>();
        msg->set_type(pb::LOCAL_MSG_PROP);
        auto entry = msg->add_entries();
        entry->set_type(pb::ENTRY_CONF_CHANGE);
        entry->mutable_data()->swap(str);
        Step(msg);
    }
}

} /* namespace impl */
} /* namespace raft */
} /* namespace jimkv */
