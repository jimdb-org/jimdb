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

#include <random>
#include <sstream>

#include "common/logger.h"
#include "raft_exception.h"
#include "ready.h"
#include "replica.h"
#include "storage/storage_disk.h"
#include "storage/storage_memory.h"

namespace jimkv {
namespace raft {
namespace impl {

RaftFsm::RaftFsm(const RaftServerOptions& sops, ServerMutableOptions* mutable_sops, const RaftOptions& ops):
    sops_(sops),
    mutable_sops_(mutable_sops),
    rops_(ops),
    node_id_(sops.node_id),
    id_(ops.id),
    sm_(ops.statemachine) {
    read_context_ = std::unique_ptr<ReadIndexContext>(new ReadIndexContext(this));
    auto s = start();
    if (!s.ok()) {
        throw RaftException(s);
    }
}

int RaftFsm::numOfPendingConf(const std::vector<EntryPtr>& ents) {
    int n = 0;
    for (const auto& ent : ents) {
        if (ent->type() == pb::ENTRY_CONF_CHANGE) ++n;
    }
    return n;
}

void RaftFsm::takeEntries(MessagePtr& msg, std::vector<EntryPtr>& ents) {
    int size = msg->entries_size();
    if (size > 0) {
        ents.reserve(size);
        for (int i = 0; i < size; ++i) {
            ents.emplace_back(std::make_shared<pb::Entry>());
            ents.back()->Swap(msg->mutable_entries(i));
        }
    }
}

void RaftFsm::putEntries(MessagePtr& msg, const std::vector<EntryPtr>& ents) {
    for (const auto& e : ents) {
        auto p = msg->add_entries();
        p->CopyFrom(*e);  // must copy
    }
}

Status RaftFsm::loadState(const pb::HardState& state) {
    if (state.commit() == 0 && state.term() == 0 && state.vote() == 0) {
        return Status::OK();
    }

    if (state.commit() < raft_log_->committed() ||
        state.commit() > raft_log_->lastIndex()) {
        std::stringstream ss;
        ss << "loadState: state.commit " << state.commit() << " is out of range ["
           << raft_log_->committed() << ", " << raft_log_->lastIndex() << "]";
        return Status(Status::kCorruption, ss.str(), "");
    }

    term_ = state.term();
    vote_for_ = state.vote();
    raft_log_->commitTo(state.commit());

    return Status::OK();
}

Status RaftFsm::recoverCommit() {
    Status s;
    while (raft_log_->applied() < raft_log_->committed()) {
        std::vector<EntryPtr> committed_entries;
        raft_log_->nextEntries(64 * 1024 * 1024, &committed_entries);
        for (const auto& entry : committed_entries) {
            raft_log_->appliedTo(entry->index());
            if (entry->type() == pb::ENTRY_CONF_CHANGE) {
                s = applyConfChange(entry);
                if (!s.ok()) {
                    return s;
                }
            }
            s = smApply(entry);
            if (!s.ok()) {
                return s;
            }
        }
    }
    return s;
}

Status RaftFsm::start() {
    unsigned seed = static_cast<unsigned>(
        std::chrono::system_clock::now().time_since_epoch().count() * node_id_);
    std::default_random_engine engine(seed);
    std::uniform_int_distribution<unsigned> distribution(sops_.election_tick,
                                                         sops_.election_tick * 2);
    random_func_ = std::bind(distribution, engine);

    if (rops_.use_memory_storage) {
        storage_ =
            std::shared_ptr<storage::Storage>(new storage::MemoryStorage(id_, 40960));
        FLOG_WARN("node_id: {}, raft[{}] use raft logger memory storage!", node_id_, id_);
    } else {
        storage::DiskStorage::Options ops;
        ops.log_file_size = rops_.log_file_size;
        ops.max_log_files = rops_.max_log_files;
        ops.allow_corrupt_startup = rops_.allow_log_corrupt;
        storage_ = std::shared_ptr<storage::Storage>(
            new storage::DiskStorage(id_, rops_.storage_path, ops));
    }
    auto s = storage_->Open();
    if (!s.ok()) {
        FLOG_ERROR("node_id: {}, raft[{}] open raft storage failed: {}", node_id_, id_, s.ToString());
        return Status(Status::kCorruption, "open raft logger", s.ToString());
    } else {
        raft_log_ = std::unique_ptr<RaftLog>(new RaftLog(id_, storage_));
    }

    pb::HardState hs;
    s = storage_->InitialState(&hs);
    if (!s.ok()) {
        return s;
    }
    s = loadState(hs);
    if (!s.ok()) {
        return s;
    }

    bool local_found = false;
    for (const auto& p : rops_.peers) {
        if (p.node_id == node_id_) {
            local_found = true;
            is_learner_ = (p.type == PeerType::kLearner);
        }
        if (p.type == PeerType::kNormal) {
            replicas_.emplace(p.node_id, newReplica(p, false));
        } else {
            learners_.emplace(p.node_id, newReplica(p, false));
        }
    }
    if (!local_found) {
        return Status(Status::kInvalidArgument, std::string("could not find local node(") +
                  std::to_string(node_id_) + ") in peers", PeersToString(rops_.peers));
    }

    FLOG_INFO("node_id: {} new raft[{}] {} commit: {}, applied: {}, lastindex: {}, peers: {}",
              node_id_, id_, (is_learner_ ? " [learner]" : ""), raft_log_->committed(),
             rops_.applied, raft_log_->lastIndex(), PeersToString(rops_.peers));

    if (rops_.applied > 0) {
        uint64_t lasti = raft_log_->lastIndex();
        if (lasti < rops_.applied) {
            raft_log_->set_committed(lasti);
            rops_.applied = lasti;
        } else if (raft_log_->committed() < rops_.applied) {
            raft_log_->set_committed(rops_.applied);
        }
        raft_log_->appliedTo(rops_.applied);
    }

    s = recoverCommit();
    if (!s.ok()) {
        return s;
    }

    do {
        if (rops_.leader == 0 || !mutable_sops_->Electable()) {
            becomeFollower(term_, 0);
            break;
        }
        if (rops_.term == 0 || rops_.term < term_) {
            becomeFollower(term_, 0);
            break;
        }
        if (rops_.leader != node_id_) {
            becomeFollower(rops_.term, rops_.leader);
            break;
        }
        if (is_learner_) {
            becomeFollower(term_, 0);
            break;
        }
        if (term_ != rops_.term) {
            term_ = rops_.term;
            vote_for_ = 0;
        }
        state_ = FsmState::kLeader;  // avoid invalid transition check
        becomeLeader();
    } while (false);

    return Status::OK();
}

bool RaftFsm::Validate(MessagePtr& msg) const {
    return IsLocalMsg(msg) || hasReplica(msg->from()) || !IsResponseMsg(msg);
}

bool RaftFsm::stepIgnoreTerm(MessagePtr& msg) {
    switch (msg->type()) {
        case pb::LOCAL_MSG_TICK:
            truncateLog();
            tick_func_();
            return true;

        case pb::LOCAL_MSG_HUP: {
            if (state_ != FsmState::kLeader && electable()) {
                std::vector<EntryPtr> ents;
                Status s = raft_log_->slice(raft_log_->applied() + 1,
                                            raft_log_->committed() + 1, kNoLimit, &ents);
                if (!s.ok()) {
                    throw RaftException(
                        std::string("[Step] unexpected error when getting "
                                    "unapplied entries:") +
                        s.ToString());
                }

                if (numOfPendingConf(ents) != 0) {
                    FLOG_INFO("node_id: {} raft[{}] pending conf exist. campaign forbidden", node_id_, id_);
                } else {
                    campaign(sops_.enable_pre_vote);
                }
            }
            return true;
        }

        case pb::LOCAL_MSG_PROP:
            step_func_(msg);
            return true;
        case pb::LOCAL_MSG_READ_INDEX:
            step_func_(msg);
            return true;
        case pb::HEARTBEAT_REQUEST:
            if (msg->from() == leader_ && msg->from() != node_id_) {
                step_func_(msg);
            }
            return true;

        case pb::HEARTBEAT_RESPONSE:
            if (state_ == FsmState::kLeader && msg->from() != node_id_) {
                step_func_(msg);
            }
            return true;

        default:
            return false;
    }
}

void RaftFsm::stepLowTerm(MessagePtr& msg) {
    if (msg->type() == pb::PRE_VOTE_REQUEST) {
        FLOG_INFO("node_id: {} raft[{}] [logterm: {}, index: {}, vote: {}] rejected low term {}"
                "from {} [logterm: {}, index: {}] at term {}.",
                  node_id_, id_, raft_log_->lastTerm(), raft_log_->lastIndex(), vote_for_,
                  MessageType_Name(msg->type()), msg->from(), msg->log_term(),
                  msg->log_index(), term_);

        MessagePtr resp(new pb::Message);
        resp->set_type(pb::PRE_VOTE_RESPONSE);
        resp->set_to(msg->from());
        resp->set_reject(true);
        send(resp);
    } else if (sops_.enable_pre_vote && msg->type() == pb::APPEND_ENTRIES_REQUEST) {
        FLOG_INFO("node_id: {} raft[{}] ignore a {} message from low term leader: {} term: {} at term: {}",
                  node_id_, id_, MessageType_Name(msg->type()), msg->from(), msg->term(), term_);

        MessagePtr resp(new pb::Message);
        resp->set_type(pb::APPEND_ENTRIES_RESPONSE);
        resp->set_to(msg->from());
        send(resp);
    } else {
        FLOG_INFO("node_id: {} raft[{}] ignore a {} message with lower term from node_id: {} term: {} at term {}",
                  node_id_, id_, MessageType_Name(msg->type()), msg->from(), msg->term(), term_);
    }
}

void RaftFsm::stepVote(MessagePtr& msg, bool pre_vote) {
    if (is_learner_) {
        FLOG_INFO("node_id: {} raft[{}] [logterm:{}, index:{}, vote:{}] ignore {} from "
                  "{}[logterm:{}, index:{}] at term {}: learner can not vote.",
                  node_id_, id_, raft_log_->lastTerm(), raft_log_->lastIndex(), vote_for_,
                  (pre_vote ? "pre-vote" : "vote"), msg->from(), msg->log_term(),
                  msg->log_term(), term_);
        return;
    }

    MessagePtr resp(new pb::Message);
    resp->set_type(pre_vote ? pb::PRE_VOTE_RESPONSE : pb::VOTE_RESPONSE);
    resp->set_to(msg->from());

    bool can_vote = (vote_for_ == msg->from()) || (vote_for_ == 0 && leader_ == 0) ||
                    (msg->type() == pb::PRE_VOTE_REQUEST && msg->term() > term_);

    if (can_vote && raft_log_->isUpdateToDate(msg->log_index(), msg->log_term())) {
        FLOG_INFO("node_id: {} raft[{}] [logterm:{}, index:{}, vote:{}] {} for "
                  "{}[logterm:{}, index:{}] at term {}",
                  node_id_, id_, raft_log_->lastTerm(), raft_log_->lastIndex(), vote_for_,
                  (pre_vote ? "pre-vote" : "vote"), msg->from(), msg->log_term(),
                  msg->log_index(), term_);

        resp->set_term(msg->term());
        resp->set_reject(false);
        send(resp);

        if (msg->type() == pb::VOTE_REQUEST) {
            vote_for_ = msg->from();
            election_elapsed_ = 0;
        }
    } else {
        FLOG_INFO("node_id: {} raft[{}] [logterm:{}, index:{}, vote:{}] {} for "
                  "{}[logterm:{}, index:{}] at term {}",
                  node_id_, id_, raft_log_->lastTerm(), raft_log_->lastIndex(), vote_for_,
                  (pre_vote ? "reject vote(pre)" : "reject vote"), msg->from(),
                  msg->log_term(), msg->log_index(), term_);

        resp->set_term(term_);
        resp->set_reject(true);
        send(resp);
    }
}

void RaftFsm::Step(MessagePtr& msg) {
    if (stepIgnoreTerm(msg)) {
        return;
    }

    if (msg->term() < term_) {
        stepLowTerm(msg);
        return;
    }

    if (msg->term() > term_) {
        if (msg->type() == pb::PRE_VOTE_REQUEST ||
            (msg->type() == pb::PRE_VOTE_RESPONSE && !msg->reject())) {
        } else {
            FLOG_INFO("node_id: {} raft[{}] received a [{}] message with higher term "
                      "from [node_id: {} term: {}] at term {}, become follower",
                      node_id_, id_, MessageType_Name(msg->type()), msg->from(), msg->term(), term_);

            if (msg->type() == pb::APPEND_ENTRIES_REQUEST ||
                msg->type() == pb::SNAPSHOT_REQUEST) {
                becomeFollower(msg->term(), msg->from());
            } else {
                becomeFollower(msg->term(), 0);
            }
        }
    }

    if (msg->type() == pb::PRE_VOTE_REQUEST) {
        stepVote(msg, true);
    } else if (msg->type() == pb::VOTE_REQUEST) {
        stepVote(msg, false);
    } else {
        step_func_(msg);
    }
}

void RaftFsm::GetReady(Ready* rd) {
    rd->committed_entries.clear();
    raft_log_->nextEntries(kNoLimit, &(rd->committed_entries));

    rd->failed_entries.clear();
    if (!failed_entries_.empty()) {
        rd->failed_entries = std::move(failed_entries_);
    }

    rd->msgs = std::move(sending_msgs_);

    if (sending_snap_ && !sending_snap_->IsDispatched()) {
        rd->send_snap = sending_snap_;
        sending_snap_->MarkAsDispatched();
    } else {
        rd->send_snap = nullptr;
    }

    if (applying_snap_ && !applying_snap_->IsDispatched()) {
        rd->apply_snap = applying_snap_;
        applying_snap_->MarkAsDispatched();
    } else {
        rd->apply_snap = nullptr;
    }
}

std::tuple<uint64_t, uint64_t> RaftFsm::GetLeaderTerm() const {
    return std::make_tuple(leader_, term_);
}

pb::HardState RaftFsm::GetHardState() const {
    pb::HardState hs;
    hs.set_term(term_);
    hs.set_vote(vote_for_);
    hs.set_commit(raft_log_->committed());
    return hs;
}

Status RaftFsm::Persist(bool persist_hardstate) {
    std::vector<EntryPtr> ents;
    raft_log_->unstableEntries(&ents);
    if (!ents.empty()) {
        auto s = storage_->StoreEntries(ents);
        if (!s.ok()) {
            return Status(Status::kIOError, "store entries", s.ToString());
        } else {
            raft_log_->stableTo(ents.back()->index(), ents.back()->term());
        }
    }
    if (persist_hardstate) {
        auto hs = GetHardState();
        auto s = storage_->StoreHardState(hs);
        if (!s.ok()) {
            return Status(Status::kIOError, "store hardstate", s.ToString());
        }
    }
    return Status::OK();
}

std::vector<Peer> RaftFsm::GetPeers() const {
    std::vector<Peer> peers;
    traverseReplicas([&](uint64_t id, const Replica& pr) { peers.push_back(pr.peer()); });
    return peers;
}

RaftStatus RaftFsm::GetStatus() const {
    RaftStatus s;
    s.node_id = sops_.node_id;
    s.leader = leader_;
    s.term = term_;
    s.index = raft_log_->lastIndex();
    s.commit = raft_log_->committed();
    s.applied = raft_log_->applied();
    s.state = FsmStateName(state_);
    if (state_ == FsmState::kLeader) {
        traverseReplicas([&](uint64_t node, const Replica& pr) {
            ReplicaStatus rs;
            rs.peer = pr.peer();
            rs.match = pr.match();
            rs.commit = pr.committed();
            rs.next = pr.next();
            // leader self is always active
            if (node != node_id_) {
                auto inactive_seconds = std::chrono::duration_cast<std::chrono::seconds>(
                        pr.inactive_ticks() * sops_.tick_interval) .count();
                rs.inactive_seconds = static_cast<int>(inactive_seconds);
            }
            rs.state = ReplicateStateName(pr.state());
            rs.snapshotting = pr.state() == ReplicaState::kSnapshot;
            s.replicas.emplace(node, rs);
        });
    }
    return s;
}

Status RaftFsm::TruncateLog(uint64_t index) {
    if (applying_snap_) return Status::OK();
    return storage_->Truncate(index);
}

Status RaftFsm::DestroyLog(bool backup) { return storage_->Destroy(backup); }

Status RaftFsm::smApply(const EntryPtr& entry) {
    Status s;
    switch (entry->type()) {
        case pb::ENTRY_NORMAL:
            if (!entry->data().empty()) {
                return sm_->Apply(entry->data(), entry->index());
            }
            break;

        case pb::ENTRY_CONF_CHANGE: {
            ConfChange cc;
            auto s = DecodeConfChange(entry->data(), &cc);
            if (!s.ok()) {
                FLOG_WARN("node_id: {}, raft[{}], term: {} decode conf change failed", node_id_, id_, term_);
                return s;
            }
            s = sm_->ApplyMemberChange(cc, entry->index());
            if (!s.ok()) {
                return s;
            }
            break;
        }
        default:
            return Status(Status::kInvalidArgument, "apply unknown raft entry type",
                          std::to_string(static_cast<int>(entry->type())));
    }
    return s;
}

Status RaftFsm::ReadProcess(const EntryPtr& entry, uint16_t verify_result) {
    if (!entry->data().empty()) {
        return sm_->ApplyReadIndex(entry->data(), verify_result);
    }
    return Status(Status::kInvalidArgument, "read unknown raft entry type",
                  std::to_string(static_cast<int>(entry->type())));
}

void RaftFsm::readErrorProcess() {
    std::deque<uint64_t> err_seqs;
    std::map<uint64_t, std::shared_ptr<ReadIndexState> > err_reqs;
    read_context_->savePendingRequest(err_seqs, err_reqs);
    for (auto it=err_reqs.begin(); it != err_reqs.end(); it++) {
        ReadProcess(it->second->Entry(), jimkv::raft::READ_FAILURE);
    }
}

Status RaftFsm::applyConfChange(const EntryPtr& e) {
    assert(e->type() == pb::ENTRY_CONF_CHANGE);

    FLOG_INFO("node_id: {}, raft[{}] apply confchange at index {}, term {}", node_id_, id_, e->index(), term_);

    pending_conf_ = false;

    ConfChange cc;
    auto s = DecodeConfChange(e->data(), &cc);
    if (!s.ok()) return s;

    if (cc.peer.node_id == 0) {  // invalid peer id
        return Status(Status::kInvalidArgument, "apply confchange invalid node id", "0");
    }

    switch (cc.type) {
        case ConfChangeType::kAdd:
            addPeer(cc.peer);
            break;
        case ConfChangeType::kRemove:
            removePeer(cc.peer);
            break;
        case ConfChangeType::kPromote:
            promotePeer(cc.peer);
            break;
        default:
            return Status(Status::kInvalidArgument, "apply confchange invalid cc type",
                          std::to_string(static_cast<int>(cc.type)));
    }

    return Status::OK();
}

std::unique_ptr<Replica> RaftFsm::newReplica(const Peer& peer, bool is_leader) const {
    if (is_leader) {
        auto r = std::unique_ptr<Replica>(new Replica(peer, sops_.max_inflight_msgs));
        auto lasti = raft_log_->lastIndex();
        r->set_next(lasti + 1);
        if (peer.node_id == node_id_) {
            r->set_match(lasti);
            r->set_committed(raft_log_->committed());
        }
        return r;
    } else {
        return std::unique_ptr<Replica>(new Replica(peer));
    }
}

void RaftFsm::traverseReplicas(const std::function<void(uint64_t, Replica&)>& f) const {
    for (const auto& pr : replicas_) {
        f(pr.first, *(pr.second));
    }
    for (const auto& pr : learners_) {
        f(pr.first, *(pr.second));
    }
}

bool RaftFsm::hasReplica(uint64_t node) const {
    return replicas_.find(node) != replicas_.cend() ||
           learners_.find(node) != learners_.cend();
}

Replica* RaftFsm::getReplica(uint64_t node) const {
    auto it = replicas_.find(node);
    if (it != replicas_.cend()) {
        return it->second.get();
    }
    it = learners_.find(node);
    if (it != learners_.cend()) {
        return it->second.get();
    }
    return nullptr;
}

void RaftFsm::addPeer(const Peer& peer) {
    FLOG_INFO("node_id: {}, raft[{}] add peer: {} at term {}", node_id_, id_, peer.ToString(), term_);

    auto old = getReplica(peer.node_id);
    if (old != nullptr) {
        FLOG_WARN("node_id: {}, raft[{}] add peer already exist: {}", node_id_, id_, old->peer().ToString());
        return;
    }

    // make replica
    auto replica = newReplica(peer, state_ == FsmState::kLeader);
    if (peer.type == PeerType::kLearner) {
        learners_.emplace(peer.node_id, std::move(replica));
    } else {
        replicas_.emplace(peer.node_id, std::move(replica));
    }
}

void RaftFsm::removePeer(const Peer& peer) {
    FLOG_INFO("node_id: {}, raft[{}] remove peer: {} at term {}", node_id_, id_, peer.ToString(), term_);

    auto replica = getReplica(peer.node_id);
    if (replica == nullptr) {
        FLOG_WARN("node_id: {}, raft[{}] remove peer doesn't exist: {}, ignore", node_id_, id_, peer.ToString());
        return;
    }

    if (replica->peer().peer_id != peer.peer_id) {
        FLOG_INFO("node_id: {}, raft[{}] ignore remove peer(inconsistent peer id), old: {}, to remove: {}",
                  node_id_, id_, replica->peer().ToString(), peer.ToString());
        return;
    }

    if (state_ == FsmState::kLeader && sending_snap_ &&
        sending_snap_->GetContext().to == peer.node_id) {
        sending_snap_->Cancel();
        sending_snap_.reset();
    }

    learners_.erase(peer.node_id);
    replicas_.erase(peer.node_id);
    if (peer.node_id == node_id_) {
        becomeFollower(term_, 0);
    } else if (state_ == FsmState::kLeader &&
               (!replicas_.empty() || !learners_.empty())) {
        if (maybeCommit()) {
            bcastAppend();
        }
    }
}

void RaftFsm::promotePeer(const Peer& peer) {
    FLOG_INFO("node_id: {}, raft[{}] promote peer: {} at term {}", node_id_, id_, peer.ToString(), term_);

    if (peer.type == PeerType::kLearner) {
        FLOG_WARN("node_id: {}, raft[{}] can't promote learner to a learner: {}, ignore", node_id_, id_, peer.ToString());
        return;
    }

    auto it = learners_.find(peer.node_id);
    if (it == learners_.end()) {
        FLOG_WARN("node_id: {}, raft[{}] promote learner doesn't exist: {}, ignore", node_id_, id_, peer.ToString());
        return;
    }

    auto old_peer = it->second->peer();
    if (old_peer.peer_id != peer.peer_id) {
        FLOG_INFO("node_id: {}, raft[{}] ignore promote learner(inconsistent peer id), old: {}, to promote: {}",
                  node_id_, id_, old_peer.ToString(), peer.ToString());
        return;
    }

    learners_.erase(peer.node_id);
    auto replica = newReplica(peer, state_ == FsmState::kLeader);
    replicas_.emplace(peer.node_id, std::move(replica));

    if (peer.node_id == node_id_) {
        is_learner_ = false;
    }
}

int RaftFsm::quorum() const { return replicas_.size() / 2 + 1; }

void RaftFsm::send(MessagePtr& msg) {
    msg->set_id(id_);
    msg->set_from(node_id_);

    if (msg->type() != pb::LOCAL_MSG_PROP && msg->term() == 0) {
        msg->set_term(term_);
    }

    sending_msgs_.push_back(msg);
}

void RaftFsm::resetAlive() {
    for (auto it = replicas_.begin(); it != replicas_.end(); it++) {
        it->second->setAlive(false);
    }
}

void RaftFsm::reset(uint64_t term, bool is_leader) {
    if (term_ != term) {
        term_ = term;
        vote_for_ = 0;
    }

    leader_ = 0;
    election_elapsed_ = 0;
    heartbeat_elapsed_ = 0;
    votes_.clear();
    pending_conf_ = false;

    // clear pending entries
    if (!pending_entries_.empty()) {
        failed_entries_ = std::move(pending_entries_);
        pending_entries_.clear();
    }
    readErrorProcess();
    abortSendSnap();
    abortApplySnap();

    // reset non-learner replicas
    auto old_replicas = std::move(replicas_);
    for (const auto& r : old_replicas) {
        assert(r.second->peer().type == PeerType::kNormal);
        replicas_.emplace(r.first, newReplica(r.second->peer(), is_leader));
    }

    // reset learner replicas
    auto old_learners = std::move(learners_);
    for (auto& r : old_learners) {
        assert(r.second->peer().type == PeerType::kLearner);
        learners_.emplace(r.first, newReplica(r.second->peer(), is_leader));
    }

    if (is_leader) {
        rand_election_tick_ = sops_.election_tick - 1;
    } else {
        resetRandomizedElectionTimeout();
    }
}

void RaftFsm::resetRandomizedElectionTimeout() {
    rand_election_tick_ = random_func_();
    FLOG_DEBUG("node_id: {}, raft[{}] election tick reset to {}", node_id_, id_, rand_election_tick_);
}

bool RaftFsm::pastElectionTimeout() const {
    return election_elapsed_ >= rand_election_tick_;
}

void RaftFsm::abortSendSnap() {
    if (sending_snap_) {
        sending_snap_->Cancel();
        sending_snap_.reset();
    }
}

void RaftFsm::abortApplySnap() {
    if (applying_snap_) {
        applying_snap_->Cancel();
        applying_snap_.reset();
    }
}

bool RaftFsm::electable() const {
    return replicas_.find(node_id_) != replicas_.cend() && mutable_sops_->Electable();
}

void RaftFsm::appliedTo(uint64_t index) {
    raft_log_->appliedTo(index);
    while (!pending_entries_.empty() && pending_entries_.front()->index() <= index) {
        pending_entries_.pop_front();
    }
}

void RaftFsm::truncateLog() {
    if (state_ != FsmState::kLeader) { // if not leader, truncate to persisted applied
        storage_->Truncate(sm_->PersistApplied());
        return;
    }

    // now we are leader,

    // check replica status
    bool snapshotting = false;
    uint64_t truncate_index = sm_->PersistApplied();
    traverseReplicas([&](uint64_t node, const Replica& pr) {
        if (node != node_id_) { // other replica
            if (pr.state() == ReplicaState::kSnapshot) {
                snapshotting = true;
            } else {
                truncate_index = std::min(truncate_index, pr.match());
            }
        }
    });

    // don't truncate when send snapshoting
    if (!snapshotting && truncate_index > 0) {
        FLOG_TRACE("raft[{}] truncate raft log to {}", id_, truncate_index);
        storage_->Truncate(truncate_index);
    }
}

} /* namespace impl */
} /* namespace raft */
} /* namespace jimkv */
