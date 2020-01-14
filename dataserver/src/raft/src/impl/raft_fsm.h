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

_Pragma("once");

#include <list>
#include <functional>

#include "raft/options.h"
#include "raft/status.h"
#include "raft_log.h"
#include "raft_read_context.h"
#include "raft_types.h"
#include "replica.h"
#include "mutable_options.h"

namespace jim {
namespace raft {
namespace impl {

struct Ready;
class SendSnapTask;
class ApplySnapTask;

class RaftFsm {
public:
    RaftFsm(const RaftServerOptions& sops, ServerMutableOptions* mutable_sops, const RaftOptions& ops);
    ~RaftFsm() = default;

    RaftFsm(const RaftFsm&) = delete;
    RaftFsm& operator=(const RaftFsm&) = delete;

    bool Validate(MessagePtr& msg) const;
    void Step(MessagePtr& msg);

    // reset and return
    void GetReady(Ready* rd);

    std::tuple<uint64_t, uint64_t> GetLeaderTerm() const;

    pb::HardState GetHardState() const;
    Status Persist(bool persist_hardstate);

    std::vector<Peer> GetPeers() const;
    RaftStatus GetStatus() const;

    Status TruncateLog(uint64_t index);
    Status DestroyLog(bool backup);

    Status ReadProcess(const EntryPtr& e, uint16_t verify_result);
    void readErrorProcess();
private:
    static int numOfPendingConf(const std::vector<EntryPtr>& ents);
    static void takeEntries(MessagePtr& msg, std::vector<EntryPtr>& ents);
    static void putEntries(MessagePtr& msg, const std::vector<EntryPtr>& ents);

    Status start();
    Status loadState(const pb::HardState& state);
    Status smApply(const EntryPtr& e);
    Status applyConfChange(const EntryPtr& e);
    Status recoverCommit();

    bool stepIgnoreTerm(MessagePtr& msg);
    void stepLowTerm(MessagePtr& msg);
    void stepVote(MessagePtr& msg, bool pre_vote);

    bool hasReplica(uint64_t node) const;
    Replica* getReplica(uint64_t node) const;
    std::unique_ptr<Replica> newReplica(const Peer& peer, bool is_leader) const;
    void traverseReplicas(const std::function<void(uint64_t, Replica&)>& f) const;

    void addPeer(const Peer& peer);
    void removePeer(const Peer& peer);
    void promotePeer(const Peer& peer);
    int quorum() const;

    void send(MessagePtr& msg);

    void reset(uint64_t term, bool is_leader);
    void resetRandomizedElectionTimeout();
    bool pastElectionTimeout() const;

    void abortSendSnap();
    void abortApplySnap();

    bool electable() const;

    void appliedTo(uint64_t index);

    // truncate raft log storage
    void truncateLog();

private:
    void becomeLeader();
    void stepReadIndex(MessagePtr& msg);
    void stepLeader(MessagePtr& msg);
    void tickHeartbeat();
    bool maybeCommit();
    void bcastAppend();
    void bcastReadAppend();
    bool activeQuorum() const;
    void sendAppend(uint64_t to, Replica& pr);
    void sendReadAppend(uint64_t to, Replica& pr);
    void appendEntry(const std::vector<EntryPtr>& ents);
    std::shared_ptr<SendSnapTask> newSendSnapTask(uint64_t to, uint64_t* snap_index);
    void checkCaughtUp();
    void resetAlive();

private:
    void becomeCandidate();
    void becomePreCandidate();
    void stepCandidate(MessagePtr& msg);
    void campaign(bool pre);
    int poll(bool pre, uint64_t node_id, bool vote);

private:
    void becomeFollower(uint64_t term, uint64_t leader);
    void stepFollower(MessagePtr& msg);
    void tickElection();
    void handleAppendEntries(MessagePtr& msg);
    void handleReadRequest(MessagePtr& msg);
    void handleSnapshot(MessagePtr& msg);
    Status applySnapshot(MessagePtr& msg);
    bool checkSnapshot(const pb::SnapshotMeta& meta);
    Status restore(const pb::SnapshotMeta& meta);

private:
    friend class RaftImpl;

    using TimePoint = std::chrono::time_point<std::chrono::steady_clock>;

    const RaftServerOptions sops_;
    ServerMutableOptions* mutable_sops_ = nullptr;
    RaftOptions rops_;
    const uint64_t node_id_ = 0;
    const uint64_t id_ = 0; //raft id
    std::shared_ptr<StateMachine> sm_;

    bool is_learner_ = false;
    FsmState state_ = FsmState::kFollower;
    uint64_t leader_ = 0;//node_id for leader
    uint64_t term_ = 0;
    uint64_t vote_for_ = 0; //node_id for vote
    bool pending_conf_ = false;
    std::shared_ptr<storage::Storage> storage_;
    std::unique_ptr<RaftLog> raft_log_;

    std::map<uint64_t, bool> votes_;
    std::map<uint64_t, std::unique_ptr<Replica>> replicas_;  // normal replicas
    std::map<uint64_t, std::unique_ptr<Replica>> learners_;  // learner replicas

    unsigned election_elapsed_ = 0;
    unsigned heartbeat_elapsed_ = 0;
    unsigned rand_election_tick_ = 0;
    std::function<unsigned()> random_func_;
    std::function<void(MessagePtr&)> step_func_;
    std::function<void()> tick_func_;

    std::vector<MessagePtr> sending_msgs_;
    std::shared_ptr<SendSnapTask> sending_snap_;

    std::shared_ptr<ApplySnapTask> applying_snap_;
    pb::SnapshotMeta applying_meta_;

    uint64_t read_index_seq_ = 0;
    std::unique_ptr<ReadIndexContext> read_context_; //pending read request

    std::deque<EntryPtr> pending_entries_; // pending entries submitted by user on leader
    std::deque<EntryPtr> failed_entries_;
};

} /* namespace impl */
} /* namespace raft */
} /* namespace jim */
