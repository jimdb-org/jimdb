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

_Pragma("once");

#include <list>
#include <future>

#include "raft/options.h"
#include "raft/raft.h"

#include "bulletin_board.h"
#include "raft_context.h"
#include "raft_types.h"
#include "ready.h"

namespace jimkv {
namespace raft {
namespace impl {

class RaftFsm;
struct SnapContext;
struct SnapResult;

class RaftImpl : public Raft, public std::enable_shared_from_this<RaftImpl> {
public:
    RaftImpl(const RaftServerOptions& sops, const RaftOptions& ops,
             const RaftContext& context);
    ~RaftImpl();

    RaftImpl(const RaftImpl&) = delete;
    RaftImpl& operator=(const RaftImpl&) = delete;

    void Stop();
    bool IsStopped() const override { return stopped_; }

    Status TryToLeader() override;

    Status Propose(std::string& entry_data, uint32_t entry_flags) override;

    Status ReadIndex(std::string& ctx) override;

    Status ChangeMemeber(const ConfChange& conf) override;

    bool IsLeader() const override { return sops_.node_id == bulletin_board_.Leader(); }

    void GetLeaderTerm(uint64_t* leader, uint64_t* term) const override {
        bulletin_board_.LeaderTerm(leader, term);
    }

    void GetStatus(RaftStatus* status) const override { bulletin_board_.Status(status); }

    void GetPeers(std::vector<Peer>* peers) const { bulletin_board_.Peers(peers); }

    void Truncate(uint64_t index) override;

    Status Destroy(bool backup);

    std::unique_ptr<LogReader> ReadLog(uint64_t start_index) override;

    Status InheritLog(const std::string& dest_dir, uint64_t last_index, bool only_index) override;

public:
    void RecvMsg(MessagePtr msg);
    void Tick(MessagePtr msg);
    void Step(MessagePtr msg);

    // notify snapshot work(apply or send) result
    // will be called from the snapshot work threads
    void ReportSnapSendResult(const SnapContext& ctx, const SnapResult& result);
    void ReportSnapApplyResult(const SnapContext& ctx, const SnapResult& result);

private:
    void initPublish();

    // wrap will bind a share_from_this() along with the function `f`,
    // so it's safe for `f` to bind a member function with the RaftImpl's `this` pointer
    // wrapWork will also check if RaftImpl is stopped before call `f`,
    // and handle RaftException
    Work wrapWork(const std::function<void()>& f);

    void post(const std::function<void()>& f);
    bool tryPost(const std::function<void()>& f);

    void smApply(const EntryPtr& e);

    void sendMessages();
    void sendSnapshot();
    void applySnapshot();

    void persist();
    void apply();
    void publish();

private:
    const RaftServerOptions sops_;
    const RaftOptions ops_;
    const RaftContext ctx_;

    std::atomic<bool> stopped_ = {false};

    BulletinBoard bulletin_board_;

    std::unique_ptr<RaftFsm> fsm_;

    Ready ready_;
    pb::HardState prev_hard_state_;
    bool conf_changed_ = false;
    std::atomic<uint64_t> tick_count_ = {0};

    std::deque<MessagePtr> propose_que_;
    std::mutex propose_lock_;

    std::deque<MessagePtr> read_index_que_;
    std::mutex read_index_lock_;
};

} /* namespace impl */
} /* namespace raft */
} /* namespace jimkv */
