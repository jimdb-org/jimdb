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

_Pragma("once");

#include <future>
#include <unordered_map>

#include "raft/raft.h"
#include "raft/server.h"

namespace jim {
namespace raft {
namespace bench {

class NodeAddress;

class Range : public raft::StateMachine,
              public std::enable_shared_from_this<Range> {
public:
    Range(uint64_t id, uint64_t node_id, RaftServer* rs,
          const std::shared_ptr<NodeAddress>& addr_mgr);
    ~Range();

    void Start();

    void WaitLeader();
    bool IsLeader() const { return leader_ == node_id_; }
    void SyncRequest();
    std::shared_future<bool> AsyncRequest();
    void Destroy();

public:
    Status Apply(const std::string& cmd, uint64_t index) override;

    void OnLeaderChange(uint64_t leader, uint64_t term) override { leader_ = leader; }
    void OnReplicateError(const std::string& cmd, const Status& status) override {}
    Status ApplyMemberChange(const ConfChange&, uint64_t) override {
        return Status::OK();
    }
    std::shared_ptr<raft::Snapshot> GetSnapshot() override { return nullptr; }
    Status ApplySnapshotStart(const std::string&, uint64_t index) override {
        return Status(Status::kNotSupported);
    }
    Status ApplySnapshotData(const std::vector<std::string>&) override {
        return Status(Status::kNotSupported);
    }
    Status ApplySnapshotFinish(uint64_t) override {
        return Status(Status::kNotSupported);
    }

    uint64_t PersistApplied() override { return 0; }

    Status ApplyReadIndex(const std::string& cmd, uint16_t verify_result) override {
        return Status(Status::kNotSupported);
    }

private:
    class RequestQueue {
    public:
        uint64_t add(std::shared_future<bool>* f);
        void set(uint64_t seq, bool value);
        void remove(uint64_t seq);

    private:
        std::unordered_map<uint64_t, std::promise<bool>> que_;
        std::mutex mu_;
        uint64_t seq_ = 0;
    };

private:
    const uint64_t id_ = 0;
    const uint64_t node_id_ = 0;
    RaftServer* raft_server_ = nullptr;
    std::shared_ptr<NodeAddress> addr_mgr_;

    std::shared_ptr<Raft> raft_;
    uint64_t leader_ = 0;

    RequestQueue request_queue_;
};

} /* namespace bench */
} /* namespace raft */
} /* namespace jim */
