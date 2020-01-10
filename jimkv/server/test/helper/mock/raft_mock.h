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

_Pragma("once");

#include "raft/raft.h"
#include "raft/server.h"
#include "raft/options.h"

using namespace jimkv;
using namespace jimkv::raft;

class RaftMock : public Raft {
public:
    explicit RaftMock(const RaftOptions& ops) : ops_(ops) {}

    bool IsStopped() const override { return false; }

    void SetLeaderTerm(uint64_t leader, uint64_t term) {
        leader_ = leader;
        term_ = term;
    }

    void GetLeaderTerm(uint64_t* leader, uint64_t* term) const override {
        *leader = leader_;
        *term = term_;
    }

    bool IsLeader() const override {
        // current node is 1
        return leader_ == 1;
    }

    Status TryToLeader() override { return Status::OK(); }

    Status Propose(std::string& entry_data, uint32_t entry_flags) override {
        ops_.statemachine->Apply(entry_data, 1);
        return Status::OK();
    }

    Status ReadIndex(std::string& ctx) override {
        ops_.statemachine->ApplyReadIndex(ctx, jimkv::raft::READ_SUCCESS);
        return Status::OK();
    }

    Status ChangeMemeber(const ConfChange& conf) override { return Status::OK(); }
    void GetStatus(RaftStatus* status) const override {}
    void Truncate(uint64_t index) override {}

    std::unique_ptr<LogReader> ReadLog(uint64_t start_index) override { return nullptr; }

    Status InheritLog(const std::string& dest_dir, uint64_t last_index, bool only_index) override {
        return Status(Status::kNotSupported);
    }

private:
    RaftOptions ops_;
    uint64_t leader_ = 0;
    uint64_t term_ = 0;
};

class RaftServerMock : public raft::RaftServer {
public:
    Status Start() override { return Status::OK(); }
    Status Stop() override { return Status::OK(); }
    const RaftServerOptions& Options() const { return rop_; }

    Status CreateRaft(const RaftOptions& ops, std::shared_ptr<Raft>* raft) override {
        auto r = std::make_shared<RaftMock>(ops);
        *raft = std::static_pointer_cast<Raft>(r);
        return Status::OK();
    }

    Status RemoveRaft(uint64_t id) override { return Status::OK(); }

    Status DestroyRaft(uint64_t id, bool backup) override { return Status::OK(); }

    std::shared_ptr<Raft> FindRaft(uint64_t id) const override {
        return std::shared_ptr<Raft>(nullptr);
    }

    void GetStatus(ServerStatus* status) const override {}

    void PostToAllApplyThreads(const std::function<void()>& task) override {}

    Status SetOptions(const std::map<std::string, std::string>& options) override {
        return Status(Status::kNotSupported);
    }

private:
    RaftServerOptions rop_;
};
