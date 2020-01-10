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

#include "raft/statemachine.h"

namespace jimkv {
namespace raft {
namespace playground {

class PGStateMachine : public StateMachine {
public:
    PGStateMachine();
    ~PGStateMachine();

    uint64_t sum() const { return sum_; }

    Status Apply(const std::string& cmd, uint64_t index) override;
    Status ApplyMemberChange(const ConfChange& cc, uint64_t index) override;

    void OnReplicateError(const std::string& cmd,
                          const Status& status) override;

    void OnLeaderChange(uint64_t leader, uint64_t term) override;

    std::shared_ptr<Snapshot> GetSnapshot() override;

    Status ApplySnapshotStart(const std::string&) override;
    Status ApplySnapshotData(const std::vector<std::string>& data) override;
    virtual Status ApplySnapshotFinish(uint64_t index) override;

private:
    uint64_t sum_{0};
    uint64_t applied_{0};
};

class PGSnapshot : public Snapshot {
public:
    PGSnapshot(uint64_t sum, uint64_t applied) : sum_(sum), applied_(applied) {}

    ~PGSnapshot() {}

    Status Next(std::string* data, bool* over) override  {
        data->assign(std::to_string(sum_));
        *over = count_ >= 10;
        ++count_;
        return Status::OK();
    }

    Status Context(std::string*) override { return Status::OK(); }

    uint64_t ApplyIndex() override { return applied_; }

    void Close() override {}

private:
    uint64_t sum_{0};
    uint64_t applied_{0};
    uint64_t count_{0};
};

} /* namespace playground */
} /* namespace raft */
} /* namespace jimkv */
