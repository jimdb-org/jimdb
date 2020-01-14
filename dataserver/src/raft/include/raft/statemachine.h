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

#include <memory>
#include <vector>
#include "base/status.h"
#include "raft/snapshot.h"
#include "raft/types.h"

namespace jim {
namespace raft {

class StateMachine {
public:
    StateMachine() = default;
    virtual ~StateMachine() = default;

    StateMachine(const StateMachine&) = delete;
    StateMachine& operator=(const StateMachine&) = delete;

    virtual Status Apply(const std::string& cmd, uint64_t index) = 0;
    virtual Status ApplyMemberChange(const ConfChange& cc, uint64_t index) = 0;

    virtual Status ApplyReadIndex(const std::string& cmd, uint16_t verify_result) = 0;

    virtual uint64_t PersistApplied() = 0;

    virtual void OnReplicateError(const std::string& cmd, const Status& status) = 0;

    virtual void OnLeaderChange(uint64_t leader, uint64_t term) = 0;

    virtual std::shared_ptr<Snapshot> GetSnapshot() = 0;

    virtual Status ApplySnapshotStart(const std::string& context, uint64_t index) = 0;
    virtual Status ApplySnapshotData(const std::vector<std::string>& datas) = 0;
    virtual Status ApplySnapshotFinish(uint64_t index) = 0;
};

} /* namespace raft */
} /* namespace jim */
