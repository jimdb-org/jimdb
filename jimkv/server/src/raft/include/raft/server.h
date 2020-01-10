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

#include <functional>
#include "base/status.h"

#include "options.h"
#include "status.h"

namespace jimkv {
namespace raft {

class Raft;

class RaftServer {
public:
    RaftServer() = default;
    virtual ~RaftServer() = default;

    RaftServer(const RaftServer&) = delete;
    RaftServer& operator=(const RaftServer&) = delete;

    virtual Status Start() = 0;
    virtual Status Stop() = 0;

    virtual Status CreateRaft(const RaftOptions&, std::shared_ptr<Raft>* raft) = 0;

    virtual Status RemoveRaft(uint64_t id) = 0;

    virtual Status DestroyRaft(uint64_t id, bool backup) = 0;

    virtual std::shared_ptr<Raft> FindRaft(uint64_t id) const = 0;

    virtual void GetStatus(ServerStatus* status) const = 0;

    virtual Status SetOptions(const std::map<std::string, std::string>& options) = 0;

    // Post a task to all apply threads to run, task should never throw an exception
    virtual void PostToAllApplyThreads(const std::function<void()>& task) = 0;
};

std::unique_ptr<RaftServer> CreateRaftServer(const RaftServerOptions& ops);

} /* namespace raft */
} /* namespace jimkv */
