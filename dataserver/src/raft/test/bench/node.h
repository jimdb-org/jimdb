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

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "raft/raft.h"
#include "raft/server.h"
#include "range.h"

namespace jim {
namespace raft {
namespace bench {

class NodeAddress;

class Node {
public:
    Node(uint64_t node_id, std::shared_ptr<NodeAddress> addrs);
    ~Node();

    void Start();
    std::shared_ptr<Range> GetRange(uint64_t i);

private:
    const uint64_t node_id_;
    std::shared_ptr<NodeAddress> addr_mgr_;

    std::unique_ptr<RaftServer> raft_server_;
    std::unordered_map<uint64_t, std::shared_ptr<Range>> ranges_;
};

} /* namespace bench */
} /* namespace raft */
} /* namespace jim */
