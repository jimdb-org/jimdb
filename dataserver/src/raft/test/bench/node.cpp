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

#include "node.h"

#include <unistd.h>
#include <iostream>

#include "address.h"
#include "config.h"

namespace jim {
namespace raft {
namespace bench {

Node::Node(uint64_t node_id, std::shared_ptr<NodeAddress> addrs)
    : node_id_(node_id), addr_mgr_(addrs) {
    RaftServerOptions ops;
    ops.node_id = node_id_;
    ops.consensus_threads_num = bench_config.raft_config.work_threads;
    ops.election_tick = 2;
    ops.transport_options.listen_port = addr_mgr_->GetListenPort(node_id_);
    ops.transport_options.use_inprocess_transport = bench_config.raft_config.use_inprocess_transport;
    ops.transport_options.resolver =
        std::static_pointer_cast<NodeResolver>(addr_mgr_);
    raft_server_ = CreateRaftServer(ops);
    auto s = raft_server_->Start();
    if (!s.ok()) {
        throw std::runtime_error(std::string("create raft server failed: ") +
                                 s.ToString());
    }
}

Node::~Node() {
    for (auto& r: ranges_) {
        r.second->Destroy();
    }
}

void Node::Start() {
    for (uint64_t i = 1; i <= bench_config.range_num; ++i) {
        auto rng = std::make_shared<Range>(i, node_id_, raft_server_.get(), addr_mgr_);
        rng->Start();
        ranges_.emplace(i, rng);
    }
}

std::shared_ptr<Range> Node::GetRange(uint64_t i) {
    auto it = ranges_.find(i);
    if (it != ranges_.end()) {
        return it->second;
    } else {
        throw std::runtime_error(std::string("could not find range ") +
                                 std::to_string(i));
    }
}

} /* namespace bench */
} /* namespace raft */
} /* namespace jim */
