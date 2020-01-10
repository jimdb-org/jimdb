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

#include "address.h"

namespace jimkv {
namespace raft {
namespace bench {

NodeAddress::NodeAddress(int n) {
    if (n < 1 && n > 9) {
        throw std::runtime_error("invalid node num, must be 1 to 9");
    }
    for (uint16_t i = 1; i <= n; ++i) {
        ports_.emplace(i, 9990 + i);
    }
}

NodeAddress::~NodeAddress() {}

std::string NodeAddress::GetNodeAddress(uint64_t node_id) {
    return std::string("127.0.0.1:") + std::to_string(GetListenPort(node_id));
}

uint16_t NodeAddress::GetListenPort(uint64_t node_id) const {
    auto it = ports_.find(node_id);
    if (it == ports_.cend()) {
        throw std::runtime_error("invalid node id, must be 1 to 9");
    } else {
        return it->second;
    }
}

void NodeAddress::GetAllNodes(std::vector<uint64_t>* nodes) const {
    for (auto it = ports_.cbegin(); it != ports_.cend(); ++it) {
        nodes->push_back(it->first);
    }
}

} /* namespace bench */
} /* namespace raft */
} /* namespace jimkv */