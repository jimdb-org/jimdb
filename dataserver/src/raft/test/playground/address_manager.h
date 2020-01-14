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

#include <iostream>
#include <map>
#include "raft/options.h"

namespace jim {
namespace raft {
namespace playground {

static const uint64_t kMaxNodeID = 9;

class AddressManager : public NodeResolver {
public:
    AddressManager() {
        for (uint64_t i = 1; i <= kMaxNodeID; ++i) {
            raft_ports_[i] = 9980 + i;
            tel_ports_[i] = 9990 + i;
        }
    }

    std::string GetNodeAddress(uint64_t node_id) override {
        auto it = raft_ports_.find(node_id);
        if (it == raft_ports_.end()) {
            return "";
        } else {
            return std::string("127.0.0.1:") + std::to_string(it->second);
        }
    }

    uint16_t GetRaftPort(uint16_t node_id) {
        auto it = raft_ports_.find(node_id);
        if (it == raft_ports_.end()) {
            return 0;
        } else {
            return it->second;
        }
    }

    uint16_t GetTelnetPort(uint64_t node_id) {
        auto it = tel_ports_.find(node_id);
        if (it == tel_ports_.end()) {
            return 0;
        } else {
            return it->second;
        }
    }

private:
    std::map<uint64_t, uint16_t> raft_ports_;
    std::map<uint64_t, uint16_t> tel_ports_;
};

} /* namespace playground */
} /* namespace raft */
} /* namespace jim */
