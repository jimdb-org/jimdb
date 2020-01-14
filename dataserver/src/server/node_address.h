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

#include <mutex>
#include <string>
#include <unordered_map>

#include "base/shared_mutex.h"
#include "master/client.h"
#include "raft/options.h"

namespace jim {
namespace ds {
namespace server {

class NodeAddress : public raft::NodeResolver {
public:
    explicit NodeAddress(master::MasterClient *master_client);
    virtual ~NodeAddress() = default;

    NodeAddress(const NodeAddress&) = delete;
    NodeAddress& operator=(const NodeAddress&) = delete;
    NodeAddress& operator=(const NodeAddress&) volatile = delete;

    std::string GetNodeAddress(uint64_t node_id) override;

    bool AddNodeAddress(uint64_t node_id, std::string address);
private:
    shared_mutex mutex_;
    std::unordered_map<uint64_t, std::string> address_map_;
    master::MasterClient* master_server_;
};

} // namespace server
} // namespace ds
} // namespace jim
