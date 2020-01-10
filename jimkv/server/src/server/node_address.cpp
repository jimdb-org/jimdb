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

#include "node_address.h"

#include "common/logger.h"
#include "common/server_config.h"

namespace jimkv {
namespace ds {
namespace server {

NodeAddress::NodeAddress(master::MasterClient *master_client)
    : master_server_(master_client) {
}

std::string NodeAddress::GetNodeAddress(uint64_t node_id) {
    {
        jimkv::shared_lock<jimkv::shared_mutex> lock(mutex_);
        auto it = address_map_.find(node_id);
        if (it != address_map_.end()) {
            return it->second;
        }
    }

    std::string addr;
    if (ds_config.b_test) {
        FLOG_ERROR("get raft address failed by node_id: {}", node_id);
        return addr;
    }
    auto s = master_server_->GetRaftAddress(node_id, &addr);
    if (!s.ok()) {
        FLOG_ERROR("get raft address to {} failed: {}",  node_id, s.ToString().c_str());
    } else if (!addr.empty()) {
        std::unique_lock<jimkv::shared_mutex> lock(mutex_);
        address_map_[node_id] = addr;
    }

    return addr;
}

bool NodeAddress::AddNodeAddress(uint64_t node_id, std::string address) {
    jimkv::shared_lock<jimkv::shared_mutex> lock(mutex_);
    auto it = address_map_.find(node_id);
    if (it != address_map_.end()) {
        FLOG_WARN("node_id: {} old address: {} is exsist, don't set new address: {}", node_id, it->second, address);
        return false;
    }
    address_map_[node_id] = address;
    return true;
}

}  // namespace server
}  // namespace ds
}  // namespace jimkv
