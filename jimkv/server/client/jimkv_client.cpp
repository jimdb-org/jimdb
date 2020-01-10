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


#include "jimkv_client.h"

#include "jimkv_log.h"

namespace jimkv {
namespace sdk {

void JimClient::Start(int thread_num, std::string name) {
    assist_ = std::make_shared<JimAssist>();
    assist_->Start();

    context_pool_.reset(new net::IOContextPool(thread_num, name));
    context_pool_->Start();

    running_ = true;
}

void JimClient::Stop() {
    running_ = false;
    context_pool_->Stop();
    assist_->Stop();
}

ClusterPtr JimClient::AddCluster(uint64_t cluster_id, std::string &host, uint32_t conn_count) {
    std::lock_guard<shared_mutex> lock(clusters_lock_);
    auto it = clusters_.find(cluster_id);
    if (it == clusters_.end()) {
        auto cluster = std::make_shared<JimCluster>(cluster_id, host, conn_count);
        clusters_[cluster_id] = cluster;
        return cluster;
    }

    return it->second;
}

void JimClient::RemoveCluster(uint64_t cluster_id) {
    std::lock_guard<shared_mutex> lock(clusters_lock_);
    clusters_.erase(cluster_id);
}

ClusterPtr JimClient::GetCluster(uint64_t cluster_id) {
    shared_lock<shared_mutex> lock(clusters_lock_);
    auto it = clusters_.find(cluster_id);
    if (it != clusters_.end()) {
        return it->second;
    }

    return nullptr;
}

bool JimClient::SendCmd(CmdPtr cmd, int timeout) {
    auto cluster = GetCluster(cmd->cluster_id());
    if (cluster != nullptr) {
        return cluster->SendCmd(cmd, timeout);
    }

    CBLOG_WARN("cluster %" PRIu64 " not found.", cmd->cluster_id());
    return false;
}

} // namespace sdk
} // namespace jimkv

