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

#include "range.h"

#include <unistd.h>
#include <iostream>

#include "base/fs_util.h"
#include "address.h"
#include "config.h"

namespace jim {
namespace raft {
namespace bench {

uint64_t Range::RequestQueue::add(std::shared_future<bool>* f) {
    std::unique_lock<std::mutex> lock(mu_);
    *f = que_[++seq_].get_future();
    return seq_;
}

void Range::RequestQueue::set(uint64_t seq, bool value) {
    std::unique_lock<std::mutex> lock(mu_);
    auto it = que_.find(seq);
    if (it != que_.end()) {
        it->second.set_value(value);
        que_.erase(it);
    }
}

void Range::RequestQueue::remove(uint64_t seq) {
    std::unique_lock<std::mutex> lock(mu_);
    que_.erase(seq);
}

Range::Range(uint64_t id, uint64_t node_id, RaftServer* rs,
             const std::shared_ptr<NodeAddress>& addr_mgr)
    : id_(id), node_id_(node_id), raft_server_(rs), addr_mgr_(addr_mgr) {}

Range::~Range() {}

void Range::Start() {
    RaftOptions rops;
    rops.id = id_;
    rops.statemachine = shared_from_this();
    rops.use_memory_storage = bench_config.raft_config.use_memory_log;
    rops.storage_path = JoinFilePath({bench_config.raft_config.log_path,
                                      std::to_string(node_id_),
                                      std::to_string(id_)});
    std::vector<uint64_t> nodes;
    addr_mgr_->GetAllNodes(&nodes);
    for (auto n : nodes) {
        Peer p;
        p.type = PeerType::kNormal;
        p.node_id = n;
        rops.peers.push_back(p);
    }

    auto s = raft_server_->CreateRaft(rops, &raft_);
    if (!s.ok()) {
        std::cerr << "creat raft on " << node_id_ << " failed: " << s.ToString()
                  << std::endl;
        ::exit(EXIT_FAILURE);
    }
}

void Range::WaitLeader() {
    while (leader_ == 0) {
        ::usleep(1000 * 10);
    }
}

void Range::SyncRequest() {
    auto f = AsyncRequest();
    f.wait();
}

std::shared_future<bool> Range::AsyncRequest() {
    std::shared_future<bool> f;
    uint64_t seq = request_queue_.add(&f);
    std::string cmd = std::to_string(seq);
    auto rs = raft_->Propose(cmd, 0);
    if (!rs.ok()) {
        throw std::runtime_error(std::string("submit failed:") + rs.ToString());
    } else {
        return f;
    }
}

Status Range::Apply(const std::string& cmd, uint64_t index) {
    uint64_t seq = strtoull(cmd.c_str(), NULL, 10);
    request_queue_.set(seq, true);
    return Status::OK();
}

void Range::Destroy() {
    raft_server_->DestroyRaft(id_, false);
}

} /* namespace bench */
} /* namespace raft */
} /* namespace jim */