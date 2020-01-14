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

#include <unistd.h>
#include <cassert>
#include <iostream>
#include <thread>
#include <functional>

#include "number_statemachine.h"
#include "raft/raft.h"
#include "raft/server.h"
#include "common/logger.h"

using namespace jim;
using namespace jim::raft;

static const size_t kNodeNum = 5;

std::condition_variable g_cv;
std::mutex g_mu;
size_t g_finish_count = 0;
std::vector<Peer> g_peers;

void run_node(uint64_t node_id) {
    RaftServerOptions ops;
    ops.node_id = node_id;
    ops.tick_interval = std::chrono::milliseconds(100);
    ops.election_tick = 5;
    ops.transport_options.use_inprocess_transport = true;

    auto rs = CreateRaftServer(ops);
    assert(rs);
    auto s = rs->Start();
    assert(s.ok());

    auto sm = std::make_shared<raft::test::NumberStateMachine>(node_id);

    RaftOptions rops;
    rops.id = 1;
    rops.statemachine = sm;
    rops.use_memory_storage = true;
    rops.peers = g_peers;

    std::shared_ptr<Raft> r;
    s = rs->CreateRaft(rops, &r);
    FLOG_DEBUG("node_id: {} create raft[{}] {}", node_id, rops.id, s.ToString());
    //std::cout << s.ToString() << std::endl;
    assert(s.ok());

    while (true) {
        uint64_t leader;
        uint64_t term;
        r->GetLeaderTerm(&leader, &term);
        if (leader != 0) {
            break;
        } else {
            usleep(1000 * 100);
        }
    }

    int reqs_count = 10000;
    if (r->IsLeader()) {
        for (int i = 1; i <= reqs_count; ++i) {
            std::string cmd = std::to_string(i);
            s = r->Propose(cmd, 0);
            assert(s.ok());
        }
    }

    s = sm->WaitNumber(reqs_count);
    assert(s.ok());
    FLOG_INFO("node_id: {} wait number return {}", node_id, s.ToString());
    //std::cout << "[NODE" << node_id << "]"
    //          << " wait number return: " << s.ToString() << std::endl;

    r->Truncate(3);

    // current node complete
    {
        std::lock_guard<std::mutex> lock(g_mu);
        ++g_finish_count;
    }
    g_cv.notify_all();

    // wait other nodes
    std::unique_lock<std::mutex> lock(g_mu);
    while (g_finish_count < kNodeNum) {
        g_cv.wait(lock);
    }
};

// five nodes cluster test
//
// 1) start two normal nodes and one learner
// 2) wait replication and then compact log
// 3) start another normal node and another learner, check snapshot flow

int main(int argc, char* argv[]) {
    LoggerConfig config;
    if (kNodeNum < 5) {
        throw std::runtime_error("node number should greater than five.");
    }

    // init cluster members
    for (uint64_t i = 1; i <= kNodeNum; ++i) {
        Peer p;
        if (i == 3 || i == kNodeNum - 1) {
            p.type = PeerType::kLearner;
        } else {
            p.type = PeerType::kNormal;
        }
        p.node_id = i;
        p.peer_id = i;
        g_peers.push_back(p);
    }

    std::vector<std::thread> threads;

    // start n-2 nodes
    for (uint64_t i = 1; i <= kNodeNum - 2; ++i) {
        threads.push_back(std::thread(std::bind(&run_node, i)));
    }

    // wait replication and log compaction
    {
        std::unique_lock<std::mutex> lock(g_mu);
        while (g_finish_count < kNodeNum - 2) {
            g_cv.wait(lock);
        }
    }
    // start two remain node, check snapshot flow
    threads.push_back(std::thread(std::bind(&run_node, kNodeNum - 1)));
    threads.push_back(std::thread(std::bind(&run_node, kNodeNum)));

    for (auto& t : threads) {
        t.join();
    }
}
