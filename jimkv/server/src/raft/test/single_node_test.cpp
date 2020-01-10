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

#include <unistd.h>
#include <cassert>

#include "common/logger.h"
#include "number_statemachine.h"
#include "raft/raft.h"
#include "raft/server.h"

using namespace jimkv;
using namespace jimkv::raft;

int main(int argc, char* argv[]) {
    // TODO:
    RaftServerOptions ops;
    LoggerConfig config;
    ops.node_id = 1;
    ops.election_tick = 2;
    ops.tick_interval = std::chrono::milliseconds(100);
    ops.transport_options.use_inprocess_transport = true;

    auto rs = CreateRaftServer(ops);
    assert(rs);
    auto s = rs->Start();
    assert(s.ok());

    auto sm = std::make_shared<raft::test::NumberStateMachine>(1);

    RaftOptions rops;
    rops.id = 9;
    rops.statemachine = sm;
    rops.use_memory_storage = true;
    Peer p;
    p.type = PeerType::kNormal;
    p.node_id = 1;
    rops.peers.push_back(p);

    std::shared_ptr<Raft> r;
    s = rs->CreateRaft(rops, &r);
    assert(s.ok());

    while (!r->IsLeader()) {
        usleep(1000 * 100);
    }

    FLOG_DEBUG("leader elected.");

    for (int i = 0; i <= 100; ++i) {
        std::string cmd = std::to_string(i);
        // s = r->Submit(cmd, 0, );
        s = r->Propose(cmd, 0);
        assert(s.ok());
    }

    s = sm->WaitNumber(100);
    assert(s.ok());
    FLOG_DEBUG("wait successfully.");
}
