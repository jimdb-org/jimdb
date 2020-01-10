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

#include <gtest/gtest.h>

#include "base/util.h"
#include "raft/src/impl/replica.h"
#include "test_util.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {

using jimkv::raft::Peer;
using jimkv::randomInt;
using namespace jimkv::raft::impl;

TEST(Replica, SetGet) {
    auto p = testutil::RandomPeer();
    int max_inflight = randomInt() % 100 + 100;
    Replica r(p, max_inflight);

    // peer
    ASSERT_EQ(r.peer().node_id, p.node_id);
    ASSERT_EQ(r.peer().type, p.type);

    // next
    ASSERT_EQ(r.next(), 0UL);
    uint64_t next = randomInt();
    r.set_next(next);
    ASSERT_EQ(r.next(), next);

    // match
    ASSERT_EQ(r.match(), 0UL);
    uint64_t match = randomInt();
    r.set_match(match);
    ASSERT_EQ(r.match(), match);

    // commited
    ASSERT_EQ(r.committed(), 0UL);
    uint64_t commit = randomInt();
    r.set_committed(commit);
    ASSERT_EQ(r.committed(), commit);

    // active
    ASSERT_EQ(r.inactive_ticks(), 0UL);
    r.incr_inactive_tick();
    ASSERT_EQ(r.inactive_ticks(), 1UL);
    r.set_active();
    ASSERT_EQ(r.inactive_ticks(), 0UL);

    // state
    ASSERT_EQ(r.state(), ReplicaState::kProbe);
    r.becomeSnapshot(123);
    ASSERT_EQ(r.state(), ReplicaState::kSnapshot);
    r.becomeProbe();
    ASSERT_EQ(r.state(), ReplicaState::kProbe);
    r.becomeReplicate();
    ASSERT_EQ(r.state(), ReplicaState::kReplicate);
    r.resetState(ReplicaState::kProbe);
    ASSERT_EQ(r.state(), ReplicaState::kProbe);

    // pause
    ASSERT_FALSE(r.isPaused());
    r.pause();
    ASSERT_TRUE(r.isPaused());
    r.resume();
    ASSERT_FALSE(r.isPaused());
}

TEST(Replica, Update) {
    Replica r(testutil::RandomPeer(), 100);
    r.update(100);
    ASSERT_EQ(r.next(), 101U);

    r.pause();
    ASSERT_TRUE(r.maybeUpdate(200, 100));
    ASSERT_EQ(r.match(), 200U);
    ASSERT_EQ(r.next(), 201U);
    ASSERT_FALSE(r.isPaused());
    ASSERT_EQ(r.committed(), 100U);

    ASSERT_FALSE(r.maybeUpdate(200, 101));
    ASSERT_EQ(r.committed(), 101U);

    r.becomeReplicate();
    ASSERT_TRUE(r.maybeDecrTo(201, 201, 102));
    ASSERT_EQ(r.next(), 201U);
    ASSERT_EQ(r.committed(), 102U);
}

TEST(Replica, Inflight) {
    Replica replica(testutil::RandomPeer(), 100);
    auto& inflight = replica.inflight();
    for (int i = 100; i < 200; ++i) {
        inflight.add(i);
        if (i < 199) {
            ASSERT_FALSE(inflight.full());
        }
    }
    ASSERT_TRUE(inflight.full());
    inflight.freeTo(99);
    ASSERT_TRUE(inflight.full());
    inflight.freeFirstOne();  // free 100
    ASSERT_FALSE(inflight.full());
    inflight.add(200);
    ASSERT_TRUE(inflight.full());
    inflight.freeTo(100);  // 100 is already free
    ASSERT_TRUE(inflight.full());
    inflight.freeTo(101);  // free 101
    ASSERT_FALSE(inflight.full());
    inflight.add(201);

    // test reset
    inflight.reset();
    for (int i = 300; i < 400; ++i) {
        inflight.add(i);
        if (i < 399) {
            ASSERT_FALSE(inflight.full());
        }
    }
    ASSERT_TRUE(inflight.full());
}

}  // namespace
