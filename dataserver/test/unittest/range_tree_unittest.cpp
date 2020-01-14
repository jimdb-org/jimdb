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

#include <gtest/gtest.h>

#include "base/util.h"
#include "base/status.h"
#include "server/range_tree.h"
#include "db/db_manager.h"
#include "master/client.h"
#include "storage/meta_store.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {

using namespace jim;
using namespace jim::ds;
using namespace jim::ds::server;
using namespace jim::ds::range;

class TestContext : public RangeContext {
public:
    uint64_t GetNodeID() const override { return 100; }
    db::DBManager* DBManager() override  { return nullptr; }
    master::MasterClient* MasterClient() override { return nullptr; }
    raft::RaftServer* RaftServer() override { return nullptr; }
    storage::MetaStore* MetaStore() override { return nullptr; }
    RangeStats* Statistics() override { return nullptr; }
    uint64_t GetDBUsagePercent() const override { return 0; }
    void ScheduleCheckSize(uint64_t range_id) override {}
    TimerQueue* GetTimerQueue() override { return nullptr; }
    std::shared_ptr<Range> FindRange(uint64_t range_id) override { return nullptr; }
    Status SplitRange(uint64_t, const dspb::SplitCommand&, uint64_t) override {
        return Status(Status::kNotSupported);
    }
};

static basepb::Range randomMeta(uint64_t id) {
    basepb::Range meta;
    meta.set_id(id);
    meta.set_start_key(randomString(100));
    meta.set_end_key(randomString(100));
    meta.set_table_id(randomInt());
    return meta;
}

TEST(RangeTree, InsertFindRemove) {
    RangeTree tree;

    // find not exist
    auto rng = tree.Find(1);
    ASSERT_TRUE(rng == nullptr);

    const auto meta = randomMeta(1);
    std::unique_ptr<RangeContext> range_ctx(new TestContext);

    RangePtr exist_one;
    RangeOptions opt;
    auto s = tree.Insert(1, [&](RangePtr& ptr) {
        ptr = std::make_shared<Range>(opt, range_ctx.get(), meta);
        return Status::OK();
    }, exist_one);
    ASSERT_TRUE(s.ok()) << s.ToString();

    // test get all
    auto all_ranges = tree.GetAll();
    ASSERT_EQ(all_ranges.size(), 1U);
    ASSERT_EQ(all_ranges[0]->GetMeta().ShortDebugString(), meta.ShortDebugString());

    std::cout << all_ranges[0]->GetMeta().id() << std::endl;

    rng = tree.Find(1);
    ASSERT_TRUE(rng != nullptr);
    ASSERT_EQ(rng->GetMeta().ShortDebugString(), meta.ShortDebugString());

    // insert duplicate
    s = tree.Insert(1, [&](RangePtr& ptr) {
        ptr = std::make_shared<Range>(opt, range_ctx.get(), meta);
        return Status::OK();
    }, exist_one);
    ASSERT_EQ(s.code(), Status::kExisted);
    ASSERT_TRUE(exist_one != nullptr);
    ASSERT_EQ(exist_one->GetMeta().ShortDebugString(), meta.ShortDebugString());

    // remove miss
    RangePtr removed;
    s = tree.RemoveIf(2, [](const RangePtr&) { return Status::OK(); }, removed);
    ASSERT_EQ(s.code(), Status::kNotFound);
    ASSERT_TRUE(removed == nullptr);
    rng = tree.Find(1);
    ASSERT_TRUE(rng != nullptr);
    ASSERT_EQ(rng->GetMeta().ShortDebugString(), meta.ShortDebugString());

    // remove miss 2
    s = tree.RemoveIf(1, [](const RangePtr&) { return Status(Status::kAborted); }, removed);
    ASSERT_EQ(s.code(), Status::kAborted);
    ASSERT_TRUE(removed == nullptr);
    rng = tree.Find(1);
    ASSERT_TRUE(rng != nullptr);
    ASSERT_EQ(rng->GetMeta().ShortDebugString(), meta.ShortDebugString());

    // remove successfully
    s = tree.RemoveIf(1, [](const RangePtr&) { return Status::OK(); }, removed);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_TRUE(removed != nullptr);
    ASSERT_EQ(removed->GetMeta().ShortDebugString(), meta.ShortDebugString());
}


}

