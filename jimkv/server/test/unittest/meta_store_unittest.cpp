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

#include <map>
#include <gtest/gtest.h>

#include "base/status.h"
#include "base/fs_util.h"
#include "common/ds_encoding.h"
#include "storage/meta_store.h"

int main(int argc, char *argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {

using namespace jimkv;
using namespace jimkv::ds;
using namespace jimkv::ds::storage;

class MetaStoreTest : public ::testing::Test {
protected:
    MetaStoreTest() : store_(nullptr) {}

    void SetUp() override {
        char path[] = "/tmp/jimkv_ds_meta_store_test_XXXXXX";
        char *tmp = mkdtemp(path);
        ASSERT_TRUE(tmp != NULL);
        tmp_dir_ = tmp;

        store_ = new MetaStore(tmp_dir_);
        auto s = store_->Open();
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    void TearDown() override {
        delete store_;
        if (!tmp_dir_.empty()) {
            jimkv::RemoveDirAll(tmp_dir_.c_str());
        }
    }

protected:
    std::string tmp_dir_;
    MetaStore *store_;
};

TEST_F(MetaStoreTest, NodeID) {
    uint64_t node = jimkv::randomInt();
    auto s = store_->SaveNodeID(node);
    ASSERT_TRUE(s.ok()) << s.ToString();

    uint64_t node2 = 0;
    s = store_->GetNodeID(&node2);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(node2, node);
}

TEST_F(MetaStoreTest, ApplyIndex) {
    uint64_t range_id = jimkv::randomInt();
    uint64_t applied = 1;
    auto s = store_->LoadApplyIndex(range_id, &applied);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(applied, 0U);

    uint64_t save_applied = jimkv::randomInt();
    s = store_->SaveApplyIndex(range_id, save_applied);
    ASSERT_TRUE(s.ok()) << s.ToString();

    s = store_->LoadApplyIndex(range_id, &applied);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(applied, save_applied);

    s = store_->DeleteApplyIndex(range_id);
    ASSERT_TRUE(s.ok()) << s.ToString();

    s = store_->LoadApplyIndex(range_id, &applied);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(applied, 0U);
}

static basepb::Range genRange(uint64_t i) {
    basepb::Range rng;
    rng.set_id(i);
    rng.set_start_key(randomString(randomInt() % 50 + 10));
    rng.set_end_key(randomString(randomInt() % 50 + 10));
    rng.set_table_id(randomInt());
    rng.mutable_range_epoch()->set_conf_ver(randomInt());
    rng.mutable_range_epoch()->set_version(randomInt());
    return rng;
}

TEST_F(MetaStoreTest, Range) {
    auto rng = genRange(randomInt());
    auto s = store_->AddRange(rng);
    ASSERT_TRUE(s.ok()) << s.ToString();

    std::vector<basepb::Range> get_results;
    s = store_->GetAllRange(&get_results);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(get_results.size(), 1U);
    ASSERT_EQ(get_results[0].id(), rng.id());

    s = store_->DelRange(rng.id());
    ASSERT_TRUE(s.ok()) << s.ToString();

    get_results.clear();
    s = store_->GetAllRange(&get_results);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_TRUE(get_results.empty());

    std::vector<basepb::Range> ranges;
    for (uint64_t i = 0; i < 100; ++i) {
        auto r = genRange(i);
        ranges.push_back(r);
    }
    s = store_->BatchAddRange(ranges);
    ASSERT_TRUE(s.ok()) << s.ToString();

    get_results.clear();
    s = store_->GetAllRange(&get_results);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(get_results.size(), ranges.size());
}

}
