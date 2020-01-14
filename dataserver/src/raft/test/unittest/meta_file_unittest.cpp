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
#include "raft/src/impl/storage/meta_file.h"
#include "test_util.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {

using namespace jim::raft::impl;
using namespace jim::raft::impl::storage;
using namespace jim::raft::impl::testutil;
using jim::Status;
using jim::randomInt;

class MetaTest : public ::testing::Test {
protected:
    void SetUp() override {
        char path[] = "/tmp/jim_raft_meta_test_XXXXXX";
        char* tmp = mkdtemp(path);
        ASSERT_TRUE(tmp != NULL);
        tmp_dir_ = tmp;

        meta_file_ = new MetaFile(tmp_dir_);
        auto s = meta_file_->Open();
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    void TearDown() override {
        if (meta_file_ != nullptr) {
            meta_file_->Destroy();
            delete meta_file_;
        }
        if (!tmp_dir_.empty()) {
            std::remove(tmp_dir_.c_str());
        }
    }

protected:
    std::string tmp_dir_;
    MetaFile* meta_file_{nullptr};
};

TEST_F(MetaTest, Empty) {
    pb::HardState hs;
    pb::TruncateMeta tm;
    uint64_t index = 0;
    auto s = meta_file_->Load(&hs, &tm, &index);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(hs.term(), 0U);
    ASSERT_EQ(hs.vote(), 0U);
    ASSERT_EQ(hs.commit(), 0U);
    ASSERT_EQ(tm.index(), 0U);
    ASSERT_EQ(tm.term(), 0U);
    ASSERT_EQ(index, 0U);
}

TEST_F(MetaTest, SaveLoad) {
    pb::HardState hs;
    hs.set_term(randomInt());
    hs.set_commit(randomInt());
    hs.set_vote(randomInt());

    pb::TruncateMeta tm;
    tm.set_index(randomInt());
    tm.set_term(randomInt());

    uint64_t inherit_index = 0;

    // save
    auto s = meta_file_->SaveHardState(hs);
    ASSERT_TRUE(s.ok()) << s.ToString();
    s = meta_file_->SaveTruncMeta(tm);
    ASSERT_TRUE(s.ok()) << s.ToString();
    s = meta_file_->SaveInheritIndex(inherit_index);
    ASSERT_TRUE(s.ok()) << s.ToString();

    // load
    pb::HardState load_hs;
    pb::TruncateMeta load_tm;
    uint64_t load_inherit = 0;
    s = meta_file_->Load(&load_hs, &load_tm, &load_inherit);
    ASSERT_TRUE(s.ok()) << s.ToString();
    s = Equal(load_hs, hs);
    ASSERT_TRUE(s.ok()) << s.ToString();
    s = Equal(load_tm, tm);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(load_inherit, inherit_index);
}

TEST_F(MetaTest, SaveLoad2) {
    pb::HardState hs;

    pb::TruncateMeta tm;
    tm.set_index(randomInt());
    tm.set_term(randomInt());

    // save truncmate only
    auto s = meta_file_->SaveTruncMeta(tm);
    ASSERT_TRUE(s.ok()) << s.ToString();

    // load
    pb::HardState load_hs;
    pb::TruncateMeta load_tm;
    uint64_t inherit_index = 0;
    s = meta_file_->Load(&load_hs, &load_tm, &inherit_index);
    ASSERT_TRUE(s.ok()) << s.ToString();
    s = Equal(load_hs, hs);
    ASSERT_TRUE(s.ok()) << s.ToString();
    s = Equal(load_tm, tm);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(inherit_index, 0U);
}

}  // namespace
