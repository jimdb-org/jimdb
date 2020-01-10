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

#include "raft/src/impl/raft_log.h"
#include "raft/src/impl/storage/storage_disk.h"
#include "test_util.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {

using namespace jimkv::raft::impl;
using namespace jimkv::raft::impl::storage;
using namespace jimkv::raft::impl::testutil;

class RaftLogTest : public ::testing::Test {
protected:
    void SetUp() override {
        char path[] = "/tmp/jimkv_raft_storage_test_XXXXXX";
        char* tmp = mkdtemp(path);
        ASSERT_TRUE(tmp != NULL);
        tmp_dir_ = tmp;

        Open();
    }

    void TearDown() override {
        if (storage_) {
            auto s = storage_->Destroy(false);
            ASSERT_TRUE(s.ok()) << s.ToString();
            storage_.reset();
        }
        if (raft_log_) {
            delete raft_log_;
        }
    }

private:
    void Open() {
        DiskStorage::Options ops;
        ops.log_file_size = 1024;
        ops.allow_corrupt_startup = true;
        storage_ = std::shared_ptr<Storage>(new DiskStorage(1, tmp_dir_, ops));
        auto s = storage_->Open();
        ASSERT_TRUE(s.ok()) << s.ToString();
        raft_log_ = new RaftLog(1, storage_);
    }

protected:
    std::string tmp_dir_;
    std::shared_ptr<Storage> storage_;
    RaftLog* raft_log_ = nullptr;
};

TEST_F(RaftLogTest, Truncate) {
    uint64_t lo = 1, hi = 70;
    std::vector<EntryPtr> wents1;
    RandomEntries(lo, hi, 256, &wents1);
    raft_log_->append(wents1);

    std::vector<EntryPtr> pents;
    raft_log_->unstableEntries(&pents);
    auto s = storage_->StoreEntries(pents);
    ASSERT_TRUE(s.ok());

    lo = 70;
    hi = 100;
    std::vector<EntryPtr> wents2;
    RandomEntries(lo, hi, 256, &wents2);
    raft_log_->append(wents2);

    lo = 50;
    hi = 61;
    std::vector<EntryPtr> wents3;
    RandomEntries(lo, hi, 256, &wents3);
    uint64_t new_index = 0;
    ASSERT_TRUE(raft_log_->maybeAppend(49, wents1[48]->term(), 50, wents3, &new_index));
    ASSERT_EQ(new_index, 60U);

    pents.clear();
    raft_log_->unstableEntries(&pents);
    s = storage_->StoreEntries(pents);
    ASSERT_TRUE(s.ok());

    ASSERT_EQ(raft_log_->lastIndex(), 60U);
    uint64_t index = 0;
    s = storage_->LastIndex(&index);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(index, 60U);
}
};
