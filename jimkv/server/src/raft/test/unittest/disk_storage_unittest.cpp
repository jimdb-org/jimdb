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

#include "base/fs_util.h"
#include "raft/src/impl/storage/storage_disk.h"
#include "raft/entry_flags.h"

#include "test_util.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {

using namespace jimkv::raft::impl;
using namespace jimkv::raft::impl::storage;
using namespace jimkv::raft::impl::testutil;

class StorageTest : public ::testing::Test {
protected:
    void SetUp() override {
        char path[] = "/tmp/jimkv_raft_storage_test_XXXXXX";
        char* tmp = mkdtemp(path);
        ASSERT_TRUE(tmp != NULL);
        tmp_dir_ = tmp;

        ops_.log_file_size = 1024;
        ops_.allow_corrupt_startup = true;

        Open();
    }

    void ReOpen() {
        auto s = storage_->Close();
        ASSERT_TRUE(s.ok()) << s.ToString();
        delete storage_;

        Open();
    }

    void TearDown() override {
        if (storage_ != nullptr) {
            auto s = storage_->Destroy(false);
            ASSERT_TRUE(s.ok()) << s.ToString();
            delete storage_;
        }
    }

    void LimitMaxLogs(size_t size) {
        ops_.max_log_files = size;
        ReOpen();
    }

private:
    void Open() {
        storage_ = new DiskStorage(1, tmp_dir_, ops_);
        auto s = storage_->Open();
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

protected:
    std::string tmp_dir_;
    DiskStorage::Options ops_; // open options
    DiskStorage* storage_;
};


TEST_F(StorageTest, LogEntry) {
    uint64_t lo = 1, hi = 100;
    std::vector<EntryPtr> to_writes;
    RandomEntries(lo, hi, 256, &to_writes);
    auto s = storage_->StoreEntries(to_writes);
    ASSERT_TRUE(s.ok()) << s.ToString();

    uint64_t index = 0;
    s = storage_->FirstIndex(&index);
    ASSERT_EQ(index, 1UL);
    s = storage_->LastIndex(&index);
    ASSERT_EQ(index, 99UL);

    // read one by one
    for (uint64_t index = lo; index < hi; ++index) {
        std::vector<EntryPtr> ents;
        bool compacted = false;
        s = storage_->Entries(index, index + 1, std::numeric_limits<uint64_t>::max(),
                &ents, &compacted);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(compacted);
        ASSERT_EQ(ents.size(), 1U);
        s = Equal(ents[0], to_writes[index-lo]);
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    // read all entries
    std::vector<EntryPtr> ents;
    bool compacted = false;
    s = storage_->Entries(lo, hi, std::numeric_limits<uint64_t>::max(), &ents,
                          &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);
    s = Equal(ents, to_writes);
    ASSERT_TRUE(s.ok()) << s.ToString();

    // test Term()
    for (uint64_t i = lo; i < hi; ++i) {
        uint64_t term = 0;
        bool compacted = false;
        s = storage_->Term(i, &term, &compacted);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(compacted);
        ASSERT_EQ(term, to_writes[i - 1]->term());
    }

    // with max_size arg
    ents.clear();
    s = storage_->Entries(lo, hi,
                          to_writes[0]->ByteSizeLong() + to_writes[1]->ByteSizeLong(),
                          &ents, &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);
    std::vector<EntryPtr> ents2(to_writes.begin(), to_writes.begin() + 2);
    s = Equal(ents, ents2);
    ASSERT_TRUE(s.ok()) << s.ToString();

    // at least one entry
    ents.clear();
    s = storage_->Entries(lo, hi, 1, &ents, &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);
    s = Equal(ents, std::vector<EntryPtr>{to_writes[0]});
    ASSERT_TRUE(s.ok()) << s.ToString();

    // read compacted entries
    ents.clear();
    s = storage_->Entries(0, hi, std::numeric_limits<uint64_t>::max(), &ents, &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_TRUE(compacted);
    ASSERT_TRUE(ents.empty());

    // reopen
    ReOpen();

    // test FristIndex
    s = storage_->FirstIndex(&index);
    ASSERT_EQ(index, 1UL);
    s = storage_->LastIndex(&index);
    ASSERT_EQ(index, 99UL);

    // read all entries
    ents.clear();
    compacted = false;
    s = storage_->Entries(lo, hi, std::numeric_limits<uint64_t>::max(), &ents,
                          &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);
    s = Equal(ents, to_writes);
    ASSERT_TRUE(s.ok()) << s.ToString();

    // test Term()
    for (uint64_t i = lo; i < hi; ++i) {
        uint64_t term = 0;
        bool compacted = false;
        s = storage_->Term(i, &term, &compacted);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(compacted);
        ASSERT_EQ(term, to_writes[i - 1]->term());
    }

    // with max_size arg
    ents.clear();
    s = storage_->Entries(lo, hi,
                          to_writes[0]->ByteSizeLong() + to_writes[1]->ByteSizeLong(),
                          &ents, &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);
    std::vector<EntryPtr> ents3(to_writes.begin(), to_writes.begin() + 2);
    s = Equal(ents, ents3);
    ASSERT_TRUE(s.ok()) << s.ToString();
}

TEST_F(StorageTest, Conflict) {
    uint64_t lo = 1, hi = 100;
    std::vector<EntryPtr> to_writes;
    RandomEntries(lo, hi, 256, &to_writes);
    auto s = storage_->StoreEntries(to_writes);
    ASSERT_TRUE(s.ok()) << s.ToString();

    auto entry = RandomEntry(50, 256);
    s = storage_->StoreEntries(std::vector<EntryPtr>{entry});
    ASSERT_TRUE(s.ok()) << s.ToString();

    uint64_t index = 0;
    s = storage_->FirstIndex(&index);
    ASSERT_EQ(index, 1U);
    s = storage_->LastIndex(&index);
    ASSERT_EQ(index, 50U);

    // read all entries
    std::vector<EntryPtr> ents;
    bool compacted = false;
    s = storage_->Entries(lo, 51, std::numeric_limits<uint64_t>::max(), &ents,
                          &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);
    std::vector<EntryPtr> ents2(to_writes.begin(), to_writes.begin() + 49);
    ents2.push_back(entry);
    s = Equal(ents, ents2);
    ASSERT_TRUE(s.ok()) << s.ToString();
}

TEST_F(StorageTest, Snapshot) {
    uint64_t lo = 1, hi = 100;
    std::vector<EntryPtr> to_writes;
    RandomEntries(lo, hi, 256, &to_writes);
    auto s = storage_->StoreEntries(to_writes);
    ASSERT_TRUE(s.ok()) << s.ToString();

    pb::SnapshotMeta meta;
    meta.set_index(jimkv::randomInt() + 100);
    meta.set_term(jimkv::randomInt());
    s = storage_->ApplySnapshot(meta);
    ASSERT_TRUE(s.ok()) << s.ToString();

    uint64_t index = 0;
    s = storage_->FirstIndex(&index);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(index, meta.index() + 1);
    s = storage_->LastIndex(&index);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(index, meta.index());

    uint64_t term = 0;
    bool compacted = false;
    s = storage_->Term(meta.index() - 20, &term, &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_TRUE(compacted);
    s = storage_->Term(meta.index(), &term, &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(term, meta.term());
    ASSERT_FALSE(compacted);

    auto e = RandomEntry(meta.index() + 1);
    s = storage_->StoreEntries(std::vector<EntryPtr>{e});
    ASSERT_TRUE(s.ok()) << s.ToString();
    std::vector<EntryPtr> ents;
    s = storage_->Entries(meta.index() + 1, meta.index() + 2,
                          std::numeric_limits<uint64_t>::max(), &ents, &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);
    s = Equal(ents, std::vector<EntryPtr>{e});
    ASSERT_TRUE(s.ok()) << s.ToString();
}

TEST_F(StorageTest, KeepCount) {
    LimitMaxLogs(3);
    uint64_t lo = 1, hi = 101;
    std::vector<EntryPtr> to_writes;
    RandomEntries(lo, hi, 256, &to_writes);
    auto s = storage_->StoreEntries(to_writes);
    auto count = storage_->FilesCount();

    storage_->Truncate(100);
    ASSERT_TRUE(s.ok()) << s.ToString();
    auto count2 = storage_->FilesCount();

    std::cout << count << ", " << count2 << std::endl;
    ASSERT_LT(count2, count);
    ASSERT_GE(count2, 3U);

    uint64_t index = 0;
    s = storage_->FirstIndex(&index);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_LE(index, 100UL);
    std::cout << "First: " << index << std::endl;

    std::vector<EntryPtr> ents;
    bool compacted = false;
    s = storage_->Entries(index, 101, std::numeric_limits<uint64_t>::max(), &ents,
                          &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);

    ReOpen();
    std::vector<EntryPtr> ents2;
    s = storage_->Entries(index, 101, std::numeric_limits<uint64_t>::max(), &ents2,
                          &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);

    s = Equal(ents, ents2);
    ASSERT_TRUE(s.ok()) << s.ToString();
}

TEST_F(StorageTest, Destroy) {
    uint64_t lo = 1, hi = 100;
    std::vector<EntryPtr> to_writes;
    RandomEntries(lo, hi, 256, &to_writes);
    auto s = storage_->StoreEntries(to_writes);
    ASSERT_TRUE(s.ok()) << s.ToString();

    s = storage_->Destroy(false);
    ASSERT_TRUE(s.ok()) << s.ToString();
    struct stat sb;
    memset(&sb, 0, sizeof(sb));
    int ret = ::stat(tmp_dir_.c_str(), &sb);
    ASSERT_EQ(ret, -1);
    ASSERT_EQ(errno, ENOENT);
}

TEST_F(StorageTest, DestroyBak) {
    uint64_t lo = 1, hi = 100;
    std::vector<EntryPtr> to_writes;
    RandomEntries(lo, hi, 256, &to_writes);
    auto s = storage_->StoreEntries(to_writes);
    ASSERT_TRUE(s.ok()) << s.ToString();

    auto start = time(NULL);

    s = storage_->Destroy(true);
    ASSERT_TRUE(s.ok()) << s.ToString();

    auto end = time(NULL);

    struct stat sb;
    memset(&sb, 0, sizeof(sb));
    int ret = ::stat(tmp_dir_.c_str(), &sb);
    ASSERT_EQ(ret, -1);
    ASSERT_EQ(errno, ENOENT);

    // find backup path
    std::string bak_path;
    for (auto t = start; t <= end; ++t) {
        std::string path = tmp_dir_ + ".bak." + std::to_string(t);
        ret = ::stat(path.c_str(), &sb);
        if (ret == 0) {
            bak_path = path;
            break;
        }
    }
    ASSERT_TRUE(!bak_path.empty());

    // load entries from backup
    DiskStorage bds(1, bak_path, DiskStorage::Options());
    s = bds.Open();
    ASSERT_TRUE(s.ok()) << s.ToString();
    std::vector<EntryPtr > ents;
    bool compacted = false;
    s = bds.Entries(lo, hi, std::numeric_limits<uint64_t>::max(), &ents, &compacted) ;
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);
    s = Equal(ents, to_writes);
    ASSERT_TRUE(s.ok()) << s.ToString();
    s = bds.Destroy(false);
    ASSERT_TRUE(s.ok()) << s.ToString();
}

#ifndef NDEBUG  // only debug
TEST_F(StorageTest, Corrupt1) {
    uint64_t lo = 1, hi = 100;
    std::vector<EntryPtr> to_writes;
    RandomEntries(lo, hi, 256, &to_writes);
    auto s = storage_->StoreEntries(to_writes);
    ASSERT_TRUE(s.ok()) << s.ToString();

    // append corrupt block on last file
    storage_->TEST_Add_Corruption1();

    // read all
    std::vector<EntryPtr> ents;
    bool compacted = false;
    s = storage_->Entries(lo, hi, std::numeric_limits<uint64_t>::max(), &ents,
                          &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);
    s = Equal(ents, to_writes);
    ASSERT_TRUE(s.ok()) << s.ToString();

    // reopen
    ReOpen();

    uint64_t index = 0;
    s = storage_->FirstIndex(&index);
    ASSERT_EQ(index, 1);
    s = storage_->LastIndex(&index);
    ASSERT_EQ(index, 99);

    ents.clear();
    s = storage_->Entries(lo, hi, std::numeric_limits<uint64_t>::max(), &ents,
                          &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);
    s = Equal(ents, to_writes);
    ASSERT_TRUE(s.ok()) << s.ToString();

    // write and read
    RandomEntries(hi, hi + 10, 256, &to_writes);
    s = storage_->StoreEntries(
        std::vector<EntryPtr>(to_writes.begin() + hi - 1, to_writes.end()));
    ASSERT_TRUE(s.ok()) << s.ToString();
    hi += 10;
    ents.clear();
    s = storage_->Entries(lo, hi, std::numeric_limits<uint64_t>::max(), &ents,
                          &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);
    s = Equal(ents, to_writes);
    ASSERT_TRUE(s.ok()) << s.ToString();
}

TEST_F(StorageTest, Corrupt2) {
    uint64_t lo = 1, hi = 100;
    std::vector<EntryPtr> to_writes;
    RandomEntries(lo, hi, 256, &to_writes);
    auto s = storage_->StoreEntries(to_writes);
    ASSERT_TRUE(s.ok()) << s.ToString();

    // append corrupt
    storage_->TEST_Add_Corruption2();

    // reopen
    ReOpen();

    uint64_t index = 0;
    s = storage_->FirstIndex(&index);
    ASSERT_EQ(index, 1U);
    s = storage_->LastIndex(&index);
    ASSERT_LT(index, 99U);
    ASSERT_GE(index, 1U);
    while (to_writes.size() > index) {
        to_writes.pop_back();
    }

    hi = index + 1;
    std::vector<EntryPtr> ents;
    bool compacted = false;
    s = storage_->Entries(lo, hi, std::numeric_limits<uint64_t>::max(), &ents,
                          &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);
    s = Equal(ents, to_writes);
    ASSERT_TRUE(s.ok()) << s.ToString();

    // write and read
    RandomEntries(hi, hi + 10, 256, &to_writes);
    s = storage_->StoreEntries(
        std::vector<EntryPtr>(to_writes.begin() + hi - 1, to_writes.end()));
    ASSERT_TRUE(s.ok()) << s.ToString();
    hi += 10;
    ents.clear();
    s = storage_->Entries(lo, hi, std::numeric_limits<uint64_t>::max(), &ents,
                          &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);
    s = Equal(ents, to_writes);
    ASSERT_TRUE(s.ok()) << s.ToString();
}
#endif

TEST_F(StorageTest, LogReader) {
    // read empty
    {
        auto reader = storage_->NewReader(1);
        ASSERT_TRUE(reader != nullptr);
        uint64_t index = 0;
        std::string data;
        bool over = false;
        auto s = reader->Next(index, data, over);
        ASSERT_TRUE(over);
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    uint64_t lo = 1, hi = 100;
    std::vector<EntryPtr> to_writes;
    RandomEntries(lo, hi, 256, &to_writes);
    auto s = storage_->StoreEntries(to_writes);
    ASSERT_TRUE(s.ok()) << s.ToString();

    // filter normal entries
    std::vector<EntryPtr> normal_entries;
    for (const auto& ent : to_writes) {
        if (ent->type() == pb::ENTRY_NORMAL) {
            EntryPtr ne(new pb::Entry(*ent));
            normal_entries.push_back(std::move(ne));
        }
    }

    auto reader = storage_->NewReader(lo);
    ASSERT_TRUE(reader != nullptr);
    size_t i = 0;
    bool over = false;
    uint64_t index = 0;
    std::string data;
    while (true) {
        s = reader->Next(index, data, over);
        ASSERT_TRUE(s.ok()) << s.ToString();
        if (over) {
            ASSERT_EQ(i, normal_entries.size());
            break;
        }
        ASSERT_EQ(index, normal_entries[i]->index());
        ASSERT_EQ(data, normal_entries[i]->data());
        ++i;
    }
}

TEST_F(StorageTest, InheritLog) {
    uint64_t lo = 1, hi = 100;
    uint64_t split_index = 50;
    std::vector<EntryPtr> to_writes;
    RandomEntries(lo, hi, 256, &to_writes);
    to_writes[split_index-lo]->set_flags(jimkv::raft::EntryFlags::kForceRotate);
    auto s = storage_->StoreEntries(to_writes);
    ASSERT_TRUE(s.ok()) << s.ToString();

    auto inherit_path = tmp_dir_ + "_inherit";
    s = storage_->InheritLog(inherit_path, split_index, false);
    ASSERT_TRUE(s.ok()) << s.ToString();

    // destroy old one
    s = storage_->Destroy(false);
    ASSERT_TRUE(s.ok()) << s.ToString();

    DiskStorage ds(2, inherit_path, DiskStorage::Options());
    s = ds.Open();
    ASSERT_TRUE(s.ok()) << s.ToString();

    uint64_t index = 0;
    s = ds.FirstIndex(&index);
    ASSERT_EQ(index, 1U);
    s = ds.LastIndex(&index);
    ASSERT_EQ(index, 50U);
    ASSERT_EQ(ds.InheritIndex(), split_index);

    std::vector<EntryPtr> ents;
    bool compacted = false;
    s = ds.Entries(1, split_index + 1, std::numeric_limits<uint64_t>::max(), &ents,
                          &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);
    s = Equal(ents, { to_writes.begin(), to_writes.begin() + split_index - lo + 1});
    ASSERT_TRUE(s.ok()) << s.ToString();

    // test Term()
    for (uint64_t i = lo; i < split_index + 1; ++i) {
        uint64_t term = 0;
        bool compacted = false;
        s = ds.Term(i, &term, &compacted);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(compacted);
        ASSERT_EQ(term, to_writes[i - lo]->term());
    }

    // read compacted entry
    ents.clear();
    s = storage_->Entries(0, split_index + 2, std::numeric_limits<uint64_t>::max(), &ents, &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_TRUE(compacted);
    ASSERT_TRUE(ents.empty());

    // append and check
    s = ds.StoreEntries({to_writes.begin() + split_index - lo, to_writes.end()});
    ASSERT_TRUE(s.ok()) << s.ToString();

    ents.clear();
    compacted = false;
    s = ds.Entries(1, hi, std::numeric_limits<uint64_t>::max(), &ents, &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);
    s = Equal(ents, to_writes);
    ASSERT_TRUE(s.ok()) << s.ToString();

    // test Term()
    for (uint64_t i = lo; i < hi; ++i) {
        uint64_t term = 0;
        bool compacted = false;
        s = ds.Term(i, &term, &compacted);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(compacted);
        ASSERT_EQ(term, to_writes[i - lo]->term());
    }

    ds.Destroy(false);
}

TEST_F(StorageTest, InheritIndex) {
    uint64_t lo = 1, hi = 100;
    uint64_t split_index = 50;
    std::vector<EntryPtr> to_writes;
    RandomEntries(lo, hi, 256, &to_writes);
    to_writes[split_index-lo]->set_flags(jimkv::raft::EntryFlags::kForceRotate);
    auto s = storage_->StoreEntries(to_writes);
    ASSERT_TRUE(s.ok()) << s.ToString();

    auto inherit_path = tmp_dir_ + "_inherit";
    s = storage_->InheritLog(inherit_path, split_index, true);
    ASSERT_TRUE(s.ok()) << s.ToString();

    // destroy old one
    s = storage_->Destroy(false);
    ASSERT_TRUE(s.ok()) << s.ToString();

    DiskStorage ds(2, inherit_path, DiskStorage::Options());
    s = ds.Open();
    ASSERT_TRUE(s.ok()) << s.ToString();

    uint64_t index = 0;
    s = ds.FirstIndex(&index);
    ASSERT_EQ(index, split_index + 1);
    s = ds.LastIndex(&index);
    ASSERT_EQ(index, split_index);
    ASSERT_EQ(ds.InheritIndex(), 0U);

    std::vector<EntryPtr> ents;
    bool compacted = false;
    s = ds.Entries(split_index, hi, std::numeric_limits<uint64_t>::max(), &ents,
                   &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_TRUE(compacted);

    to_writes.clear();
    RandomEntries(split_index+1, 200, 256, &to_writes);
    s = ds.StoreEntries(to_writes);
    ASSERT_TRUE(s.ok()) << s.ToString();

    ents.clear();
    compacted = false;
    s = ds.Entries(split_index+1, 200, std::numeric_limits<uint64_t>::max(), &ents,
                   &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);
    s = Equal(ents, to_writes);
    ASSERT_TRUE(s.ok()) << s.ToString();

    ds.Destroy(false);
}

TEST_F(StorageTest, ForceRotate) {
    uint64_t lo = 1, hi = 100;
    std::vector<EntryPtr> to_writes;
    RandomEntries(lo, hi, 256, &to_writes);
    to_writes.back()->set_flags(jimkv::raft::EntryFlags::kForceRotate);
    auto s = storage_->StoreEntries(to_writes);
    ASSERT_TRUE(s.ok()) << s.ToString();

    uint64_t index = 0;
    s = storage_->FirstIndex(&index);
    ASSERT_EQ(index, 1UL);
    s = storage_->LastIndex(&index);
    ASSERT_EQ(index, 99UL);

    // read one by one
    for (uint64_t index = lo; index < hi; ++index) {
        std::vector<EntryPtr> ents;
        bool compacted = false;
        s = storage_->Entries(index, index + 1, std::numeric_limits<uint64_t>::max(),
                              &ents, &compacted);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(compacted);
        ASSERT_EQ(ents.size(), 1U);
        s = Equal(ents[0], to_writes[index-lo]);
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    // read all entries
    std::vector<EntryPtr> ents;
    bool compacted = false;
    s = storage_->Entries(lo, hi, std::numeric_limits<uint64_t>::max(), &ents,
                          &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);
    s = Equal(ents, to_writes);
    ASSERT_TRUE(s.ok()) << s.ToString();

    // test Term()
    for (uint64_t i = lo; i < hi; ++i) {
        uint64_t term = 0;
        bool compacted = false;
        s = storage_->Term(i, &term, &compacted);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(compacted);
        ASSERT_EQ(term, to_writes[i - 1]->term());
    }
}


} /* namespace  */
