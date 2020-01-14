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

#include "base/fs_util.h"
#include "db/mass_tree_impl/wal.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {

using namespace jim;
using namespace jim::ds::db;

class WALTest: public ::testing::Test {
protected:
    void SetUp() override {
        char path[] = "/tmp/jim_ds_wal_test_XXXXXX";
        char *tmp = mkdtemp(path);
        ASSERT_TRUE(tmp != NULL);
        data_path_ = tmp;
    }

    void TearDown() override {
        if (!data_path_.empty()) {
            jim::RemoveDirAll(data_path_.c_str());
        }
    }

protected:
    std::string data_path_;
};

void randBatch(MassTreeBatch& batch) {
    batch.SetIndex(randomInt());
    auto count = 1 + randomInt() % 5;
    for (int i = 0; i < count; ++i) {
        KvEntry entry;
        entry.cf = randomInt() % 2 == 0 ? CFType::kData : CFType::kTxn;
        entry.type = randomInt() % 2 == 0 ? KvEntryType::kPut : KvEntryType::kDelete;
        entry.key = randomString(10, 30);
        if (entry.type == KvEntryType::kPut) {
            entry.value = randomString(10, 30);
        }
        batch.Append(entry.type, entry.cf, entry.key, entry.value);
    }
}

TEST_F(WALTest, Empty) {
    auto file_path = JoinFilePath({data_path_, "1.wal"});

    WALWriter writer(file_path);
    auto s = writer.Open();
    ASSERT_TRUE(s.ok()) << s.ToString();

    WALReader reader(file_path);
    s = reader.Open();
    ASSERT_TRUE(s.ok()) << s.ToString();

    MassTreeBatch batch;
    uint64_t index = 0;
    bool over = false;
    s = reader.Next(batch, over);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_TRUE(over);
}


TEST_F(WALTest, WriteRead) {
    auto file_path = JoinFilePath({data_path_, "1.wal"});

    std::vector<MassTreeBatch> records;
    auto count = 1 + randomInt() % 100;
    for (int i = 0; i < count; ++i) {
        MassTreeBatch batch;
        randBatch(batch);
        records.push_back(batch);
    }

    WALWriter writer(file_path);
    auto s = writer.Open();
    ASSERT_TRUE(s.ok()) << s.ToString();
    for (const auto& rec: records) {
        s = writer.Append(rec);
        ASSERT_TRUE(s.ok()) << s.ToString();
    }
    ASSERT_TRUE(s.ok()) << s.ToString();
    s = writer.Sync();
    ASSERT_TRUE(s.ok()) << s.ToString();

    WALReader reader(file_path);
    s = reader.Open();
    ASSERT_TRUE(s.ok()) << s.ToString();

    bool over = false;
    for (const auto& expected_rec : records) {
        MassTreeBatch actaul_rec;
        s = reader.Next(actaul_rec, over);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(over);
        s = actaul_rec.AssertEuqalWith(expected_rec);
        ASSERT_TRUE(s.ok()) << s.ToString();
    }
    MassTreeBatch actaul_rec;
    s = reader.Next(actaul_rec, over);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_TRUE(over);
}

}
