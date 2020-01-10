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
#include "db/mass_tree_impl/checkpoint.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {

using namespace jimkv;
using namespace jimkv::ds::db;

class CheckpointTest: public ::testing::Test {
protected:
    CheckpointTest() = default;

    void SetUp() override {
        char path[] = "/tmp/jimkv_ds_checkpoint_test_XXXXXX";
        char *tmp = mkdtemp(path);
        ASSERT_TRUE(tmp != NULL);
        data_path_ = tmp;
    }

    void TearDown() override {
        if (!data_path_.empty()) {
            jimkv::RemoveDirAll(data_path_.c_str());
        }
    }

protected:
    std::string data_path_;
};


TEST_F(CheckpointTest, OnlyHeader) {
    uint64_t applied = randomInt();
    std::string file_path = JoinFilePath({data_path_, "1.ckp"});

    CheckpointWriter writer(file_path, applied, true);
    auto s = writer.Open();
    ASSERT_TRUE(s.ok()) << s.ToString();
    s = writer.Finish();
    ASSERT_TRUE(s.ok()) << s.ToString();

    CheckpointReader reader(file_path, true);
    s = reader.Open();
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(reader.AppliedIndex(), applied);
    ASSERT_EQ(reader.Header().version, kCheckpointVersion);
    ASSERT_EQ(reader.Header().timestamp, writer.Timestamp());
    std::cout << "timestamp: " << reader.Header().timestamp << std::endl;

    CFType cf;
    std::string key, value;
    bool over = false;
    s = reader.Next(cf, key, value, over);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_TRUE(over);
    ASSERT_TRUE(key.empty());
    ASSERT_TRUE(value.empty());
}

struct KvRecord {
    CFType cf;
    std::string key;
    std::string value;
};

KvRecord randomKV() {
    KvRecord kv;
    kv.cf = randomInt() % 2 == 0 ? CFType::kData: CFType::kTxn;
    kv.key = randomString(10, 50);
    kv.value = randomString(10, 50);
    return kv;
}

TEST_F(CheckpointTest, ReadWrite) {
    uint64_t applied = randomInt();
    std::string file_path = JoinFilePath({data_path_, "1.ckp"});
    std::vector<KvRecord> kvs;

    CheckpointWriter writer(file_path, applied, true);
    auto s = writer.Open();
    ASSERT_TRUE(s.ok()) << s.ToString();
    auto count = randomInt() % 500 + 100;
    std::cout << "write kv count: " << count << std::endl;
    for (int i = 0; i < count; ++i) {
        auto kv = randomKV();
        kv.key = std::to_string(i) + kv.key;
        kvs.push_back(kv);
        s = writer.Append(kv.cf, kv.key, kv.value);
        ASSERT_TRUE(s.ok()) << s.ToString();
    }
    s = writer.Finish();
    ASSERT_TRUE(s.ok()) << s.ToString();

    // read and check
    CheckpointReader reader(file_path, true);
    s = reader.Open();
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(reader.AppliedIndex(), applied);
    ASSERT_EQ(reader.Header().version, kCheckpointVersion);
    ASSERT_EQ(reader.Header().timestamp, writer.Timestamp());
    std::cout << "timestamp: " << reader.Header().timestamp << std::endl;

    CFType cf;
    std::string key, value;
    bool over = false;
    for (const auto& kv : kvs) {
        s = reader.Next(cf, key, value, over);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(over);

        ASSERT_EQ(cf, kv.cf);
        ASSERT_EQ(key, kv.key);
        ASSERT_EQ(value, kv.value);
    }
    // last one
    s = reader.Next(cf, key, value, over);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_TRUE(over) << "key: " << key;
}

}

