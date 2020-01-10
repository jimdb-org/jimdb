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

#include "db/mass_tree_impl/mass_tree_wrapper.h"
#include "common/ds_encoding.h"
#include "base/util.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {

using namespace jimkv;
using namespace jimkv::ds;
using namespace jimkv::ds::db;

class ScanerTest: public ::testing::Test {
public:
    ScanerTest() = default;
    ~ScanerTest() = default;

    void SetUp() override {
    }

    void TearDown() override {
    }

    MasstreeWrapper tree_;
};

void test_scan(std::unique_ptr<Iterator> scaner_ptr,
               uint64_t expected_starti, uint64_t* expected_endi) {
    auto i = expected_starti;
    auto scaner = scaner_ptr.get();
    while (scaner->Valid()) {
        auto k = scaner->Key();
        auto v = scaner->Value();

//        std::string buf;
//        EncodeUvarintAscending(&buf, i);
//        ASSERT_TRUE(k == buf);

        scaner->Next();
        ++i;
    }
    *expected_endi = i;
}

TEST_F(ScanerTest, empty) {
    auto iter = tree_.NewIterator("", "");
    ASSERT_FALSE(iter->Valid());
}

TEST_F(ScanerTest, rows100) {
    for (auto i = 0; i < 10; i++) {
        std::string buf;
        EncodeUvarintAscending(&buf, i);
        tree_.Put(buf, buf);
    }

    { // "" ... ""
        std::string start("");
        std::string end("");
        uint64_t expected_endi;

        test_scan(tree_.NewIterator(start, end), 0, &expected_endi);
        ASSERT_TRUE(expected_endi == 10);
    }

    { // "" ... 1
        std::string start;
        std::string end;
        uint64_t expected_endi;

        EncodeUvarintAscending(&end, 1);
        test_scan(tree_.NewIterator(start, end), 0, &expected_endi);
        ASSERT_TRUE(expected_endi == 1);
    }
    { // "" ... 2
        std::string start;
        std::string end;
        uint64_t expected_endi;

        EncodeUvarintAscending(&end, 2);
        test_scan(tree_.NewIterator(start, end), 0, &expected_endi);
        ASSERT_TRUE(expected_endi == 2);
    }
    { // "" ... 3
        std::string start;
        std::string end;
        uint64_t expected_endi;

        EncodeUvarintAscending(&end, 3);
        test_scan(tree_.NewIterator(start, end), 0, &expected_endi);
        ASSERT_TRUE(expected_endi == 3);
    }

    { // "" ... 9
        std::string start;
        std::string end;
        uint64_t expected_endi;

        EncodeUvarintAscending(&end, 9);
        test_scan(tree_.NewIterator(start, end), 0, &expected_endi);
        ASSERT_TRUE(expected_endi == 9);
    }
    { // "" ... 10
        std::string start;
        std::string end;
        uint64_t expected_endi;

        EncodeUvarintAscending(&end, 10);
        test_scan(tree_.NewIterator(start, end), 0, &expected_endi);
        ASSERT_TRUE(expected_endi == 10);
    }
    { // "" ... 11
        std::string start;
        std::string end;
        uint64_t expected_endi;

        EncodeUvarintAscending(&end, 11);
        test_scan(tree_.NewIterator(start, end), 0, &expected_endi);
        ASSERT_TRUE(expected_endi == 10);
    }

    { // 1 ... 9
        std::string start;
        std::string end;
        uint64_t expected_endi;

        EncodeUvarintAscending(&start, 1);
        EncodeUvarintAscending(&end, 9);
        test_scan(tree_.NewIterator(start, end), 1, &expected_endi);
        ASSERT_TRUE(expected_endi == 9);
    }
    { // 1 ... 10
        std::string start;
        std::string end;
        uint64_t expected_endi;

        EncodeUvarintAscending(&start, 1);
        EncodeUvarintAscending(&end, 10);
        test_scan(tree_.NewIterator(start, end), 1, &expected_endi);
        ASSERT_TRUE(expected_endi == 10);
    }
    { // 1 ... 11
        std::string start;
        std::string end;
        uint64_t expected_endi;

        EncodeUvarintAscending(&start, 1);
        EncodeUvarintAscending(&end, 11);
        test_scan(tree_.NewIterator(start, end), 1, &expected_endi);
        ASSERT_TRUE(expected_endi == 10);
    }
}

TEST_F(ScanerTest, rows234) {
    for (auto i = 0; i < 234; i++) {
        std::string key;
        EncodeUvarintAscending(&key, i);
        tree_.Put(key, key);
    }
    for (auto i = 0; i < 234; i++) {
        std::string key;
        EncodeUvarintAscending(&key, i);

        std::string value;
        tree_.Get(key, value);
        ASSERT_TRUE(value == key);
    }

    { // "" ... ""
        std::string start("");
        std::string end("");
        uint64_t expected_endi;

        test_scan(tree_.NewIterator(start, end), 0, &expected_endi);
        ASSERT_TRUE(expected_endi == 234);
    }

}

TEST_F(ScanerTest, rows500) {
    for (auto i = 0; i < 500; i++) {
        auto key = jimkv::randomString(200);
        tree_.Put(key, key);
    }
    { // "\0" ... "\xff"
        std::string start("");
        std::string end("\xff", 1);
        uint64_t expected_endi;

        test_scan(tree_.NewIterator(start, end), 0, &expected_endi);
        ASSERT_TRUE(expected_endi == 500);
    }
}

}

