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
#include "db/mass_tree_impl/mass_tree_wrapper.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

using namespace jim;
using namespace jim::ds::db;

namespace {

TEST(MassTree, PutGetDelete) {
    MasstreeWrapper db;
    for (int i = 0; i < 1000; ++i) {
        std::string key = jim::randomString(32, 1024);
        std::string value = jim::randomString(64);
        auto s = db.Put(key, value);
        ASSERT_TRUE(s.ok()) << s.ToString();

        std::string actual_value;
        s = db.Get(key, actual_value);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_EQ(actual_value, value);

        s = db.Delete(key);
        ASSERT_TRUE(s.ok()) << s.ToString();
        s = db.Get(key, actual_value);
        ASSERT_EQ(s.code(), Status::kNotFound);
    }
}

TEST(MassTree, Iter) {
    struct KeyValue {
        std::string key;
        std::string value;

        bool operator<(const KeyValue &rh) const {
            return key < rh.key;
        }
    };

    std::set<KeyValue> key_values;
    MasstreeWrapper tree;
    for (int i = 0; i < 500; ++i) {
        std::string key = jim::randomString(32, 1024);
        std::string value = jim::randomString(64);
        tree.Put(key, value);
        key_values.insert(KeyValue{key, value});
    }
    auto tree_iter = tree.NewIterator("", "");
    size_t count = 0;
    auto set_iter = key_values.begin();
    while (tree_iter->Valid()) {
//        std::cout << tree_iter->Key() << std::endl;
        ASSERT_LT(count, key_values.size());
        ASSERT_EQ(tree_iter->Key(), set_iter->key) << " at index " << count;
        ASSERT_EQ(tree_iter->Value(), set_iter->value) << " at index " << count;
        ++count;
        ++set_iter;
        tree_iter->Next();
    }
    ASSERT_EQ(count, key_values.size());
}

TEST(MassTree, DISABLED_GC) {
    MasstreeWrapper tree;
    std::set<std::string> keys;
    for (int i = 0; i < 100000; ++i) {
        auto key = randomString(10, 1024);
        auto s = tree.Put(key, "");
        ASSERT_TRUE(s.ok()) << s.ToString();
        keys.insert(key);
    }
    std::cout << "Put over. " << std::endl;
    std::cout << "======================================================= " << std::endl;
    std::cout << tree.CollectTreeCouter() << std::endl;
    std::cout << "======================================================= " << std::endl;

    for (auto it = keys.cbegin(); it != keys.cend(); ++it) {
        std::string value;
        auto s = tree.Get(*it, value);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_TRUE(value.empty());
    }

    for (auto it = keys.cbegin(); it != keys.cend(); ++it) {
        auto s = tree.Delete(*it);
        ASSERT_TRUE(s.ok()) << s.ToString();
    }
    std::cout << "Delete over. " << std::endl;
    std::cout << tree.CollectTreeCouter() << std::endl;

    for (auto it = keys.cbegin(); it != keys.cend(); ++it) {
        std::string value;
        auto s = tree.Get(*it, value);
        ASSERT_EQ(s.code(), Status::kNotFound);
    }
    std::cout << "Get over. " << std::endl;
    std::cout << tree.CollectTreeCouter() << std::endl;

    for (int i = 0; i < 100; ++i) {
        sleep(10);
        std::cout << tree.CollectTreeCouter() << std::endl;
    }
}

} /* namespace  */
