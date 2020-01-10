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

#include "raft/src/impl/raft_log_unstable.h"
#include "test_util.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {

using namespace jimkv::raft::impl;
using namespace jimkv::raft::impl::testutil;

TEST(Unstable, All) {
    UnstableLog log(100);
    ASSERT_EQ(log.offset(), 100U);
    uint64_t last_index = 0;
    ASSERT_FALSE(log.maybeLastIndex(&last_index));

    std::vector<EntryPtr> entries;
    RandomEntries(100, 200, 64, &entries);
    log.truncateAndAppend(entries);
    ASSERT_TRUE(log.maybeLastIndex(&last_index));
    ASSERT_EQ(last_index, 199U);

    // test maybeTerm
    uint64_t term = 0;
    ASSERT_FALSE(log.maybeTerm(99, &term));
    ASSERT_FALSE(log.maybeTerm(200, &term));
    for (uint64_t i = 100; i < 200; ++i) {
        ASSERT_TRUE(log.maybeTerm(i, &term));
        ASSERT_EQ(term, entries[i - 100]->term());
    }

    // test entries()
    std::vector<EntryPtr> entries2;
    log.entries(&entries2);
    auto s = Equal(entries2, entries);
    ASSERT_TRUE(s.ok()) << s.ToString();

    // test slice()
    entries2.clear();
    log.slice(100, 200, &entries2);
    s = Equal(entries2, entries);
    ASSERT_TRUE(s.ok()) << s.ToString();
    entries2.clear();
    log.slice(150, 160, &entries2);
    s = Equal(entries2,
              std::vector<EntryPtr>(entries.begin() + 50, entries.begin() + 60));
    ASSERT_TRUE(s.ok()) << s.ToString();

    // test stableTo
    log.stableTo(entries[50]->index(), entries[50]->term());
    entries2.clear();
    log.entries(&entries2);
    s = Equal(entries2, std::vector<EntryPtr>(entries.begin() + 51, entries.end()));
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(log.offset(), entries[51]->index());

    // test restore
    log.restore(500);
    entries2.clear();
    log.entries(&entries2);
    ASSERT_TRUE(entries2.empty());
    ASSERT_EQ(log.offset(), 501U);
}

TEST(UnstableLog, Append) {
    UnstableLog log(100);

    std::vector<EntryPtr> ents1;
    RandomEntries(100, 200, 64, &ents1);
    log.truncateAndAppend(ents1);

    std::vector<EntryPtr> ents;
    log.entries(&ents);
    auto s = Equal(ents, ents1);
    ASSERT_TRUE(s.ok()) << s.ToString();

    std::vector<EntryPtr> ents2;
    RandomEntries(50, 150, 64, &ents2);
    log.truncateAndAppend(ents2);  // 50-150
    ASSERT_EQ(log.offset(), 50U);
    ents.clear();
    log.entries(&ents);
    s = Equal(ents, ents2);
    ASSERT_TRUE(s.ok()) << s.ToString();

    log.truncateAndAppend(ents1);
    ASSERT_EQ(log.offset(), 50U);
    ents.clear();
    log.entries(&ents);  // 50-200
    std::vector<EntryPtr> ents4(ents2.begin(), ents2.begin() + 50);
    std::copy(ents1.begin(), ents1.end(), std::back_inserter(ents4));
    s = Equal(ents, ents4);
    ASSERT_TRUE(s.ok()) << s.ToString();

    std::vector<EntryPtr> ents5;
    RandomEntries(200, 250, 64, &ents5);
    log.truncateAndAppend(ents5);
    ents.clear();
    log.entries(&ents);  // 50-250
    std::copy(ents5.begin(), ents5.end(), std::back_inserter(ents4));
    s = Equal(ents, ents4);
    ASSERT_TRUE(s.ok()) << s.ToString();
}

}  // namespace
