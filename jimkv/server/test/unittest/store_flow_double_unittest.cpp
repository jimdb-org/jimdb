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

#include "base/util.h"
#include "helper/store_test_fixture.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {

using namespace jimkv;
using namespace jimkv::test::helper;
using namespace jimkv::ds;

const double height_base = 0.36;

// test base the account table
class StoreTest : public StoreTestFixture {
public:
    StoreTest() : StoreTestFixture(CreatePersonTable()) {}

    void InitRows(uint64_t *total_bytes = nullptr) {
        double height_base = 0.36;
        for (int i = 1; i <= 10; ++i) {
            std::vector<std::string> row;
            row.push_back(std::to_string(i));

            char name[32] = {'\0'};
            snprintf(name, 32, "person-%04d", i);
            row.emplace_back(name);

            row.push_back(std::to_string(20 + i));
            row.push_back(std::to_string(height_base + 155 + i));

            rows_.push_back(std::move(row));

        }
        auto s = testInsert(rows_, total_bytes);
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

protected:
    // inserted rows
    uint64_t index_ = 0;
    std::vector<std::vector<std::string>> rows_;
};

TEST_F(StoreTest, SelectFlows) {
    InitRows();
    std::vector<std::vector<std::string>> rows_tmp;
    // select * from person 
    {
        auto s = testSelectFlow(
                [](SelectFlowRequestBuilder& b) {
                    b.SetSQL("select * from person");
                },
                rows_
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    // select * from person where height = 156.36
    {
        rows_tmp.push_back(rows_[0]);
        auto s = testSelectFlow(
                [](SelectFlowRequestBuilder& b) {
                    b.SetSQL("select * from person where height = 156.36");
                },
                rows_tmp
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    // select * from person where height <= 156.36
    {
        rows_tmp.clear();
        rows_tmp.push_back(rows_[0]);
        auto s = testSelectFlow(
                [](SelectFlowRequestBuilder& b) {
                    b.SetSQL("select * from person where height <= 156.36");
                },
                rows_tmp
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    // select * from person where height < 156.361
    {
        rows_tmp.clear();
        rows_tmp.push_back(rows_[0]);
        auto s = testSelectFlow(
                [](SelectFlowRequestBuilder& b) {
                    b.SetSQL("select * from person where height < 156.361");
                },
                rows_tmp
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    // select * from person where height >= 165.36
    {
        rows_tmp.clear();
        rows_tmp.push_back(rows_[9]);
        auto s = testSelectFlow(
                [](SelectFlowRequestBuilder& b) {
                    b.SetSQL("select * from person where height >= 165.36");
                },
                rows_tmp
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    // select * from person where height > 165.35
    {
        rows_tmp.clear();
        rows_tmp.push_back(rows_[9]);
        auto s = testSelectFlow(
                [](SelectFlowRequestBuilder& b) {
                    b.SetSQL("select * from person where height > 165.35");
                },
                rows_tmp
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    // select * from person where height != 165.36
    {
        rows_tmp.clear();
        rows_tmp = rows_;
        rows_tmp.pop_back();
        auto s = testSelectFlow(
                [](SelectFlowRequestBuilder& b) {
                    b.SetSQL("select * from person where height != 165.36");
                },
                rows_tmp
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    // select height from person where height != 165.36
    {
        rows_tmp.clear();
        for (auto &row : rows_) {
            rows_tmp.push_back({row[3]});
        }
        rows_tmp.pop_back();

        auto s = testSelectFlow(
                [](SelectFlowRequestBuilder& b) {
                    b.SetSQL("select height from person where height != 165.36");
                },
                rows_tmp
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    // select height from person where height = 155.36 + 1.0
    {
        rows_tmp.clear();
        rows_tmp.push_back(rows_[0]);
        auto s = testSelectFlow(
                [](SelectFlowRequestBuilder& b) {
                    b.SetSQL("select * from person where height = 155.36 + 1.0");
                },
                rows_tmp
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    // select height from person where height = 157.36 - 1.0
    {
        rows_tmp.clear();
        rows_tmp.push_back(rows_[0]);
        auto s = testSelectFlow(
                [](SelectFlowRequestBuilder& b) {
                    b.SetSQL("select * from person where height = 157.36 - 1.0");
                },
                rows_tmp
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    // select height from person where height = 78.18 * 2.0
    {
        rows_tmp.clear();
        rows_tmp.push_back(rows_[0]);
        auto s = testSelectFlow(
                [](SelectFlowRequestBuilder& b) {
                    b.SetSQL("select * from person where height = 78.18 * 2.0");
                },
                rows_tmp
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    // select height from person where height = 312.72 / 2.0
    {
        rows_tmp.clear();
        rows_tmp.push_back(rows_[0]);
        auto s = testSelectFlow(
                [](SelectFlowRequestBuilder& b) {
                    b.SetSQL("select * from person where height = 312.72 / 2.0");
                },
                rows_tmp
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    // select * from person where height = 312.72 / 2.0 * 4.0 / 4.0
    {
        rows_tmp.clear();
        rows_tmp.push_back(rows_[0]);
        auto s = testSelectFlow(
                [](SelectFlowRequestBuilder& b) {
                    b.SetSQL("select * from person where height = 312.72 / 2.0 * 4.0 / 4.0");
                },
                rows_tmp
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }
    
    // select * from person where height = age + 135.36
    {
        // rows_tmp.clear();
        // rows_tmp.push_back(rows_[0]);
        // auto s = testSelectFlow(
        //         [](SelectFlowRequestBuilder& b) {
        //             b.SetSQL("select * from person where height = age + 135.36");
        //         },
        //         rows_tmp
        // );
        // ASSERT_TRUE(s.ok()) << s.ToString();
    }

    // select height from person where height = 312.72 div 2.0
    // sql-parse no support
    // {
    //     rows_tmp.clear();
    //     rows_tmp.push_back(rows_[0]);
    //     auto s = testSelectFlow(
    //             [](SelectFlowRequestBuilder& b) {
    //                 b.SetSQL("select * from person where height = 312.72 div 2.0");
    //             },
    //             rows_tmp
    //     );
    //     ASSERT_TRUE(s.ok()) << s.ToString();
    // }
}

} /* namespace  */
