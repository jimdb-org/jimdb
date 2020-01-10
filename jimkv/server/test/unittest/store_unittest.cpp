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

// test base the account table
class StoreTest : public StoreTestFixture {
public:
    StoreTest() : StoreTestFixture(CreateAccountTable()) {}

    void InsertSomeRows(uint64_t *total_bytes = nullptr) {
        for (int i = 1; i <= 100; ++i) {
            std::vector<std::string> row;
            row.push_back(std::to_string(i));

            char name[32] = {'\0'};
            snprintf(name, 32, "user-%04d", i);
            row.emplace_back(name);

            row.push_back(std::to_string(100 + i));
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


TEST_F(StoreTest, KeyValue) {
    // test put and get
    std::string key = jimkv::randomString(32);
    std::string value = jimkv::randomString(64);
    auto s = store_->Put(key, value, ++index_);
    ASSERT_TRUE(s.ok()) << s.ToString();

    std::string actual_value;
    s = store_->Get(key, &actual_value);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(actual_value, value);

    // test delete and get
    s = store_->Delete(key, ++index_);
    ASSERT_TRUE(s.ok());
    s = store_->Get(key, &actual_value);
    ASSERT_FALSE(s.ok());
    ASSERT_EQ(s.code(), jimkv::Status::kNotFound);
}

TEST_F(StoreTest, Insert) {
    // one
    auto s = testInsert({{"1", "user1", "1.1"}});
    ASSERT_TRUE(s.ok()) << s.ToString();

    // multi
    {
        std::vector<std::vector<std::string>> rows;
        for (int i = 0; i < 100; ++i) {
            rows.push_back({std::to_string(i), "user", "1.1"});
        }
        auto s = testInsert(rows);
        ASSERT_TRUE(s.ok()) << s.ToString();
    }
    // insert check duplicate
    {
        InsertRequestBuilder builder(table_.get());
        builder.AddRow({"1", "user1", "100"});
        builder.SetCheckUnique();
        auto req = builder.Build();
        dspb::PrepareResponse resp;
        store_->TxnPrepare(req, ++index_, &resp);
        ASSERT_GT(resp.errors_size(), 0);
        ASSERT_TRUE(resp.errors(0).has_not_unique());
    }
}

TEST_F(StoreTest, SelectEmpty) {
    // all fields
    auto s = testSelect(
            [](SelectRequestBuilder& b) { b.SetSQL("select * from account"); },
            {});
    ASSERT_TRUE(s.ok()) << s.ToString();

//    // select count
//    s = testSelect(
//            [](SelectRequestBuilder& b) { b.SetSQL("select count(*) from account"); },
//            {{"0"}});
//    ASSERT_TRUE(s.ok()) << s.ToString();
}

TEST_F(StoreTest, SelectFields) {
    InsertSomeRows();

    // select all rows
    {
        auto s = testSelect(
                [](SelectRequestBuilder& b) {
                    b.SetSQL("select * from account");
                },
                rows_
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    // Select one row per loop, all fields
    {
        for (size_t i = 0; i < rows_.size(); ++i) {
            auto s = testSelect(
                    [this, i](SelectRequestBuilder& b) {
                        b.SetSQL("select * from account");
                        b.SetKey({rows_[i][0]});
                    },
                    {rows_[i]}
            );
            ASSERT_TRUE(s.ok()) << s.ToString();
        }
    }

    // one filed
    {
        for (size_t i = 0; i < rows_.size(); ++i) {
            auto s = testSelect(
                    [this, i](SelectRequestBuilder& b) {
                        b.SetSQL("select id from account");
                        b.SetKey({rows_[i][0]});
                    },
                    {{rows_[i][0]}}
            );
            ASSERT_TRUE(s.ok()) << s.ToString();
        }

        for (size_t i = 0; i < rows_.size(); ++i) {
            auto s = testSelect(
                    [this, i](SelectRequestBuilder& b) {
                        b.SetSQL("select name from account");
                        b.SetKey({rows_[i][0]});
                    },
                    {{rows_[i][1]}}
            );
            ASSERT_TRUE(s.ok()) << s.ToString();
        }

        for (size_t i = 0; i < rows_.size(); ++i) {
            auto s = testSelect(
                    [this, i](SelectRequestBuilder& b) {
                        b.SetSQL("select balance from account");
                        b.SetKey({rows_[i][0]});
                    },
                    {{rows_[i][2]}}
            );
            ASSERT_TRUE(s.ok()) << s.ToString();
        }
    }

    // no pk fileds
    {
        for (size_t i = 0; i < rows_.size(); ++i) {
            auto s = testSelect(
                    [this, i](SelectRequestBuilder& b) {
                        b.SetSQL("select name, balance from account");
                        b.SetKey({rows_[i][0]});
                    },
                    {{rows_[i][1], rows_[i][2]}}
            );
            ASSERT_TRUE(s.ok()) << s.ToString();
        }
    }
}

TEST_F(StoreTest, SelectScope) {
    InsertSomeRows();

    // scope: [2-4)
    auto s = testSelect(
            [](SelectRequestBuilder& b) {
                b.SetSQL("select * from account");
                b.SetScope({"2"}, {"4"});
            },
            {rows_[1], rows_[2]}
    );
    ASSERT_TRUE(s.ok()) << s.ToString();

    // scope: [2-
    s = testSelect(
            [](SelectRequestBuilder& b) {
                b.SetSQL("select * from account");
                b.SetScope({"2"}, {});
            },
            {rows_.cbegin() + 1, rows_.cend()}
    );
    ASSERT_TRUE(s.ok()) << s.ToString();

    // scope: -4)
    s = testSelect(
            [](SelectRequestBuilder& b) {
                b.SetSQL("select * from account");
                b.SetScope({}, {"4"});
            },
            {rows_[0], rows_[1], rows_[2]}
    );
    ASSERT_TRUE(s.ok()) << s.ToString();
}

TEST_F(StoreTest, SelectLimit) {
    InsertSomeRows();

    auto s = testSelect(
            [](SelectRequestBuilder& b) {
                b.SetSQL("select * from account limit 3");
            },
            {rows_[0], rows_[1], rows_[2]}
    );
    ASSERT_TRUE(s.ok()) << s.ToString();

    // TODO: FIXME current parser doesn't support
//    s = testSelect(
//            [](SelectRequestBuilder& b) {
//                b.SetSQL("select * from account limit 3 1");
//            },
//            {rows_[1], rows_[2], rows_[3]}
//    );
//    ASSERT_TRUE(s.ok()) << s.ToString();
}

TEST_F(StoreTest, SelectWhere) {
    InsertSomeRows();

    // select * where id
    {
        // id == 1
        auto s = testSelect(
                [](SelectRequestBuilder& b) {
                    b.SetSQL("select * from account where id=1");
                },
                {rows_[0]}
        );
        ASSERT_TRUE(s.ok()) << s.ToString();

        // id != 1
        s = testSelect(
                [](SelectRequestBuilder& b) {
                    b.SetSQL("select * from account where id != 1");
                },
                {rows_.cbegin() + 1, rows_.cend()}
        );
        ASSERT_TRUE(s.ok()) << s.ToString();

        // id < 3
        s = testSelect(
                [](SelectRequestBuilder& b) {
                    b.SetSQL("select * from account where id < 3");
                },
                {rows_[0], rows_[1]}
        );
        ASSERT_TRUE(s.ok()) << s.ToString();

        // id <= 2
        s = testSelect(
                [](SelectRequestBuilder& b) {
                    b.SetSQL("select * from account where id <= 2");
                },
                {rows_[0], rows_[1]}
        );
        ASSERT_TRUE(s.ok()) << s.ToString();

        // id > 2
        s = testSelect(
                [](SelectRequestBuilder& b) {
                    b.SetSQL("select * from account where id > 2");
                },
                {rows_.cbegin() + 2, rows_.cend()}
        );
        ASSERT_TRUE(s.ok()) << s.ToString();

        // id >= 2
        s = testSelect(
                [](SelectRequestBuilder& b) {
                    b.SetSQL("select * from account where id >= 2");
                },
                {rows_.cbegin() + 1, rows_.cend()}
        );
        ASSERT_TRUE(s.ok()) << s.ToString();

        // id > 1 and id < 4
        s = testSelect(
                [](SelectRequestBuilder& b) {
                    b.SetSQL("select * from account where id > 1 and id < 4");
                },
                {rows_[1], rows_[2]}
        );
        ASSERT_TRUE(s.ok()) << s.ToString();

        // id > 4 and id < 1
        s = testSelect(
                [](SelectRequestBuilder& b) {
                    b.SetSQL("select * from account where id > 4 and id < 1");
                },
                {}
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    // select where name
    {
        auto s = testSelect(
                [](SelectRequestBuilder& b) {
                    b.SetSQL("select * from account where name > \'user-0002\'");
                },
                {rows_.cbegin() + 2, rows_.cend()}
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }
    // select where balance
    {
        auto s = testSelect(
                [](SelectRequestBuilder& b) {
                    b.SetSQL("select * from account where balance < 103");
                },
                {rows_[0], rows_[1]}
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }
}

// TODO: fix me
TEST_F(StoreTest, DISABLED_SelectAggreCount) {
    InsertSomeRows();

    // select count(*)
    {
        auto s = testSelect(
                [](SelectRequestBuilder& b) {
                    b.SetSQL("select count(*) from account");
                },
                {{std::to_string(rows_.size())}}
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    // select count(*) where id = {id}
    {
        for (size_t i = 0; i < rows_.size(); ++i) {
            auto s = testSelect(
                    [this, i](SelectRequestBuilder& b) {
                        b.SetKey({rows_[i][0]});
                        b.SetSQL("select count(*) from account");
                    },
                    {{"1"}}
            );
            ASSERT_TRUE(s.ok()) << s.ToString();
        }

        // not exist
        auto s = testSelect(
                [](SelectRequestBuilder& b) {
                    b.SetKey({std::to_string(std::numeric_limits<int64_t>::max())});
                    b.SetSQL("select count(*) from account");
                },
                {{"0"}}
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    // select count(*) where id
    {
        auto s = testSelect(
                [](SelectRequestBuilder& b) {
                    b.SetSQL("select count(*) from account where id = 1");
                },
                {{"1"}}
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }
    {
        auto s = testSelect(
                [](SelectRequestBuilder& b) {
                    b.SetSQL("select count(*) from account where id != 1");
                },
                {{std::to_string(rows_.size() - 1)}}
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }
    {
        auto s = testSelect(
                [](SelectRequestBuilder& b) {
                    b.SetSQL("select count(*) from account where id < 5");
                },
                {{"4"}}
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }
    {
        auto s = testSelect(
                [](SelectRequestBuilder& b) {
                    b.SetSQL("select count(*) from account where id > 5");
                },
                {{std::to_string(rows_.size() - 5)}}
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }
}

TEST_F(StoreTest, DISABLED_SelectAggreMore) {
    InsertSomeRows();

    // select aggre id
    {
        // max
        auto s = testSelect(
                [](SelectRequestBuilder &b) {
                    b.SetSQL("select max(id) from account");
                },
                {{"100"}}
        );
        ASSERT_TRUE(s.ok()) << s.ToString();

        // min
        s = testSelect(
                [](SelectRequestBuilder &b) {
                    b.SetSQL("select min(id) from account");
                },
                {{"1"}}
        );
        ASSERT_TRUE(s.ok()) << s.ToString();

        int sum = 0;
        for (int i = 1; i <= 100; ++i) {
            sum += i;
        }
        // sum
        s = testSelect(
                [](SelectRequestBuilder &b) {
                    b.SetSQL("select sum(id) from account");
                },
                {{std::to_string(sum)}}
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    // banlance
    {
        // max
        auto s = testSelect(
                [](SelectRequestBuilder &b) {
                    b.SetSQL("select max(balance) from account");
                },
                {{"200"}}
        );
        ASSERT_TRUE(s.ok()) << s.ToString();

        // min
        s = testSelect(
                [](SelectRequestBuilder &b) {
                    b.SetSQL("select min(balance) from account");
                },
                {{"101"}}
        );
        ASSERT_TRUE(s.ok()) << s.ToString();

        int sum = 0;
        for (int i = 1; i <= 100; ++i) {
            sum += i;
            sum += 100;
        }
        // sum
        s = testSelect(
                [](SelectRequestBuilder &b) {
                    b.SetSQL("select sum(balance) from account");
                },
                {{std::to_string(sum)}}
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }
}

TEST_F(StoreTest, DeleteBasic) {
    InsertSomeRows();

    // delete 1
    auto s = testDelete(
            [this](DeleteRequestBuilder& b) {
                b.AddRow(rows_[0]);
            },
            1
    );
    ASSERT_TRUE(s.ok()) << s.ToString();

    s = testSelect(
            [](SelectRequestBuilder& b) {
                b.SetSQL("select * from account");
            },
            {rows_.cbegin() + 1, rows_.cend()}
    );
    ASSERT_TRUE(s.ok()) << s.ToString();


    // delete all
    s = testDelete(
            [this](DeleteRequestBuilder& b) {
                b.AddRows(rows_);
            },
            rows_.size() - 4
    );
    ASSERT_TRUE(s.ok()) << s.ToString();

    s = testSelect(
            [](SelectRequestBuilder& b) {
                b.SetSQL("select * from account");
            },
            {});
    ASSERT_TRUE(s.ok()) << s.ToString();
}

} /* namespace  */
