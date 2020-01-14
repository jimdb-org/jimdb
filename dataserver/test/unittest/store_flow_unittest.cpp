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
#include "helper/store_test_fixture.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {

using namespace jim;
using namespace jim::test::helper;
using namespace jim::ds;

// test base the account table
class StoreTest : public StoreTestFixture {
public:
    StoreTest() : StoreTestFixture(CreateAccountTable()) {}

    void InitRows(uint64_t *total_bytes = nullptr) {
        for (int i = 1; i <= 50; ++i) {
            std::vector<std::string> row;
            row.push_back(std::to_string(i));

            char name[32] = {'\0'};
            snprintf(name, 32, "user-%04d", i%5);
            row.emplace_back(name);

            row.push_back(std::to_string(i%7));
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
    // select * from account
    {
        auto s = testSelectFlow(
                [](SelectFlowRequestBuilder& b) {
                    b.SetSQL("select * from account");
                },
                rows_
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    // select * from account where id = 1
    {
        rows_tmp.clear();
        for (auto & rw: rows_) {
            if ( std::atoi(rw[0].c_str()) == 1) {
                rows_tmp.push_back(rw);
            }
        }

        auto s = testSelectFlow(
                [](SelectFlowRequestBuilder& b) {
                    b.SetSQL("select * from account where id = 1");
                },
                rows_tmp
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    // select * from account where id <= 1
    {
        rows_tmp.clear();
        for (auto & rw: rows_) {
            if ( std::atoi(rw[0].c_str()) <= 1) {
                rows_tmp.push_back(rw);
            }
        }
        auto s = testSelectFlow(
                [](SelectFlowRequestBuilder& b) {
                    b.SetSQL("select * from account where id <= 1");
                },
                rows_tmp
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    // select * from account where id < 2
    {
        rows_tmp.clear();
        for (auto & rw: rows_) {
            if (std::atoi(rw[0].c_str()) < 2) {
                rows_tmp.push_back(rw);
            }
        }
        auto s = testSelectFlow(
                [](SelectFlowRequestBuilder& b) {
                    b.SetSQL("select * from account where id < 2");
                },
                rows_tmp
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    // select * from account where id > 9
    {
        rows_tmp.clear();

        for (auto & rw: rows_) {
            if (std::atoi(rw[0].c_str()) > 9) {
                rows_tmp.push_back(rw);
            }
        }

        auto s = testSelectFlow(
                [](SelectFlowRequestBuilder& b) {
                    b.SetSQL("select * from account where id > 9");
                },
                rows_tmp
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    // select * from account where id >= 10
    {
        rows_tmp.clear();
        for (auto & rw: rows_) {
            if (std::atoi(rw[0].c_str()) >= 10) {
                rows_tmp.push_back(rw);
            }
        }
        auto s = testSelectFlow(
                [](SelectFlowRequestBuilder& b) {
                    b.SetSQL("select * from account where id >= 10");
                },
                rows_tmp
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    // select count(*) from account group by balance
    // {
    //     rows_tmp.clear();
    //     for (int i = 0; i < 7; ++i) {
    //         rows_tmp.push_back({std::to_string(i)});
    //     }
    //     auto s = testSelectFlow(
    //             [](SelectFlowRequestBuilder& b) {
    //                 b.SetSQL("select count(*) from account group by balance");
    //             },
    //             rows_tmp
    //     );
    //     ASSERT_TRUE(s.ok()) << s.ToString();
    // }

    // select avg(id) from account
    {
        int sum = 0;
        for (int i = 1; i <= 50; ++i) {
            sum += i;
        }
        rows_tmp.clear();
        rows_tmp.push_back({std::to_string(sum/50)});
        auto s = testSelectFlow(
                [](SelectFlowRequestBuilder& b) {
                    b.SetSQL("select avg(id) from account");
                },
                rows_tmp
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    // select count(*) from account
    {
        rows_tmp.clear();
        rows_tmp.push_back({"50"});
        auto s = testSelectFlow(
                [](SelectFlowRequestBuilder& b) {
                    b.SetSQL("select count(*) from account");
                },
                rows_tmp
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    // select max(id) from account
    {
        rows_tmp.clear();
        rows_tmp.push_back({"50"});
        auto s = testSelectFlow(
                [](SelectFlowRequestBuilder& b) {
                    b.SetSQL("select max(id) from account");
                },
                rows_tmp
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    // select min(id) from account
    {
        rows_tmp.clear();
        rows_tmp.push_back({"1"});
        auto s = testSelectFlow(
                [](SelectFlowRequestBuilder& b) {
                    b.SetSQL("select min(id) from account");
                },
                rows_tmp
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    // select sum(id) from account
    {
        int sum = 0;
        for (int i = 1; i <= 50; ++i) {
            sum += i;
        }
        rows_tmp.clear();
        rows_tmp.push_back({std::to_string(sum)});
        auto s = testSelectFlow(
                [](SelectFlowRequestBuilder& b) {
                    b.SetSQL("select sum(id) from account");
                },
                rows_tmp
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    // select * from account order by id desc
    {
        rows_tmp.clear();
        for (auto it = rows_.rbegin(); it != rows_.rend(); ++it) {
            rows_tmp.push_back(*it);
        }
        auto s = testSelectFlow(
                [](SelectFlowRequestBuilder& b) {
                    b.SetSQL("select * from account order by id desc");
                },
                rows_tmp
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    // select * from account limit 3
    {
        rows_tmp.clear();
        rows_tmp.push_back(rows_[0]);
        rows_tmp.push_back(rows_[1]);
        rows_tmp.push_back(rows_[2]);
        auto s = testSelectFlow(
                [](SelectFlowRequestBuilder& b) {
                    b.SetSQL("select * from account limit 3");
                },
                rows_tmp
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    // select * from account limit 1,3
    // sql-parser not support
    {
        rows_tmp.clear();
        rows_tmp.push_back(rows_[1]);
        rows_tmp.push_back(rows_[2]);
        auto s = testSelectFlow(
                [](SelectFlowRequestBuilder& b) {
                    b.SetSQL("select * from account limit 2 offset 1");
                },
                rows_tmp
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    // select * from account where id > 1 order by id desc limit 1
    {
        rows_tmp.clear();
        rows_tmp.push_back(rows_[49]);
        auto s = testSelectFlow(
                [](SelectFlowRequestBuilder& b) {
                    b.SetSQL("select * from account where id > 1 order by id desc limit 1");
                },
                rows_tmp
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }
    {
        rows_tmp.clear();
        for (auto & rw: rows_) {
            if ( std::atoi(rw[0].c_str())%7 == 0) {
                rows_tmp.push_back(rw);
            }
        }

        for (auto & rw: rows_) {
            if ( std::atoi(rw[0].c_str())%7 == 1) {
                rows_tmp.push_back(rw);
            }
        }

        for (auto & rw: rows_) {
            if ( std::atoi(rw[0].c_str())%7 == 2) {
                rows_tmp.push_back(rw);
            }
        }

       auto s = testSelectFlow(
                [](SelectFlowRequestBuilder& b) {
                    b.SetSQL("select * from account where balance < 3 order by balance ");
                },
                rows_tmp
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    {
        std::vector<std::vector<std::string>> rows_tmp_t;
        rows_tmp.clear();
        for (auto & rw: rows_) {
            if ( std::atoi(rw[0].c_str())%7 == 0) {
                rows_tmp_t.push_back(rw);
            }
        }
        for (auto & rw: rows_tmp_t) {
            if ( rw[1] == "user-0000") {
                rows_tmp.push_back(rw);
            }
        }
        for (auto & rw: rows_tmp_t) {
            if ( rw[1] == "user-0001") {
                rows_tmp.push_back(rw);
            }
        }

        rows_tmp_t.clear();

        for (auto & rw: rows_) {
            if ( std::atoi(rw[0].c_str())%7 == 1) {
                rows_tmp_t.push_back(rw);
            }
        }
        for (auto & rw: rows_tmp_t) {
            if ( rw[1] == "user-0000") {
                rows_tmp.push_back(rw);
            }
        }
        for (auto & rw: rows_tmp_t) {
            if ( rw[1] == "user-0001") {
                rows_tmp.push_back(rw);
            }
        }

        auto s = testSelectFlow(
                [](SelectFlowRequestBuilder& b) {
                    b.SetSQL("select * from account where balance < 2 and name < 'user-0002' ORDER BY balance, name ");
                },
                rows_tmp
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    {
        rows_tmp.clear();
        for (auto & rw: rows_) {
            if ( std::atoi(rw[0].c_str())%7 == 2) {
                rows_tmp.push_back(rw);
            }
        }

        for (auto & rw: rows_) {
            if ( std::atoi(rw[0].c_str())%7 == 1) {
                rows_tmp.push_back(rw);
            }
        }

        for (auto & rw: rows_) {
            if ( std::atoi(rw[0].c_str())%7 == 0) {
                rows_tmp.push_back(rw);
            }
        }

       auto s = testSelectFlow(
                [](SelectFlowRequestBuilder& b) {
                    b.SetSQL("select * from account where balance < 3 order by balance desc ");
                },
                rows_tmp
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    {
        std::vector<std::vector<std::string>> rows_tmp_t;
        rows_tmp.clear();
        for (auto & rw: rows_) {
            if ( std::atoi(rw[0].c_str())%7 == 0) {
                rows_tmp_t.push_back(rw);
            }
        }
        for (auto & rw: rows_tmp_t) {
            if ( rw[1] == "user-0001") {
                rows_tmp.push_back(rw);
            }
        }

        for (auto & rw: rows_tmp_t) {
            if ( rw[1] == "user-0000") {
                rows_tmp.push_back(rw);
            }
        }

        rows_tmp_t.clear();

        for (auto & rw: rows_) {
            if ( std::atoi(rw[0].c_str())%7 == 1) {
                rows_tmp_t.push_back(rw);
            }
        }

        for (auto & rw: rows_tmp_t) {
            if ( rw[1] == "user-0001") {
                rows_tmp.push_back(rw);
            }
        }

        for (auto & rw: rows_tmp_t) {
            if ( rw[1] == "user-0000") {
                rows_tmp.push_back(rw);
            }
        }

        auto s = testSelectFlow(
                [](SelectFlowRequestBuilder& b) {
                    b.SetSQL("select * from account where balance < 2 and name < 'user-0002' ORDER BY balance, name DESC ");
                },
                rows_tmp
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }


    // select * from account where id = 10 / 2
    {
        rows_tmp.clear();
        rows_tmp.push_back(rows_[4]);
        auto s = testSelectFlow(
                [](SelectFlowRequestBuilder& b) {
                    b.SetSQL("select * from account where id = 10 / 2");
                },
                rows_tmp
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    // select * from account where id = 5 - 2
    {
        rows_tmp.clear();
        rows_tmp.push_back(rows_[2]);
        auto s = testSelectFlow(
                [](SelectFlowRequestBuilder& b) {
                    b.SetSQL("select * from account where id = 5 - 2");
                },
                rows_tmp
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    // select * from account where id = 2 * 3
    {
        rows_tmp.clear();
        rows_tmp.push_back(rows_[5]);
        auto s = testSelectFlow(
                [](SelectFlowRequestBuilder& b) {
                    b.SetSQL("select * from account where id = 2 * 3");
                },
                rows_tmp
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    // select * from account where id is not null
    {
        // auto s = testSelectFlow(
        //         [](SelectFlowRequestBuilder& b) {
        //             b.SetSQL("select * from account where id is not null");
        //         },
        //         rows_
        // );
        // ASSERT_TRUE(s.ok()) << s.ToString();
    }

    // select * from account where id = 5 % 3
    // sql-parse not support mod
    {
        // rows_tmp.clear();
        // rows_tmp.push_back(rows_[1]);
        // auto s = testSelectFlow(
        //         [](SelectFlowRequestBuilder& b) {
        //             b.SetSQL("select * from account where id = 5 % 3");
        //         },
        //         rows_tmp
        // );
        // ASSERT_TRUE(s.ok()) << s.ToString();
    }
}

} /* namespace  */
