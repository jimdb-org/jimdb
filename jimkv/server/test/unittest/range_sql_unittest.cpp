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

#include "helper/range_test_fixture.h"
#include "helper/helper_util.h"
#include "helper/request_builder.h"
#include "helper/query_parser.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {

using namespace dspb;
using namespace jimkv::test::helper;
using namespace jimkv::ds;
using namespace jimkv::ds::storage;

TEST_F(RangeTestFixture, NoLeader) {
    {
        dspb::RangeRequest req;
        MakeHeader(req.mutable_header());
        dspb::RangeResponse resp;
        Process(req, &resp);
        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_not_leader());
        ASSERT_EQ(resp.header().error().not_leader().range_id(), GetRangeID());
        ASSERT_FALSE(resp.header().error().not_leader().has_leader());
    }
}

TEST_F(RangeTestFixture, NotLeader) {
    SetLeader(2);
    {
        dspb::RangeRequest req;
        MakeHeader(req.mutable_header());
        dspb::RangeResponse resp;
        Process(req, &resp);
        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_not_leader());
        ASSERT_EQ(resp.header().error().not_leader().range_id(), GetRangeID());
        ASSERT_TRUE(resp.header().error().not_leader().has_leader());
        ASSERT_EQ(resp.header().error().not_leader().leader().node_id(), 2UL);
        ASSERT_EQ(resp.header().error().not_leader().leader().id(), GetPeerID(2));
    }
}

TEST_F(RangeTestFixture, StaleEpoch) {
    SetLeader(GetNodeID());
    auto old_ver = range_->GetMeta().range_epoch().version();
    auto s = Split();
    ASSERT_TRUE(s.ok()) << s.ToString();
    auto old_meta = range_->GetMeta();
    auto split_range = context_->FindRange(GetSplitRangeID());
    ASSERT_TRUE(split_range != nullptr);
    auto new_meta = split_range->GetMeta();
    {
        dspb::RangeRequest req;
        MakeHeader(req.mutable_header(), old_ver);
        dspb::RangeResponse resp;
        Process(req, &resp);
        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_stale_epoch());
        auto &stale_err = resp.header().error().stale_epoch();

        ASSERT_EQ(stale_err.old_range().id(), old_meta.id());
        ASSERT_EQ(stale_err.old_range().start_key(), old_meta.start_key());
        ASSERT_EQ(stale_err.old_range().end_key(), old_meta.end_key());
        ASSERT_EQ(stale_err.old_range().table_id(), old_meta.table_id());
        ASSERT_EQ(stale_err.old_range().range_epoch().ShortDebugString(), old_meta.range_epoch().ShortDebugString());

        ASSERT_EQ(stale_err.new_range().id(), new_meta.id());
        ASSERT_EQ(stale_err.new_range().start_key(), new_meta.start_key());
        ASSERT_EQ(stale_err.new_range().end_key(), new_meta.end_key());
        ASSERT_EQ(stale_err.new_range().table_id(), new_meta.table_id());
        ASSERT_EQ(stale_err.new_range().range_epoch().ShortDebugString(), new_meta.range_epoch().ShortDebugString());
    }
}

TEST_F(RangeTestFixture, CURD) {
    SetLeader(GetNodeID());

    std::vector<std::vector<std::string>> rows = {
            {"1", "user1", "111"},
            {"2", "user2", "222"},
            {"3", "user3", "333"},
    };

    // insert some rows
    {
        dspb::RangeRequest req;
        MakeHeader(req.mutable_header());
        InsertRequestBuilder builder(table_.get());
        builder.AddRows(rows);
        req.mutable_prepare()->CopyFrom(builder.Build());
        dspb::RangeResponse resp;
        Process(req, &resp);
        ASSERT_FALSE(resp.header().has_error()) << resp.header().error().ShortDebugString();
        ASSERT_EQ(resp.prepare().errors_size(), 0) << resp.ShortDebugString();
    }
    // test select
    {
        dspb::RangeRequest req;
        MakeHeader(req.mutable_header());
        SelectRequestBuilder builder(table_.get());
        builder.SetSQL("select * from account");
        *req.mutable_select() = builder.Build();

        dspb::RangeResponse resp;
        Process(req, &resp);
        ASSERT_FALSE(resp.header().has_error()) << resp.header().error().ShortDebugString();
        SelectResultParser parser(req.select(), resp.select());
        auto s = parser.Match(rows);
        ASSERT_TRUE(s.ok()) << s.ToString();
    }
    // test delete one
    {
        dspb::RangeRequest req;
        MakeHeader(req.mutable_header());
        DeleteRequestBuilder builder(table_.get());
        builder.AddRow(rows[0]);
        *req.mutable_prepare() = builder.Build();
        RangeResponse resp;
        Process(req, &resp);
        ASSERT_FALSE(resp.header().has_error()) << resp.header().error().ShortDebugString();
        ASSERT_EQ(resp.prepare().errors_size(), 0) << resp.ShortDebugString();
    }
    // select and check
    {
        RangeRequest req;
        MakeHeader(req.mutable_header());
        SelectRequestBuilder builder(table_.get());
        builder.SetSQL("select * from account");
        *req.mutable_select() = builder.Build();
        RangeResponse resp;
        Process(req, &resp);
        ASSERT_FALSE(resp.header().has_error()) << resp.header().error().ShortDebugString();
        SelectResultParser parser(req.select(), resp.select());
        auto s = parser.Match({rows[1], rows[2]});
        ASSERT_TRUE(s.ok()) << s.ToString();
    }
    // delete all
    {
        RangeRequest req;
        MakeHeader(req.mutable_header());
        DeleteRequestBuilder builder(table_.get());
        builder.AddRows(rows);
        *req.mutable_prepare() = builder.Build();
        RangeResponse resp;
        Process(req, &resp);
        ASSERT_FALSE(resp.header().has_error()) << resp.header().error().ShortDebugString();
        ASSERT_EQ(resp.prepare().errors_size(), 0) << resp.ShortDebugString();
    }
    // select and check
    {
        RangeRequest req;
        MakeHeader(req.mutable_header());
        SelectRequestBuilder builder(table_.get());
        builder.SetSQL("select * from account");
        *req.mutable_select() = builder.Build();
        RangeResponse resp;
        Process(req, &resp);
        ASSERT_FALSE(resp.header().has_error()) << resp.header().error().ShortDebugString();
        SelectResultParser parser(req.select(), resp.select());
        auto s = parser.Match({});
        ASSERT_TRUE(s.ok()) << s.ToString();
    }
}

TEST_F(RangeTestFixture, WhereExpr) {
    SetLeader(GetNodeID());

    std::vector<std::vector<std::string>> rows = {
            {"1", "user1", "111"},
            {"2", "user2", "222"},
            {"3", "user3", "333"},
    };
    std::vector<std::vector<std::string>> rows1 = {
            {"1", "user1", "111"},
    };
    std::vector<std::vector<std::string>> rows2 = {
            {"2", "user2", "222"},
    };
    std::vector<std::vector<std::string>> rows3 = {
            {"3", "user3", "333"},
    };
    std::vector<std::vector<std::string>> rows12 = {
            {"1", "user1", "111"},
            {"2", "user2", "222"},
    };
    std::vector<std::vector<std::string>> rows13 = {
            {"1", "user1", "111"},
            {"3", "user3", "333"},
    };
    std::vector<std::vector<std::string>> rows23 = {
            {"2", "user2", "222"},
            {"3", "user3", "333"},
    };
    std::vector<std::vector<std::string>> rows0;
    rows0.clear();

    // insert some rows
    {
        RangeRequest req;
        MakeHeader(req.mutable_header());
        InsertRequestBuilder builder(table_.get());
        builder.AddRows(rows);
        req.mutable_prepare()->CopyFrom(builder.Build());
        RangeResponse resp;
        Process(req, &resp);
        ASSERT_FALSE(resp.header().has_error()) << resp.header().error().ShortDebugString();
        ASSERT_EQ(resp.prepare().errors_size(), 0) << resp.ShortDebugString();
    }

    // test select
    {
        RangeRequest req;
        MakeHeader(req.mutable_header());
        SelectRequestBuilder builder(table_.get());

        //id = 1
        builder.SetSQL("select * from account where id = 1");
        *req.mutable_select() = builder.Build();

        RangeResponse resp;
        Process(req, &resp);
        ASSERT_FALSE(resp.header().has_error()) << resp.header().error().ShortDebugString();
        SelectResultParser parser(req.select(), resp.select());
        auto s = parser.Match(rows1);
        ASSERT_TRUE(s.ok()) << s.ToString();

        //id = 1 or id = 2
        builder.SetSQL("select * from account where id = 1 or id = 2");
        *req.mutable_select() = builder.Build();
        Process(req, &resp);
        SelectResultParser parser12(req.select(), resp.select());
        s = parser12.Match(rows12);
        ASSERT_TRUE(s.ok()) << s.ToString();

        //id = 2 and balance = "222" and name = "user2"
        builder.SetSQL(R"(select * from account where id =2 and balance=222 and name='user2')");
        *req.mutable_select() = builder.Build();
        Process(req, &resp);
        SelectResultParser parser2(req.select(), resp.select());
        s = parser2.Match(rows2);
        ASSERT_TRUE(s.ok()) << s.ToString();

        //name = user33 or name = "user3" => rows3
        builder.SetSQL(R"(select * from account where name='user33' or name='user3')");
        *req.mutable_select() = builder.Build();
        Process(req, &resp);
        SelectResultParser parser3(req.select(), resp.select());
        s = parser3.Match(rows3);
        ASSERT_TRUE(s.ok()) << s.ToString();

        //id <> 1 or id > 1 and (id < 4 and id > 1) => rows23
        builder.SetSQL("select * from account where id != 1 or id > 1 and id < 4 and id > 1");
        *req.mutable_select() = builder.Build();
        Process(req, &resp);
        SelectResultParser parser33(req.select(), resp.select());
        s = parser33.Match(rows23);
        ASSERT_TRUE(s.ok()) << s.ToString();

        //id = 2 or name = "user2" and balance = "100" => rows0
        builder.SetSQL(R"(select * from account where id=2 or name='user2' and balance=100)");
        *req.mutable_select() = builder.Build();
        Process(req, &resp);
        SelectResultParser parser1_1(req.select(), resp.select());
        s = parser1_1.Match(rows2);
        ASSERT_TRUE(s.ok()) << s.ToString();
    }
}

TEST_F(RangeTestFixture, MatchMathExpr) {
    SetLeader(GetNodeID());

    std::vector<std::vector<std::string>> rows = {
            {"1", "user1", "111"},
            {"2", "user2", "222"},
            {"3", "user3", "333"},
    };
    std::vector<std::vector<std::string>> rows0;
    std::vector<std::vector<std::string>> rows2 = {
            {"2", "user2", "222"},
    };

    std::vector<std::vector<std::string>> rows1 = {
            {"1", "user1", "111"},
    };

    // insert some rows
    {
        RangeRequest req;
        MakeHeader(req.mutable_header());
        InsertRequestBuilder builder(table_.get());
        builder.AddRows(rows);
        req.mutable_prepare()->CopyFrom(builder.Build());
        RangeResponse resp;
        Process(req, &resp);
        ASSERT_FALSE(resp.header().has_error()) << resp.header().error().ShortDebugString();
        ASSERT_EQ(resp.prepare().errors_size(), 0);
    }
    // test select
    {

        RangeRequest req;
        MakeHeader(req.mutable_header());
        SelectRequestBuilder builder(table_.get());

        //id = 1 + 1
        builder.SetSQL("select *  from account where id = 1+1");
        *req.mutable_select() = builder.Build();

        RangeResponse resp;
        Process(req, &resp);
        ASSERT_FALSE(resp.header().has_error()) << resp.header().error().ShortDebugString();
        SelectResultParser parser(req.select(), resp.select());
        auto s = parser.Match(rows2);
        ASSERT_TRUE(s.ok()) << s.ToString();

        //where id = 10 - 9
        builder.SetSQL("select * from account where id = -9 - -10");
        *req.mutable_select() = builder.Build();
        Process(req, &resp);
        SelectResultParser parser1_1(req.select(), resp.select());
        s = parser1_1.Match(rows1);
        ASSERT_TRUE(s.ok()) << s.ToString();

        builder.SetSQL("select * from account where id=1*1");
        *req.mutable_select() = builder.Build();
        Process(req, &resp);
        SelectResultParser parser1(req.select(), resp.select());
        s = parser1.Match(rows1);
        ASSERT_TRUE(s.ok()) << s.ToString();

        builder.SetSQL("select * from account where id = 10/10");
        *req.mutable_select() = builder.Build();
        Process(req, &resp);
        SelectResultParser parser2(req.select(), resp.select());
        s = parser2.Match(rows1);
        ASSERT_TRUE(s.ok()) << s.ToString();

        builder.SetSQL("select * from account where id = id +2 or id = id - -1");
        *req.mutable_select() = builder.Build();
        Process(req, &resp);
        SelectResultParser parser3(req.select(), resp.select());
        s = parser3.Match(rows0);
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

}

}
