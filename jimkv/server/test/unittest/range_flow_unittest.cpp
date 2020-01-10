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

#include "base/status.h"
#include "base/util.h"
#include "common/server_config.h"
#include "common/ds_encoding.h"
#include "base/fs_util.h"
#include "server/run_status.h"
#include "storage/store.h"
#include "storage/util.h"

#include "masstree-beta/config.h"
#include "masstree-beta/string.hh"

#include "helper/cpp_permission.h"
#include "range/range.h"
#include "server/range_tree.h"
#include "server/range_server.h"

#include "helper/table.h"
#include "helper/mock/raft_mock.h"
#include "helper/mock/rpc_request_mock.h"
#include "helper/helper_util.h"

int main(int argc, char *argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

using namespace jimkv::test::helper;
using namespace jimkv::test::mock;
using namespace jimkv::ds;
using namespace jimkv::ds::storage;

const static uint32_t default_range_id = 1;
std::unique_ptr<Table> ptr_table_account = CreateAccountTable();

class SelectFlowTest : public ::testing::Test {
protected:
    void SetUp() override {
        data_path_ = "/tmp/jimkv_ds_store_test_";
        data_path_ += std::to_string(NowMilliSeconds());

        ds_config.engine_type = EngineType::kMassTree;
        ds_config.masstree_config.data_path = data_path_ + "/mass-data";
        ds_config.meta_path = data_path_ + "/meta-data";

        ds_config.range_config.recover_concurrency = 1;

        range_server_ = new server::RangeServer;

        context_ = new server::ContextServer;

        context_->node_id = 1;
        context_->range_server = range_server_;
        context_->raft_server = new RaftServerMock;
        context_->run_status = new server::RunStatus;

        range_server_->Init(context_);
    }

    void TearDown() override {
        if (context_->range_server) {
            context_->range_server->Stop();
            delete context_->range_server;
        }
        delete context_->raft_server;
        delete context_->run_status;
        delete context_;
        if (!data_path_.empty()) {
            RemoveDirAll(data_path_.c_str());
        }
    }

    Status testSelectFlow(const dspb::RangeRequest& req, dspb::RangeResponse& resp) {
        auto rpc = NewMockRPCRequest(req, dspb::kFuncRangeRequest);
        range_server_->DealTask(std::move(rpc.first));
        return rpc.second->Get(resp);
    }

    Status testKv(const dspb::RangeRequest& req, dspb::RangeResponse& resp) {
        auto rpc = NewMockRPCRequest(req, dspb::kFuncRangeRequest);
        range_server_->DealTask(std::move(rpc.first));
        return rpc.second->Get(resp);
    }

protected:
    server::ContextServer *context_;
    std::string data_path_;
    server::RangeServer *range_server_;
};

/**
 * Test Info.
 *
 *  create table account (
 *      id bigint,
 *      name varchar(255),
 *      balance bigint,
 *      primary key (id)
 *  );
 *
 * =========================================
 * |  id  |       name        |   balance  |
 * =========================================
 * | 1000 |    name_1000      |     1000   |
 * | 1001 |    name_1001      |     1001   |
 * | 1002 |    name_1002      |     1002   |
 * | 1003 |    name_1003      |     1003   |
 * | 1004 |    name_1004      |     1004   |
 * | 1005 |    name_1005      |     1005   |
 * | 1006 |    name_1006      |     1006   |
 * | 1007 |    name_1007      |     1007   |
 * | 1008 |    name_1008      |     1008   |
 * | 1009 |    name_1009      |     1009   |
 * | 1010 |    name_1010      |     1010   |
 * | 1011 |    name_1011      |     1011   |
 * | 1012 |    name_1012      |     1012   |
 * | 1013 |    name_1013      |     1013   |
 * | 1014 |    name_1014      |     1014   |
 * | 1015 |    name_1015      |     1015   |
 * | 1016 |    name_1016      |     1016   |
 * | 1017 |    name_1017      |     1017   |
 * | 1018 |    name_1018      |     1018   |
 * | 1019 |    name_1019      |     1019   |
 * | 1020 |    name_1020      |     1020   |
 * =========================================
 *
 *
 * */

basepb::Range *genRange() {
    auto meta = new basepb::Range;

    meta->set_id(default_range_id);
    std::string start_key;
    std::string end_key;
    EncodeKeyPrefix(&start_key, default_account_table_id);
    EncodeKeyPrefix(&end_key, default_account_table_id+1);

    meta->set_start_key(start_key);
    meta->set_end_key(end_key);
    meta->mutable_range_epoch()->set_conf_ver(1);
    meta->mutable_range_epoch()->set_version(1);

    meta->set_table_id(default_account_table_id);

    auto peer = meta->add_peers();
    peer->set_id(1);
    peer->set_node_id(1);

    peer = meta->add_peers();
    peer->set_id(2);
    peer->set_node_id(2);

    auto pks = ptr_table_account->GetPKs();
    for (const auto& pk : pks) {
        auto p = meta->mutable_key_schema()->add_key_cols();

        p->set_id(pk.id());
        p->set_type(pk.sql_type().type());
        p->set_unsigned_(pk.sql_type().unsigned_());
    }

    return meta;
}
basepb::Range *genIndexKeyRange(int id) {
    auto meta = new basepb::Range;

    meta->set_id(default_range_id+1);
    std::string start_key;
    std::string end_key;
    EncodeKeyPrefix(&start_key, id);
    EncodeKeyPrefix(&end_key, id+1);

    meta->set_start_key(start_key);
    meta->set_end_key(end_key);
    meta->mutable_range_epoch()->set_conf_ver(1);
    meta->mutable_range_epoch()->set_version(1);

    meta->set_table_id(default_account_table_id);
    meta->set_index_id(id);

    auto peer = meta->add_peers();
    peer->set_id(1);
    peer->set_node_id(1);

    peer = meta->add_peers();
    peer->set_id(2);
    peer->set_node_id(2);

    // add key-schema key-cols
    auto ks = meta->mutable_key_schema();
    auto col_info_1 = ptr_table_account->GetColumn("name");
    auto ad_col1 = ks->add_key_cols();
    ad_col1->set_id(col_info_1.id());
    ad_col1->set_type(col_info_1.sql_type().type());
    ad_col1->set_unsigned_(col_info_1.sql_type().unsigned_());

    auto col_info_2 = ptr_table_account->GetColumn("balance");
    auto ad_col2 = ks->add_key_cols();
    ad_col2->set_id(col_info_2.id());
    ad_col2->set_type(col_info_2.sql_type().type());
    ad_col2->set_unsigned_(col_info_2.sql_type().unsigned_());

    ks->set_unique_index(true);

    // add key-schema extra-cols
    auto pks = ptr_table_account->GetPKs();
    for (const auto& pk : pks) {
        auto p = ks->add_extra_cols();
        p->set_id(pk.id());
        p->set_type(pk.sql_type().type());
        p->set_unsigned_(pk.sql_type().unsigned_());
    }

    return meta;
}

void genKvUniqueIndexKeyData(std::map<std::string, std::string > & mp)
{
    typedef int col_id_type;
    for (auto id = 0; id < 20; id++) {
        std::string key;
        std::string value;

        // make row data
        std::map<col_id_type, std::string> row;
        row.emplace(1, std::to_string(1000+id));
        row.emplace(2, "name_" + std::to_string(1000+id));
        row.emplace(3, std::to_string(1000+id));

        // kv_key
        EncodeKeyPrefix(&key, default_account_index_id1);
        auto i_col1 = ptr_table_account->GetColumn("name");
        EncodeKvKey(&key, i_col1, row.at(i_col1.id()));

        auto i_col2 = ptr_table_account->GetColumn("balance");
        EncodeKvKey(&key, i_col2, row.at(i_col2.id()));

        // kv_value
        std::string tmp_str;
        for ( auto & col : ptr_table_account->GetPKs()) {
            EncodeKvKey(&tmp_str, col, row.at(col.id()));
        }

        pb::InternalTag tag;
        tag.set_txn_id(std::string("index-0xoooiiiii-") + std::to_string(id));
        auto s = assembleValue(value, tmp_str, id, &tag);
        if (!s.ok()) {
            std::cerr << "assembleValue error. id:" << id << std::endl;
        }

        mp.emplace(key, value);
    }
}
TEST_F(SelectFlowTest, SelectFlowAndAgg) {
    std::string start_key;
    std::string end_key;
    std::string end_key_1;
    long long data_count = 20;

    { // create table
        EncodeKeyPrefix(&start_key, default_account_table_id);
        EncodeKeyPrefix(&end_key, default_account_table_id+1);
        EncodeKeyPrefix(&end_key_1, default_account_table_id+2);
    }


    { // begin create range
        dspb::SchRequest req;
        req.mutable_create_range()->set_allocated_range(genRange());

        // create range
        auto rpc = NewMockRPCRequest(req);
        range_server_->dispatchSchedule(rpc.first);
        ASSERT_FALSE(range_server_->ranges_.Empty());
        ASSERT_TRUE(range_server_->Find(default_range_id) != nullptr);

        // check meta
        std::vector<basepb::Range> metas;
        auto ret = range_server_->meta_store_->GetAllRange(&metas);
        ASSERT_TRUE(metas.size() == 1) << metas.size();

        // raft
        auto raft = static_cast<RaftMock *>(range_server_->ranges_.Find(1)->raft_.get());
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_.Find(1)->is_leader_ = true;

    }

    { // put data

        typedef int col_id_type;

        for (auto id = 0; id < data_count; id++) {
            std::string key;
            std::string value;

            // make row data
            std::map<col_id_type, std::string> row;
            row.emplace(1, std::to_string(1000+id));
            row.emplace(2, "name_" + std::to_string(1000+id));
            row.emplace(3, std::to_string(1000+(id%5)));

            // make encode key
            EncodeKeyPrefix(&key, default_account_table_id); // key:00000001 ("\x01"+"00000001")
            for ( auto & col : ptr_table_account->GetPKs()) {
                EncodeKvKey(&key, col,row.at(col.id()));   // key: key+ row_id(encoded)
            }

            // make encode value
            std::string tmp_str;
            for (auto & col : ptr_table_account->GetNonPkColumns()) {
                EncodeKvValue(&tmp_str, col, row.at(col.id()));
            }
            pb::InternalTag tag;
            tag.set_txn_id(std::string("table-0xoooiiiii-") + std::to_string(id));
            auto s1 = assembleValue(value, tmp_str, id, &tag);
            if (!s1.ok()) {
                std::cerr << "assembleValue error. id:" << id << std::endl;
            }

            // put data to db
            dspb::RangeRequest req;
            req.mutable_header()->set_range_id(default_range_id);
            req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
            req.mutable_header()->mutable_range_epoch()->set_version(1);
            req.mutable_kv_put()->set_key(key);
            req.mutable_kv_put()->set_value(value);

            dspb::RangeResponse resp;
            auto s = testKv(req, resp);
            ASSERT_TRUE(s.ok()) << s.ToString();
            ASSERT_FALSE(resp.header().has_error());
        }
    }

    { // test select and order by and limit
        // table_read;

        dspb::RangeRequest req;
        auto header = req.mutable_header();
        header->set_range_id(default_range_id);
        header->mutable_range_epoch()->set_conf_ver(1);
        header->mutable_range_epoch()->set_version(1);

        auto select_flow = req.mutable_select_flow();

        // table_read
        auto table_read_processor = select_flow->add_processors();
        table_read_processor->set_type(dspb::TABLE_READ_TYPE );
        auto table_read = table_read_processor->mutable_table_read();
        for ( const auto & col : ptr_table_account->GetAllColumns()) {
            table_read->add_columns()->CopyFrom(ptr_table_account->GetColumnInfo(col.id()));
        }

        table_read->set_type(dspb::KEYS_RANGE_TYPE);
        table_read->mutable_range()->set_start_key(start_key);
        table_read->mutable_range()->set_end_key(end_key);
        table_read->set_desc(false);

        // agg
        auto agg_processor = select_flow->add_processors();
        agg_processor->set_type(dspb::AGGREGATION_TYPE);
        auto agg = agg_processor->mutable_aggregation();

        dspb::Expr *func_tmp = nullptr;
        dspb::Expr *child = nullptr;
        std::string cl_name;
        basepb::ColumnInfo cl_table;
        //avg
        func_tmp = agg->add_func();
        func_tmp->set_expr_type(dspb::Avg);
        child = func_tmp->add_child();
        child->set_expr_type(dspb::Column);
        cl_name = "balance";
        cl_table = ptr_table_account->GetColumn(cl_name);
        child->mutable_column()->set_id(cl_table.id());

        //count(id)
        func_tmp = agg->add_func();
        func_tmp->set_expr_type(dspb::Count);
        child = func_tmp->add_child();
        cl_name = "id";
        cl_table = ptr_table_account->GetColumn(cl_name);
        child->set_expr_type(dspb::Column);
        child->mutable_column()->set_id(cl_table.id());

        //count(1)
         func_tmp = agg->add_func();
        func_tmp->set_expr_type(dspb::Count);
        // child = func_tmp->add_child();
        // child->set_expr_type(dspb::Const_Int);
        // child->set_value("1");
        
        //Max
        func_tmp = agg->add_func();
        func_tmp->set_expr_type(dspb::Max);
        child = func_tmp->add_child();
        child->set_expr_type(dspb::Column);
        cl_name = "id";
        cl_table = ptr_table_account->GetColumn(cl_name);
        child->mutable_column()->set_id(cl_table.id());

        //Min
        func_tmp = agg->add_func();
        func_tmp->set_expr_type(dspb::Min);
        child = func_tmp->add_child();
        child->set_expr_type(dspb::Column);
        cl_name = "id";
        cl_table = ptr_table_account->GetColumn(cl_name);
        child->mutable_column()->set_id(cl_table.id());

        //Sum
        func_tmp = agg->add_func();
        func_tmp->set_expr_type(dspb::Sum);
        child = func_tmp->add_child();
        child->set_expr_type(dspb::Column);
        cl_name = "id";
        cl_table = ptr_table_account->GetColumn(cl_name);
        child->mutable_column()->set_id(cl_table.id());

        // group by balance
        auto group_by_tmp = agg->add_group_by();
        group_by_tmp->set_expr_type(dspb::Column);
        cl_name = "balance";
        cl_table = ptr_table_account->GetColumn(cl_name);
        group_by_tmp->mutable_column()->set_id(cl_table.id());
        // group_by_tmp = agg->add_group_by();
        // group_by_tmp->set_expr_type(dspb::Column);
        // cl_name = "id";
        // cl_table = ptr_table_account->GetColumn(cl_name);
        // group_by_tmp->mutable_column()->set_id(cl_table.id());

        select_flow->set_gather_trace(false);

//        std::cerr << "selectFlowReq:\n" << req.DebugString() << std::endl;
        dspb::RangeResponse resp;
        auto s = testSelectFlow(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error());
        auto selectFlowResp = resp.select_flow();
//        std::cerr << "row size:" << selectFlowResp.rows().size() << std::endl;
//        std::cerr << "selectFlowResp:\n" << selectFlowResp.DebugString() << std::endl;
        std::vector<std::vector<int64_t>> res_end{
            {4012, 4, 4, 4, 1018, 1003, 4042, 1003},
            {4008, 4, 4, 4, 1017, 1002, 4038, 1002},
            {4004, 4, 4, 4, 1016, 1001, 4034, 1001},
            {4016, 4, 4, 4, 1019, 1004, 4046, 1004},
            {4000, 4, 4, 4, 1015, 1000, 4030, 1000}
        };
        // std::cout << "sum:" << nSum << " agv:" << nSum/data_count << std::endl;
        size_t offset = 0;
        int64_t value = 0, sum = 0;
        std::vector<std::vector<int64_t>>::const_iterator it = res_end.begin();
        for (auto &row : selectFlowResp.rows()) {
            offset = 0;
            value = 0;
            auto vt = *it++;
            // avg sum
            ASSERT_TRUE(DecodeNonSortingVarint(row.value().fields(), ++offset, &value));
            ASSERT_EQ(value, vt[0]);
            // avg count 
            ASSERT_TRUE(DecodeNonSortingVarint(row.value().fields(), ++offset, &value));
            ASSERT_EQ(value, vt[1]);

            // count(id)
            ASSERT_TRUE(DecodeNonSortingVarint(row.value().fields(), ++offset, &value));
            ASSERT_EQ(value, vt[2]);

            // count(1)
            ASSERT_TRUE(DecodeNonSortingVarint(row.value().fields(), ++offset, &value));
            ASSERT_EQ(value, vt[3]);

            // Max
            ASSERT_TRUE(DecodeNonSortingVarint(row.value().fields(), ++offset, &value));
            ASSERT_EQ(value, vt[4]);

            // Min
            ASSERT_TRUE(DecodeNonSortingVarint(row.value().fields(), ++offset, &value));
            ASSERT_EQ(value, vt[5]);

            // Sum
            ASSERT_TRUE(DecodeNonSortingVarint(row.value().fields(), ++offset, &value));
            ASSERT_EQ(value, vt[6]);
            ASSERT_TRUE(DecodeNonSortingVarint(row.value().fields(), ++offset, &value));
            ASSERT_EQ(value, vt[7]);
        }
    }

    { // test select and order by and limit
        // table_read;

        dspb::RangeRequest req;
        auto header = req.mutable_header();
        header->set_range_id(default_range_id);
        header->mutable_range_epoch()->set_conf_ver(1);
        header->mutable_range_epoch()->set_version(1);

        auto select_flow = req.mutable_select_flow();

        // table_read
        auto table_read_processor = select_flow->add_processors();
        table_read_processor->set_type(dspb::TABLE_READ_TYPE );
        auto table_read = table_read_processor->mutable_table_read();
        for ( const auto & col : ptr_table_account->GetAllColumns()) {
            table_read->add_columns()->CopyFrom(ptr_table_account->GetColumnInfo(col.id()));
        }

        table_read->set_type(dspb::KEYS_RANGE_TYPE);
        table_read->mutable_range()->set_start_key(start_key);
        table_read->mutable_range()->set_end_key(end_key);
        table_read->set_desc(false);

        // agg
        auto agg_processor = select_flow->add_processors();
        agg_processor->set_type(dspb::STREAM_AGGREGATION_TYPE);
        auto agg = agg_processor->mutable_stream_aggregation();

        dspb::Expr *func_tmp = nullptr;
        dspb::Expr *child = nullptr;
        std::string cl_name;
        basepb::ColumnInfo cl_table;
        //avg
        func_tmp = agg->add_func();
        func_tmp->set_expr_type(dspb::Avg);
        child = func_tmp->add_child();
        child->set_expr_type(dspb::Column);
        cl_name = "id";
        cl_table = ptr_table_account->GetColumn(cl_name);
        child->mutable_column()->set_id(cl_table.id());

        //count(id)
        func_tmp = agg->add_func();
        func_tmp->set_expr_type(dspb::Count);
        child = func_tmp->add_child();
        cl_name = "id";
        cl_table = ptr_table_account->GetColumn(cl_name);
        child->set_expr_type(dspb::Column);
        child->mutable_column()->set_id(cl_table.id());

        //count(1)
         func_tmp = agg->add_func();
        func_tmp->set_expr_type(dspb::Count);
        // child = func_tmp->add_child();
        // child->set_expr_type(dspb::Const_Int);
        // child->set_value("1");
        
        //Max
        func_tmp = agg->add_func();
        func_tmp->set_expr_type(dspb::Max);
        child = func_tmp->add_child();
        child->set_expr_type(dspb::Column);
        cl_name = "id";
        cl_table = ptr_table_account->GetColumn(cl_name);
        child->mutable_column()->set_id(cl_table.id());

        //Min
        func_tmp = agg->add_func();
        func_tmp->set_expr_type(dspb::Min);
        child = func_tmp->add_child();
        child->set_expr_type(dspb::Column);
        cl_name = "id";
        cl_table = ptr_table_account->GetColumn(cl_name);
        child->mutable_column()->set_id(cl_table.id());

        //Sum
        func_tmp = agg->add_func();
        func_tmp->set_expr_type(dspb::Sum);
        child = func_tmp->add_child();
        child->set_expr_type(dspb::Column);
        cl_name = "id";
        cl_table = ptr_table_account->GetColumn(cl_name);
        child->mutable_column()->set_id(cl_table.id());

        // group by balance
        auto group_by_tmp = agg->add_group_by();
        group_by_tmp->set_expr_type(dspb::Column);
        cl_name = "balance";
        cl_table = ptr_table_account->GetColumn(cl_name);
        group_by_tmp->mutable_column()->set_id(cl_table.id());
        // group_by_tmp = agg->add_group_by();
        // group_by_tmp->set_expr_type(dspb::Column);
        // cl_name = "id";
        // cl_table = ptr_table_account->GetColumn(cl_name);
        // group_by_tmp->mutable_column()->set_id(cl_table.id());

        select_flow->set_gather_trace(false);

        dspb::RangeResponse resp;
        auto s = testSelectFlow(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error());
        auto selectFlowResp = resp.select_flow();
        // std::cout << "row size:" << selectFlowResp.rows().size() << std::endl;
        // std::cout << "selectFlowResp:\n" << selectFlowResp.DebugString() << std::endl;
        std::vector<std::vector<int64_t>> res_end;
        for (int i = 0; i < data_count; ++i) {
            std::vector<int64_t> vt;
            //agv sum
            vt.push_back(1000 + i);
            //agv count
            vt.push_back(1);
            //count(id)
            vt.push_back(1);
            //count(1)
            vt.push_back(1);
            //max
            vt.push_back(1000 + i);
            //min
            vt.push_back(1000 + i);
            //sum
            vt.push_back(1000 + i);
            //balanc
            vt.push_back(1000 + i%5);
            res_end.push_back(vt);
        }
        size_t offset = 0;
        int64_t value = 0;
        
        std::vector<std::vector<int64_t>>::const_iterator it = res_end.begin();
        for (auto &row : selectFlowResp.rows()) {
            offset = 0;
            value = 0;
            auto vt = *it++;
            // avg sum
            ASSERT_TRUE(DecodeNonSortingVarint(row.value().fields(), ++offset, &value));
            ASSERT_EQ(value, vt[0]);
            // avg count 
            ASSERT_TRUE(DecodeNonSortingVarint(row.value().fields(), ++offset, &value));
            ASSERT_EQ(value, vt[1]);

            // count(id)
            ASSERT_TRUE(DecodeNonSortingVarint(row.value().fields(), ++offset, &value));
            ASSERT_EQ(value, vt[2]);

            // count(1)
            ASSERT_TRUE(DecodeNonSortingVarint(row.value().fields(), ++offset, &value));
            ASSERT_EQ(value, vt[3]);

            // Max
            ASSERT_TRUE(DecodeNonSortingVarint(row.value().fields(), ++offset, &value));
            ASSERT_EQ(value, vt[4]);

            // Min
            ASSERT_TRUE(DecodeNonSortingVarint(row.value().fields(), ++offset, &value));
            ASSERT_EQ(value, vt[5]);

            // Sum
            ASSERT_TRUE(DecodeNonSortingVarint(row.value().fields(), ++offset, &value));
            ASSERT_EQ(value, vt[6]);

            ASSERT_TRUE(DecodeNonSortingVarint(row.value().fields(), ++offset, &value));
            ASSERT_EQ(value, vt[7]);
        }
    }
}

TEST_F(SelectFlowTest, SelectFlowAndOrdering) {
    std::string start_key;
    std::string end_key;
    std::string end_key_1;

    { // create table
        EncodeKeyPrefix(&start_key, default_account_table_id);
        EncodeKeyPrefix(&end_key, default_account_table_id+1);
        EncodeKeyPrefix(&end_key_1, default_account_table_id+2);
    }


    { // begin create range
        dspb::SchRequest req;
        req.mutable_create_range()->set_allocated_range(genRange());

        // create range
        auto rpc = NewMockRPCRequest(req);
        range_server_->dispatchSchedule(rpc.first);
        ASSERT_FALSE(range_server_->ranges_.Empty());
        ASSERT_TRUE(range_server_->Find(default_range_id) != nullptr);

        // check meta
        std::vector<basepb::Range> metas;
        auto ret = range_server_->meta_store_->GetAllRange(&metas);
        ASSERT_TRUE(metas.size() == 1) << metas.size();

        // raft
        auto raft = static_cast<RaftMock *>(range_server_->ranges_.Find(1)->raft_.get());
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_.Find(1)->is_leader_ = true;

    }

    { // put data

        typedef int col_id_type;

        for (auto id = 0; id < 20; id++) {
            std::string key;
            std::string value;

            // make row data
            std::map<col_id_type, std::string> row;
            row.emplace(1, std::to_string(1000+id));
            row.emplace(2, "name_" + std::to_string(1000+id));
            row.emplace(3, std::to_string(1000+id));

            // make encode key
            EncodeKeyPrefix(&key, default_account_table_id);
            for ( auto & col : ptr_table_account->GetPKs()) {
                EncodeKvKey(&key, col,row.at(col.id()));
            }

            // make encode value
            std::string tmp_str;
            for (auto & col : ptr_table_account->GetNonPkColumns()) {
                EncodeKvValue(&tmp_str, col, row.at(col.id()));
            }

            pb::InternalTag tag;
            tag.set_txn_id(std::string("table-0xoooiiiii-") + std::to_string(id));
            auto s1 = assembleValue(value, tmp_str, id, &tag);
            if (!s1.ok()) {
                std::cerr << "assembleValue error. id:" << id << std::endl;
            }

            // put data to db
            dspb::RangeRequest req;
            req.mutable_header()->set_range_id(default_range_id);
            req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
            req.mutable_header()->mutable_range_epoch()->set_version(1);
            req.mutable_kv_put()->set_key(key);
            req.mutable_kv_put()->set_value(value);

            dspb::RangeResponse resp;
            auto s = testKv(req, resp);
            ASSERT_TRUE(s.ok()) << s.ToString();
            ASSERT_FALSE(resp.header().has_error());
        }
    }

    { // test select and order by
        // table_read;

        dspb::RangeRequest req;
        auto header = req.mutable_header();
        header->set_range_id(default_range_id);
        header->mutable_range_epoch()->set_conf_ver(1);
        header->mutable_range_epoch()->set_version(1);

        auto select_flow = req.mutable_select_flow();

        // table_read
        auto table_read_processor = select_flow->add_processors();
        table_read_processor->set_type(dspb::TABLE_READ_TYPE );
        auto table_read = table_read_processor->mutable_table_read();
        for ( const auto & col : ptr_table_account->GetAllColumns()) {
            table_read->add_columns()->CopyFrom(ptr_table_account->GetColumnInfo(col.id()));
        }

        table_read->set_type(dspb::KEYS_RANGE_TYPE);
        table_read->mutable_range()->set_start_key(start_key);
        table_read->mutable_range()->set_end_key(end_key);
        table_read->set_desc(false);

        // order by
        auto order_by_processor = select_flow->add_processors();
        order_by_processor->set_type(dspb::ORDER_BY_TYPE);
        auto ordering = order_by_processor->mutable_ordering();
        std::string cl_name = "id";
        auto cl_table = ptr_table_account->GetColumn(cl_name);
        // auto cl1 = ordering->add_columns();
        // cl1->set_asc(false);
        // cl1->mutable_expr()->set_expr_type(dspb::Column);
        // cl1->mutable_expr()->mutable_column()->set_id(cl_table.id());
        // cl_name = "name";
        // cl_table = ptr_table_account->GetColumn(cl_name);
        // auto cl2 = ordering->add_columns();
        // cl2->set_asc(false);
        // cl2->mutable_expr()->set_expr_type(dspb::Column);
        // cl2->mutable_expr()->mutable_column()->set_id(cl_table.id());
        cl_name = "balance";
        cl_table = ptr_table_account->GetColumn(cl_name);
        auto cl3 = ordering->add_columns();
        cl3->set_asc(false);
        cl3->mutable_expr()->set_expr_type(dspb::Column);
        cl3->mutable_expr()->mutable_column()->set_id(cl_table.id());
        int64_t count = 2;
        ordering->set_count(count);

        select_flow->add_output_offsets(0);
        select_flow->add_output_offsets(1);
        select_flow->add_output_offsets(2);

        select_flow->set_gather_trace(false);

        dspb::RangeResponse resp;
        auto s = testSelectFlow(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error());
        auto selectFlowResp = resp.select_flow();
        ASSERT_EQ(selectFlowResp.rows().size(), count);
        // std::cout << "row size:" << selectFlowResp.rows().size() << std::endl;
        // std::cout << "selectFlowResp:\n" << selectFlowResp.DebugString() << std::endl;
    }
}

TEST_F(SelectFlowTest, SelectFlowLimit) {

    std::string start_key;
    std::string end_key;
    std::string end_key_1;

    { // create table
        EncodeKeyPrefix(&start_key, default_account_table_id);
        EncodeKeyPrefix(&end_key, default_account_table_id+1);
        EncodeKeyPrefix(&end_key_1, default_account_table_id+2);
    }


    { // begin create range
        dspb::SchRequest req;
        req.mutable_create_range()->set_allocated_range(genRange());

        // create range
        auto rpc = NewMockRPCRequest(req);
        range_server_->dispatchSchedule(rpc.first);
        ASSERT_FALSE(range_server_->ranges_.Empty());
        ASSERT_TRUE(range_server_->Find(default_range_id) != nullptr);

        // check meta
        std::vector<basepb::Range> metas;
        auto ret = range_server_->meta_store_->GetAllRange(&metas);
        ASSERT_TRUE(metas.size() == 1) << metas.size();

        // raft
        auto raft = static_cast<RaftMock *>(range_server_->ranges_.Find(1)->raft_.get());
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_.Find(1)->is_leader_ = true;

    }

    { // put data

        typedef int col_id_type;

        for (auto id = 0; id < 20; id++) {
            std::string key;
            std::string value;

            // make row data
            std::map<col_id_type, std::string> row;
            row.emplace(1, std::to_string(1000+id));
            row.emplace(2, "name_" + std::to_string(1000+id));
            row.emplace(3, std::to_string(1000+id));

            // make encode key
            EncodeKeyPrefix(&key, default_account_table_id);
            for ( auto & col : ptr_table_account->GetPKs()) {
                EncodeKvKey(&key, col,row.at(col.id()));
            }

            // make encode value
            std::string tmp_str;
            for (auto & col : ptr_table_account->GetNonPkColumns()) {
                EncodeKvValue(&tmp_str, col, row.at(col.id()));
            }

            pb::InternalTag tag;
            tag.set_txn_id(std::string("table-0xoooiiiii-") + std::to_string(id));
            auto s1 = assembleValue(value, tmp_str, id, &tag);
            if (!s1.ok()) {
                std::cerr << "assembleValue error. id:" << id << std::endl;
            }

            // put data to db
            dspb::RangeRequest req;
            req.mutable_header()->set_range_id(default_range_id);
            req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
            req.mutable_header()->mutable_range_epoch()->set_version(1);
            req.mutable_kv_put()->set_key(key);
            req.mutable_kv_put()->set_value(value);

            dspb::RangeResponse resp;
            auto s = testKv(req, resp);
            ASSERT_TRUE(s.ok()) << s.ToString();
            ASSERT_FALSE(resp.header().has_error());
        }
    }

    { // test select and limit
        // table_read;

        dspb::RangeRequest req;
        auto header = req.mutable_header();
        header->set_range_id(default_range_id);
        header->mutable_range_epoch()->set_conf_ver(1);
        header->mutable_range_epoch()->set_version(1);

        auto select_flow = req.mutable_select_flow();

        // table_read
        auto table_read_processor = select_flow->add_processors();
        table_read_processor->set_type(dspb::TABLE_READ_TYPE );
        auto table_read = table_read_processor->mutable_table_read();
        for ( const auto & col : ptr_table_account->GetAllColumns()) {
            table_read->add_columns()->CopyFrom(ptr_table_account->GetColumnInfo(col.id()));
        }

        table_read->set_type(dspb::KEYS_RANGE_TYPE);
        table_read->mutable_range()->set_start_key(start_key);
        table_read->mutable_range()->set_end_key(end_key);
        table_read->set_desc(false);

        // limit
        auto limit_processor = select_flow->add_processors();
        limit_processor->set_type(dspb::LIMIT_TYPE);
        auto limit_read = limit_processor->mutable_limit();
        uint64_t count = 3;
        uint64_t offset = 2;
        limit_read->set_count(count);
        limit_read->set_offset(offset);

        select_flow->add_output_offsets(0);
        select_flow->add_output_offsets(1);
        select_flow->add_output_offsets(2);

        select_flow->set_gather_trace(false);

        dspb::RangeResponse resp;
        auto s = testSelectFlow(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error());
        auto selectFlowResp = resp.select_flow();
        ASSERT_EQ(selectFlowResp.rows().size(), int64_t(count));
        // std::cout << "row size:" << selectFlowResp.rows().size() << std::endl;
        // std::cout << "selectFlowResp:\n" << selectFlowResp.DebugString() << std::endl;
        
    }
}

TEST_F(SelectFlowTest, SelectFlow_index_read_unique) {

    std::string start_key;
    std::string end_key;

    std::map<std::string, std::string> mp_kv;

    { // create table
        EncodeKeyPrefix(&start_key, default_account_index_id1);
        EncodeKeyPrefix(&end_key, default_account_index_id1+1);
    }


    { // begin create range
        dspb::SchRequest req;
        req.mutable_create_range()->set_allocated_range(genIndexKeyRange(default_account_index_id1));

        // create range
        auto rpc = NewMockRPCRequest(req);
        range_server_->dispatchSchedule(rpc.first);
        ASSERT_FALSE(range_server_->ranges_.Empty());
        ASSERT_TRUE(range_server_->Find(default_range_id+1) != nullptr);

        // check meta
        std::vector<basepb::Range> metas;
        auto ret = range_server_->meta_store_->GetAllRange(&metas);
        ASSERT_TRUE(metas.size() == 1) << metas.size();

        // raft
        auto raft = static_cast<RaftMock *>(range_server_->ranges_.Find(default_range_id + 1)->raft_.get());
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_.Find(default_range_id + 1)->is_leader_ = true;

    }

    { // put data

        genKvUniqueIndexKeyData(mp_kv);

        for (auto kv : mp_kv) {
            // put data to db
            dspb::RangeRequest req;
            req.mutable_header()->set_range_id(default_range_id+1);
            req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
            req.mutable_header()->mutable_range_epoch()->set_version(1);
            req.mutable_kv_put()->set_key(kv.first);
            req.mutable_kv_put()->set_value(kv.second);

            dspb::RangeResponse resp;
            auto s = testKv(req, resp);
            ASSERT_TRUE(s.ok()) << s.ToString();
            ASSERT_FALSE(resp.header().has_error());
        }

    }

    {   // test SelectFlow index_read test1
        // read [start_key, end_key) data.

        dspb::RangeRequest req;
        auto header = req.mutable_header();
        header->set_range_id(default_range_id+1);
        header->mutable_range_epoch()->set_conf_ver(1);
        header->mutable_range_epoch()->set_version(1);

        auto select_flow = req.mutable_select_flow();
        auto index_read_processor = select_flow->add_processors();
        index_read_processor->set_type(dspb::INDEX_READ_TYPE);
        auto index_read = index_read_processor-> mutable_index_read();

        index_read->add_columns()->CopyFrom(ptr_table_account->GetColumnInfo("name"));
        index_read->add_columns()->CopyFrom(ptr_table_account->GetColumnInfo("balance"));
        index_read->add_columns()->CopyFrom(ptr_table_account->GetColumnInfo("id"));

        index_read->set_type(dspb::KEYS_RANGE_TYPE);
        index_read->mutable_range()->set_start_key(start_key);
        index_read->mutable_range()->set_end_key(end_key);
        index_read->set_desc(false);
        index_read->set_unique(true);

        // agg
        auto agg_processor = select_flow->add_processors();
        agg_processor->set_type(dspb::STREAM_AGGREGATION_TYPE);
        auto agg = agg_processor->mutable_stream_aggregation();

        dspb::Expr *func_tmp = nullptr;
        dspb::Expr *child = nullptr;
        std::string cl_name;
        basepb::ColumnInfo cl_table;
        //avg
        func_tmp = agg->add_func();
        func_tmp->set_expr_type(dspb::Avg);
        child = func_tmp->add_child();
        child->set_expr_type(dspb::Column);
        cl_name = "id";
        cl_table = ptr_table_account->GetColumn(cl_name);
        child->mutable_column()->set_id(cl_table.id());

        //count(id)
        func_tmp = agg->add_func();
        func_tmp->set_expr_type(dspb::Count);
        child = func_tmp->add_child();
        cl_name = "id";
        cl_table = ptr_table_account->GetColumn(cl_name);
        child->set_expr_type(dspb::Column);
        child->mutable_column()->set_id(cl_table.id());

        //count(1)
         func_tmp = agg->add_func();
        func_tmp->set_expr_type(dspb::Count);
        // child = func_tmp->add_child();
        // child->set_expr_type(dspb::Const_Int);
        // child->set_value("1");
        
        //Max
        func_tmp = agg->add_func();
        func_tmp->set_expr_type(dspb::Max);
        child = func_tmp->add_child();
        child->set_expr_type(dspb::Column);
        cl_name = "id";
        cl_table = ptr_table_account->GetColumn(cl_name);
        child->mutable_column()->set_id(cl_table.id());

        //Min
        func_tmp = agg->add_func();
        func_tmp->set_expr_type(dspb::Min);
        child = func_tmp->add_child();
        child->set_expr_type(dspb::Column);
        cl_name = "id";
        cl_table = ptr_table_account->GetColumn(cl_name);
        child->mutable_column()->set_id(cl_table.id());

        //Sum
        func_tmp = agg->add_func();
        func_tmp->set_expr_type(dspb::Sum);
        child = func_tmp->add_child();
        child->set_expr_type(dspb::Column);
        cl_name = "id";
        cl_table = ptr_table_account->GetColumn(cl_name);
        child->mutable_column()->set_id(cl_table.id());

        // group by balance
        auto group_by_tmp = agg->add_group_by();
        group_by_tmp->set_expr_type(dspb::Column);
        cl_name = "balance";
        cl_table = ptr_table_account->GetColumn(cl_name);
        group_by_tmp->mutable_column()->set_id(cl_table.id());
        // group_by_tmp = agg->add_group_by();
        // group_by_tmp->set_expr_type(dspb::Column);
        // cl_name = "id";
        // cl_table = ptr_table_account->GetColumn(cl_name);
        // group_by_tmp->mutable_column()->set_id(cl_table.id());

        select_flow->set_gather_trace(false);

        dspb::RangeResponse resp;
        auto s = testSelectFlow(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error());
        auto selectFlowResp = resp.select_flow();
        // std::cout << "row size:" << selectFlowResp.rows().size() << std::endl;
        // std::cout << "selectFlowResp:\n" << selectFlowResp.DebugString() << std::endl;
        std::vector<std::vector<int64_t>> res_end;
        for (int i = 0; i < 20; ++i) {
            std::vector<int64_t> vt;
            //agv sum
            vt.push_back(1000 + i);
            //agv count
            vt.push_back(1);
            //count(id)
            vt.push_back(1);
            //count(1)
            vt.push_back(1);
            //max
            vt.push_back(1000 + i);
            //min
            vt.push_back(1000 + i);
            //sum
            vt.push_back(1000 + i);
            //balanc
            vt.push_back(1000 + i);
            res_end.push_back(vt);
        }
        size_t offset = 0;
        int64_t value = 0;
        
        std::vector<std::vector<int64_t>>::const_iterator it = res_end.begin();
        for (auto &row : selectFlowResp.rows()) {
            offset = 0;
            value = 0;
            auto vt = *it++;
            // avg sum
            ASSERT_TRUE(DecodeNonSortingVarint(row.value().fields(), ++offset, &value));
            ASSERT_EQ(value, vt[0]);
            // avg count 
            ASSERT_TRUE(DecodeNonSortingVarint(row.value().fields(), ++offset, &value));
            ASSERT_EQ(value, vt[1]);

            // count(id)
            ASSERT_TRUE(DecodeNonSortingVarint(row.value().fields(), ++offset, &value));
            ASSERT_EQ(value, vt[2]);

            // count(1)
            ASSERT_TRUE(DecodeNonSortingVarint(row.value().fields(), ++offset, &value));
            ASSERT_EQ(value, vt[3]);

            // Max
            ASSERT_TRUE(DecodeNonSortingVarint(row.value().fields(), ++offset, &value));
            ASSERT_EQ(value, vt[4]);

            // Min
            ASSERT_TRUE(DecodeNonSortingVarint(row.value().fields(), ++offset, &value));
            ASSERT_EQ(value, vt[5]);

            // Sum
            ASSERT_TRUE(DecodeNonSortingVarint(row.value().fields(), ++offset, &value));
            ASSERT_EQ(value, vt[6]);

            // balance
            ASSERT_TRUE(DecodeNonSortingVarint(row.value().fields(), ++offset, &value));
            ASSERT_EQ(value, vt[7]);
        }
    }
}
