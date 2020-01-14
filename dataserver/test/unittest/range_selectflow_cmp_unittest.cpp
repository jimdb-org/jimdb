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

#include "base/status.h"
#include "base/util.h"
#include "common/server_config.h"
#include "base/fs_util.h"
#include "server/run_status.h"
#include "storage/store.h"
#include "storage/util.h"

#include "masstree-beta/config.h"
#include "masstree-beta/string.hh"

#include "helper/table.h"
#include "helper/mock/raft_mock.h"
#include "helper/mock/rpc_request_mock.h"
#include "helper/helper_util.h"

#include "helper/cpp_permission.h"
#include "range/range.h"
#include "server/range_tree.h"
#include "server/range_server.h"

#include <string>
#include <map>

int main(int argc, char *argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

using namespace jim::test::helper;
using namespace jim::test::mock;
using namespace jim::ds;
using namespace jim::ds::storage;

const static uint32_t default_range_id = 1;
std::unique_ptr<Table> ptr_table_account = CreateAccountTable();

class SelectFlowTest : public ::testing::Test {
protected:
    void SetUp() override {
        data_path_ = "/tmp/jim_ds_store_test_";
        data_path_ += std::to_string(NowMilliSeconds());

        ds_config.engine_type = EngineType::kMassTree;
        ds_config.masstree_config.data_path = data_path_ + "/mass-data";
        ds_config.meta_path = data_path_ + "/meta-data";

        ds_config.range_config.recover_concurrency = 1;
        ds_config.logger_config.level = "debug" ;

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
 * id is unique index
 * balance is non unique index
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

void genKvPrimaryKeyData(std::map<std::string, std::string > & mp)
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
        auto s = assembleValue(value, tmp_str, id, &tag);
        if (!s.ok()) {
            std::cerr << "assembleValue error. id:" << id << std::endl;
        }

        mp.emplace(key, value);
    }
}

bool check_data(std::string & id, std::string & name, std::string & balance)
{
    bool flg = false;
    // id, name, balance
    // x, name_x, x
    if ( id == balance && "name_" + id == name ) {

        flg = true;
    } else {
        flg = false;
        std::cerr << "id:" << id << "\tname:" << name << "\tbalance:" << balance << std::endl;
    }

    return flg;

}

bool check_row_value (const dspb::RowValue & v)
{
    size_t offset = 0;

    std::string ret_id;
    std::string ret_name;
    std::string ret_balance;

    std::string buf = v.fields();
    // return data sort is id,name,balance

    DecodeValueColumn(buf, offset, ptr_table_account->GetColumnInfo("id"), &ret_id);
    DecodeValueColumn(buf, offset, ptr_table_account->GetColumnInfo("name"), &ret_name);
    DecodeValueColumn(buf, offset, ptr_table_account->GetColumnInfo("balance"), &ret_balance);

    return check_data(ret_id, ret_name, ret_balance);
}

basepb::Range *getPrimaryKeyRange() {
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

TEST_F(SelectFlowTest, SelectFlow_selection_table_read_cmp)
{
    std::string start_key;
    std::string end_key;
    std::map<std::string, std::string> mp_kv;

    { // create table
        EncodeKeyPrefix(&start_key, default_account_table_id);
        EncodeKeyPrefix(&end_key, default_account_table_id+1);
    }


    { // begin create range
        dspb::SchRequest req;
        req.mutable_create_range()->set_allocated_range(getPrimaryKeyRange());

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

        genKvPrimaryKeyData(mp_kv);

        for (auto kv : mp_kv) {
            // put data to db
            dspb::RangeRequest req;
            req.mutable_header()->set_range_id(default_range_id);
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

    // test <
    {
        dspb::RangeRequest req;
        auto header = req.mutable_header();
        header->set_range_id(default_range_id);
        header->mutable_range_epoch()->set_conf_ver(1);
        header->mutable_range_epoch()->set_version(1);

        auto select_flow = req.mutable_select_flow();
        // add table read processor
        auto table_read_processor = select_flow->add_processors();
        table_read_processor->set_type(dspb::TABLE_READ_TYPE );
        auto table_read = table_read_processor-> mutable_table_read();

        for ( const auto & col : ptr_table_account->GetAllColumns()) {

            table_read->add_columns()->CopyFrom(ptr_table_account->GetColumnInfo(col.id()));
        }

        table_read->set_type(dspb::KEYS_RANGE_TYPE);
        table_read->mutable_range()->set_start_key(start_key);
        table_read->mutable_range()->set_end_key(end_key);
        table_read->set_desc(false);

        auto selection_processor = select_flow->add_processors();
        selection_processor->set_type(dspb::SELECTION_TYPE);
        auto selection = selection_processor->mutable_selection();

        // selection: id < 1001
        dspb::Expr expr_less;
        int int_val = 1001;
        expr_less.set_expr_type(dspb::Less);

        auto expr_less_int_column = expr_less.add_child();
        expr_less_int_column->set_expr_type(dspb::Column);
        expr_less_int_column->mutable_column()->CopyFrom(ptr_table_account->GetColumnInfo("id"));

        auto expr_less_int_value = expr_less.add_child();
        expr_less_int_value->set_expr_type(dspb::Const_Int);
        expr_less_int_value->set_value(std::to_string(int_val));

        selection->add_filter()->CopyFrom(expr_less);

        select_flow->add_output_offsets(0);
        select_flow->add_output_offsets(1);
        select_flow->add_output_offsets(2);

        select_flow->set_gather_trace(false);

        dspb::RangeResponse resp;
        auto s = testSelectFlow(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error());
        ASSERT_TRUE(resp.has_select_flow());
        ASSERT_TRUE(resp.select_flow().rows_size() == 1) << resp.select_flow().rows_size();

        {
            for (auto & row : resp.select_flow().rows()) {
                ASSERT_TRUE(mp_kv.find(row.key()) != mp_kv.end())
                    << "Cann't found key:" <<  row.key() << "\tmp_kv.size:" << mp_kv.size();
                ASSERT_TRUE(check_row_value(row.value())) << "check_row_value error:" << row.DebugString();
                // std::cout << "row:" << row.DebugString() << std::endl;
            }

        }
    }

    // test <=
    {
        dspb::RangeRequest req;
        auto header = req.mutable_header();
        header->set_range_id(default_range_id);
        header->mutable_range_epoch()->set_conf_ver(1);
        header->mutable_range_epoch()->set_version(1);

        auto select_flow = req.mutable_select_flow();
        // add table read processor
        auto table_read_processor = select_flow->add_processors();
        table_read_processor->set_type(dspb::TABLE_READ_TYPE );
        auto table_read = table_read_processor-> mutable_table_read();

        for ( const auto & col : ptr_table_account->GetAllColumns()) {

            table_read->add_columns()->CopyFrom(ptr_table_account->GetColumnInfo(col.id()));
        }

        table_read->set_type(dspb::KEYS_RANGE_TYPE);
        table_read->mutable_range()->set_start_key(start_key);
        table_read->mutable_range()->set_end_key(end_key);
        table_read->set_desc(false);

        auto selection_processor = select_flow->add_processors();
        selection_processor->set_type(dspb::SELECTION_TYPE);
        auto selection = selection_processor->mutable_selection();

        // selection: id <= 1000
        dspb::Expr expr_less;
        int int_val = 1000;
        expr_less.set_expr_type(dspb::LessOrEqual);

        auto expr_less_int_column = expr_less.add_child();
        expr_less_int_column->set_expr_type(dspb::Column);
        expr_less_int_column->mutable_column()->CopyFrom(ptr_table_account->GetColumnInfo("id"));

        auto expr_less_int_value = expr_less.add_child();
        expr_less_int_value->set_expr_type(dspb::Const_Int);
        expr_less_int_value->set_value(std::to_string(int_val));

        selection->add_filter()->CopyFrom(expr_less);

        select_flow->add_output_offsets(0);
        select_flow->add_output_offsets(1);
        select_flow->add_output_offsets(2);

        select_flow->set_gather_trace(false);

        dspb::RangeResponse resp;
        auto s = testSelectFlow(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error());
        ASSERT_TRUE(resp.has_select_flow());
        ASSERT_TRUE(resp.select_flow().rows_size() == 1) << resp.select_flow().rows_size();

        {
            for (auto & row : resp.select_flow().rows()) {
                ASSERT_TRUE(mp_kv.find(row.key()) != mp_kv.end())
                    << "Cann't found key:" <<  row.key() << "\tmp_kv.size:" << mp_kv.size();
                ASSERT_TRUE(check_row_value(row.value())) << "check_row_value error:" << row.DebugString();
                // std::cout << "row:" << row.DebugString() << std::endl;
            }

        }
    }

    // test >
    {
        dspb::RangeRequest req;
        auto header = req.mutable_header();
        header->set_range_id(default_range_id);
        header->mutable_range_epoch()->set_conf_ver(1);
        header->mutable_range_epoch()->set_version(1);

        auto select_flow = req.mutable_select_flow();
        // add table read processor
        auto table_read_processor = select_flow->add_processors();
        table_read_processor->set_type(dspb::TABLE_READ_TYPE );
        auto table_read = table_read_processor-> mutable_table_read();

        for ( const auto & col : ptr_table_account->GetAllColumns()) {

            table_read->add_columns()->CopyFrom(ptr_table_account->GetColumnInfo(col.id()));
        }

        table_read->set_type(dspb::KEYS_RANGE_TYPE);
        table_read->mutable_range()->set_start_key(start_key);
        table_read->mutable_range()->set_end_key(end_key);
        table_read->set_desc(false);

        auto selection_processor = select_flow->add_processors();
        selection_processor->set_type(dspb::SELECTION_TYPE);
        auto selection = selection_processor->mutable_selection();

        // selection: id > 1018
        dspb::Expr expr_larger;
        int int_val = 1018;
        expr_larger.set_expr_type(dspb::Larger);

        auto expr_larger_int_column = expr_larger.add_child();
        expr_larger_int_column->set_expr_type(dspb::Column);
        expr_larger_int_column->mutable_column()->CopyFrom(ptr_table_account->GetColumnInfo("id"));

        auto expr_larger_int_value = expr_larger.add_child();
        expr_larger_int_value->set_expr_type(dspb::Const_Int);
        expr_larger_int_value->set_value(std::to_string(int_val));

        selection->add_filter()->CopyFrom(expr_larger);

        select_flow->add_output_offsets(0);
        select_flow->add_output_offsets(1);
        select_flow->add_output_offsets(2);

        select_flow->set_gather_trace(false);

        dspb::RangeResponse resp;
        auto s = testSelectFlow(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error());
        ASSERT_TRUE(resp.has_select_flow());
        ASSERT_TRUE(resp.select_flow().rows_size() == 1) << resp.select_flow().rows_size();

        {
            for (auto & row : resp.select_flow().rows()) {
                ASSERT_TRUE(mp_kv.find(row.key()) != mp_kv.end())
                    << "Cann't found key:" <<  row.key() << "\tmp_kv.size:" << mp_kv.size();
                ASSERT_TRUE(check_row_value(row.value())) << "check_row_value error:" << row.DebugString();
                // std::cout << "row:" << row.DebugString() << std::endl;
            }

        }
    }

    // test >=
    {
        dspb::RangeRequest req;
        auto header = req.mutable_header();
        header->set_range_id(default_range_id);
        header->mutable_range_epoch()->set_conf_ver(1);
        header->mutable_range_epoch()->set_version(1);

        auto select_flow = req.mutable_select_flow();
        // add table read processor
        auto table_read_processor = select_flow->add_processors();
        table_read_processor->set_type(dspb::TABLE_READ_TYPE );
        auto table_read = table_read_processor-> mutable_table_read();

        for ( const auto & col : ptr_table_account->GetAllColumns()) {

            table_read->add_columns()->CopyFrom(ptr_table_account->GetColumnInfo(col.id()));
        }

        table_read->set_type(dspb::KEYS_RANGE_TYPE);
        table_read->mutable_range()->set_start_key(start_key);
        table_read->mutable_range()->set_end_key(end_key);
        table_read->set_desc(false);

        auto selection_processor = select_flow->add_processors();
        selection_processor->set_type(dspb::SELECTION_TYPE);
        auto selection = selection_processor->mutable_selection();

        // selection: id >= 1019
        dspb::Expr expr_larger;
        int int_val = 1019;
        expr_larger.set_expr_type(dspb::LargerOrEqual);

        auto expr_larger_int_column = expr_larger.add_child();
        expr_larger_int_column->set_expr_type(dspb::Column);
        expr_larger_int_column->mutable_column()->CopyFrom(ptr_table_account->GetColumnInfo("id"));

        auto expr_larger_int_value = expr_larger.add_child();
        expr_larger_int_value->set_expr_type(dspb::Const_Int);
        expr_larger_int_value->set_value(std::to_string(int_val));

        selection->add_filter()->CopyFrom(expr_larger);

        select_flow->add_output_offsets(0);
        select_flow->add_output_offsets(1);
        select_flow->add_output_offsets(2);

        select_flow->set_gather_trace(false);

        dspb::RangeResponse resp;
        auto s = testSelectFlow(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error());
        ASSERT_TRUE(resp.has_select_flow());
        ASSERT_TRUE(resp.select_flow().rows_size() == 1) << resp.select_flow().rows_size();

        {
            for (auto & row : resp.select_flow().rows()) {
                ASSERT_TRUE(mp_kv.find(row.key()) != mp_kv.end())
                    << "Cann't found key:" <<  row.key() << "\tmp_kv.size:" << mp_kv.size();
                ASSERT_TRUE(check_row_value(row.value())) << "check_row_value error:" << row.DebugString();
                // std::cout << "row:" << row.DebugString() << std::endl;
            }

        }
    }
}

