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
const static int32_t default_table_data_count = 20;
const static uint8_t ecode_null= 0x00;

class StatsTest : public ::testing::Test {
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

    Status testRangeReq(const dspb::RangeRequest& req, dspb::RangeResponse& resp) {
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
 * | 1001 |    name_1001      |     1001   |
 * | 1002 |    name_1002      |     1002   |
 * | 1003 |    name_1003      |     1003   |
 * | 1004 |    name_1004      |     1004   |
 * | 1005 |    nullptr        |     1005   |
 * =========================================
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

basepb::Range *genNonUniqueIndexKeyRange1Col(int id) {
    auto meta = new basepb::Range;

    meta->set_id(default_range_id+2);
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

    auto ks = meta->mutable_key_schema();

    auto i_col2 = ptr_table_account->GetColumn("name");
    auto k_col_2 = ks->add_key_cols();
    k_col_2->set_id(i_col2.id());
    k_col_2->set_type(i_col2.sql_type().type());
    k_col_2->set_unsigned_(i_col2.sql_type().unsigned_());

    ks->set_unique_index(false);

    auto pks = ptr_table_account->GetPKs();
    for (const auto& pk : pks) {
        auto p = ks->add_extra_cols();

        p->set_id(pk.id());
        p->set_type(pk.sql_type().type());
        p->set_unsigned_(pk.sql_type().unsigned_());
    }

    return meta;
}

basepb::Range *genNonUniqueIndexKeyRange2Cols(int id) {
    auto meta = new basepb::Range;

    meta->set_id(default_range_id+2);
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

    auto ks = meta->mutable_key_schema();

    auto i_col2 = ptr_table_account->GetColumn("name");
    auto k_col_2 = ks->add_key_cols();
    k_col_2->set_id(i_col2.id());
    k_col_2->set_type(i_col2.sql_type().type());
    k_col_2->set_unsigned_(i_col2.sql_type().unsigned_());
    
    i_col2 = ptr_table_account->GetColumn("balance");
    k_col_2 = ks->add_key_cols();
    k_col_2->set_id(i_col2.id());
    k_col_2->set_type(i_col2.sql_type().type());
    k_col_2->set_unsigned_(i_col2.sql_type().unsigned_());

    ks->set_unique_index(false);

    auto pks = ptr_table_account->GetPKs();
    for (const auto& pk : pks) {
        auto p = ks->add_extra_cols();

        p->set_id(pk.id());
        p->set_type(pk.sql_type().type());
        p->set_unsigned_(pk.sql_type().unsigned_());
    }

    return meta;
}

void genKvUniqueIndexKeyData_1_col(std::map<std::string, std::string > & mp)
{
    typedef int col_id_type;
    for (auto id = 0; id < default_table_data_count; id++) {
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
        
        // kv_value
        std::string tmp_str;
        for ( auto & col : ptr_table_account->GetPKs()) {
            EncodeKvKey(&tmp_str, col, row.at(col.id()));
        }
        auto s = assembleValue(value, tmp_str, id, nullptr);
        if (!s.ok()) {
            std::cerr << "assembleValue error. id:" << id << std::endl;
        }

        mp.emplace(key, value);
    }
}

void genKvUniqueIndexKeyData_1_col_have_null(std::map<std::string, std::string > & mp)
{
    typedef int col_id_type;
    for (auto id = 0; id < default_table_data_count; id++) {
        std::string key;
        std::string value;

        // make row data
        std::map<col_id_type, std::string> row;
        row.emplace(1, std::to_string(1000+id));
        row.emplace(2, "name_" + std::to_string(1000+id));    
        row.emplace(3, std::to_string(1000+id));

        EncodeKeyPrefix(&key, default_account_index_id1);
        
        if (id % 2 == 1) {
            key.push_back(ecode_null);
        } else {
            auto i_col1 = ptr_table_account->GetColumn("name");
            EncodeKvKey(&key, i_col1, row.at(i_col1.id()));
        } 

        // kv_value
        std::string tmp_str;
        for ( auto & col : ptr_table_account->GetPKs()) {
            EncodeKvKey(&tmp_str, col,row.at(col.id()));
        }
        auto s = assembleValue(value, tmp_str, id, nullptr);
        if (!s.ok()) {
            std::cerr << "assembleValue error. id:" << id << std::endl;
        }

        mp.emplace(key, value);
    }
}
void genKvUniqueIndexKeyData_2_col(std::map<std::string, std::string > & mp)
{
    typedef int col_id_type;
    for (auto id = 0; id < default_table_data_count; id++) {
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
        auto s = assembleValue(value, tmp_str, id, nullptr);
        if (!s.ok()) {
            std::cerr << "assembleValue error. id:" << id << std::endl;
        }

        mp.emplace(key, value);
    }
}

void genKvUniqueIndexKeyData_2_col_have_null(std::map<std::string, std::string > & mp)
{
    typedef int col_id_type;

    for (auto id = 0; id < default_table_data_count; id++) {
        std::string key;
        std::string value;

        // make row data
        std::map<col_id_type, std::string> row;
        row.emplace(1, std::to_string(1000+id));
        row.emplace(2, "name_" + std::to_string(1000+id));    
        row.emplace(3, std::to_string(1000+id));

        // kv_key
        EncodeKeyPrefix(&key, default_account_index_id1);
        
        if (id % 2 == 1) {
            key.push_back(ecode_null);
            key.push_back(ecode_null);
        } else {
            auto i_col1 = ptr_table_account->GetColumn("name");
            EncodeKvKey(&key, i_col1, row.at(i_col1.id()));

            auto i_col2 = ptr_table_account->GetColumn("balance");
            EncodeKvKey(&key, i_col2, row.at(i_col2.id()));
        }

        // kv_value
        std::string tmp_str;
        for ( auto & col : ptr_table_account->GetPKs()) {
            EncodeKvKey(&tmp_str, col, row.at(col.id()));
        }
        auto s = assembleValue(value, tmp_str, id, nullptr);
        if (!s.ok()) {
            std::cerr << "assembleValue error. id:" << id << std::endl;
        }

        mp.emplace(key, value);
    }
}

void genKvNonUniqueIndexKeyData_1_col(std::map<std::string, std::string > & mp)
{

    typedef int col_id_type;
    for (auto id = 0; id < default_table_data_count; id++) {
        std::string key;
        std::string value;

        // make row data
        std::map<col_id_type, std::string> row;
        row.emplace(1, std::to_string(1000+id));
        row.emplace(2, "name_" + std::to_string(1000+id));

        // kv_key
        EncodeKeyPrefix(&key, default_account_index_id2);
        auto i_col = ptr_table_account->GetColumn("name");
        EncodeKvKey(&key, i_col,row.at(i_col.id()));
        for ( auto & col : ptr_table_account->GetPKs()) {
            EncodeKvKey(&key, col,row.at(col.id()));
        }

        mp.emplace(key, value);
    }
}

void genKvNonUniqueIndexKeyData_1_col_have_null(std::map<std::string, std::string > & mp)
{

    typedef int col_id_type;
    for (auto id = 0; id < default_table_data_count; id++) {
        std::string key;
        std::string value;

        // make row data
        std::map<col_id_type, std::string> row;
        row.emplace(1, std::to_string(1000+id));
        row.emplace(2, "name_" + std::to_string(1000+id));
        row.emplace(3, std::to_string(1000+id));

        EncodeKeyPrefix(&key, default_account_index_id2);
        if (id % 2 == 0) {
            auto i_col = ptr_table_account->GetColumn("name");
            EncodeKvKey(&key, i_col,row.at(i_col.id()));
        } else {
            key.push_back(ecode_null);
        }
       
        for ( auto & col : ptr_table_account->GetPKs()) {
            EncodeKvKey(&key, col,row.at(col.id()));
        }

        mp.emplace(key, value);
    }
}

void genKvNonUniqueIndexKeyData_2_col(std::map<std::string, std::string > & mp)
{

    typedef int col_id_type;
    for (auto id = 0; id < default_table_data_count; id++) {
        std::string key;
        std::string value;

        // make row data
        std::map<col_id_type, std::string> row;
        row.emplace(1, std::to_string(1000+id));
        row.emplace(2, "name_" + std::to_string(1000+id));
        row.emplace(3, std::to_string(1000+id));

        EncodeKeyPrefix(&key, default_account_index_id2);
        auto i_col = ptr_table_account->GetColumn("name");
        EncodeKvKey(&key, i_col,row.at(i_col.id()));
        i_col = ptr_table_account->GetColumn("balance");
        EncodeKvKey(&key, i_col,row.at(i_col.id()));
        
        for ( auto & col : ptr_table_account->GetPKs()) {
            EncodeKvKey(&key, col,row.at(col.id()));
        }

        mp.emplace(key, value);
    }

}

void genKvNonUniqueIndexKeyData_2_col_have_null(std::map<std::string, std::string > & mp)
{

    typedef int col_id_type;
    for (auto id = 0; id < default_table_data_count; id++) {
        std::string key;
        std::string value;

        // make row data
        std::map<col_id_type, std::string> row;
        row.emplace(1, std::to_string(1000+id));
        row.emplace(2, "name_" + std::to_string(1000+id));
        row.emplace(3, std::to_string(1000+id));

        EncodeKeyPrefix(&key, default_account_index_id2);
        if (id % 2 == 0) {
            auto i_col = ptr_table_account->GetColumn("name");
            EncodeKvKey(&key, i_col,row.at(i_col.id()));
            i_col = ptr_table_account->GetColumn("balance");
            EncodeKvKey(&key, i_col,row.at(i_col.id()));
        } else {
            key.push_back(ecode_null);
            key.push_back(ecode_null);
        }
        
        for ( auto & col : ptr_table_account->GetPKs()) {
            EncodeKvKey(&key, col,row.at(col.id()));
        }

        mp.emplace(key, value);
    }
}


TEST_F(StatsTest, colsStatsAnalyze) {
    std::string start_key;
    std::string end_key;
    std::string end_key_1;
    long long data_count = 5;

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

        for (auto id = 1; id < data_count + 1; id++) {
            std::string key;
            std::string value;

            // make row data
            std::map<col_id_type, std::string> row;
            row.emplace(1, std::to_string(1000+id));
            if (id != data_count) { 
                row.emplace(2, "name_" + std::to_string(1000+id));    
            }
            
            row.emplace(3, std::to_string(1000+(id%5)));

            // make encode key
            EncodeKeyPrefix(&key, default_account_table_id); // key:100000001 ("\x01"+"00000001")
            for ( auto & col : ptr_table_account->GetPKs()) {
                EncodeKvKey(&key, col,row.at(col.id()));   // key: key+ row_id(encoded)
            }

            // make encode value
            std::string tmp_str;
            for (auto & col : ptr_table_account->GetNonPkColumns()) {
                if (row.find(col.id()) != row.end()) {
                    EncodeKvValue(&tmp_str, col, row.at(col.id()));    
                }
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
            auto s = testRangeReq(req, resp);
            ASSERT_TRUE(s.ok()) << s.ToString();
            ASSERT_FALSE(resp.header().has_error());
        }
    }

    { // test ColumnsStatsRequest

        dspb::RangeRequest req;
        auto header = req.mutable_header();
        header->set_range_id(default_range_id);
        header->mutable_range_epoch()->set_conf_ver(1);
        header->mutable_range_epoch()->set_version(1);

        auto colsStatsReq = req.mutable_columns_stats();
        colsStatsReq->set_bucket_max(3);
        colsStatsReq->set_sample_max(3);
        colsStatsReq->set_sketch_max(3);
        colsStatsReq->set_cmsketch_depth(4);
        colsStatsReq->set_cmsketch_width(32);
        for ( const auto & col : ptr_table_account->GetAllColumns()) {
            colsStatsReq->add_columns_info()->CopyFrom(ptr_table_account->GetColumnInfo(col.id()));
        }
        colsStatsReq->mutable_range()->set_start_key(start_key);
        colsStatsReq->mutable_range()->set_end_key(end_key);
        
        dspb::RangeResponse resp;
        // std::cout << "req before:" << req.DebugString() << std::endl;
        auto s = testRangeReq(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error());
        auto colsStatsResp = resp.cols_stats();        
        // std::cout << "colsStatsResp:\n" << colsStatsResp.DebugString() << std::endl;
        ASSERT_EQ(colsStatsResp.pk_hist().buckets_size(), 3);
        ASSERT_EQ(colsStatsResp.pk_hist().num_dist(), 5);
        auto collectors = colsStatsResp.collectors();
        auto table_account_len = ptr_table_account->GetAllColumns().size();
        ASSERT_EQ((uint64_t)collectors.size(), table_account_len - 1);
        ASSERT_EQ(collectors[0].null_count(), 1);
        ASSERT_EQ(collectors[0].count(), 4);
        auto rows = collectors[0].cm_sketch().rows();
        ASSERT_EQ(rows.size(), 4);
        
        for (int i = 0; i < rows.size(); ++i) {
            uint32_t sum = 0;
            for (auto countersVal : rows[i].counters()) {
                sum += countersVal;
            }
            // std::cout << "collectors[0].rows[" << i
            //           << "] sum:" << sum
            //           << std::endl;
            ASSERT_EQ(sum, (uint32_t)4);
        }
        
        ASSERT_EQ(collectors[1].null_count(), 0);
        ASSERT_EQ(collectors[1].count(), 5);
        rows = collectors[1].cm_sketch().rows();
        ASSERT_EQ(rows.size(), 4);
        for (int i = 0; i < rows.size(); ++i) {
            uint32_t sum = 0;
            for (auto countersVal : rows[i].counters()) {
                sum += countersVal;
            }
            // std::cout << "collectors[0].rows[" << i
            //           << "] sum:" << sum
            //           << std::endl;
            ASSERT_EQ(sum, (uint32_t)5);     
        }
     
    }
}

TEST_F(StatsTest, idxUniqueStatsAnalyze_1_col) {
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

        genKvUniqueIndexKeyData_1_col(mp_kv);

        for (auto kv : mp_kv) {
            // put data to db
            dspb::RangeRequest req;
            req.mutable_header()->set_range_id(default_range_id+1);
            req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
            req.mutable_header()->mutable_range_epoch()->set_version(1);
            req.mutable_kv_put()->set_key(kv.first);
            req.mutable_kv_put()->set_value(kv.second);

            dspb::RangeResponse resp;
            auto s = testRangeReq(req, resp);
            ASSERT_TRUE(s.ok()) << s.ToString();
            ASSERT_FALSE(resp.header().has_error());
        }

    }

    {   
        dspb::RangeRequest req;
        auto header = req.mutable_header();
        header->set_range_id(default_range_id+1);
        header->mutable_range_epoch()->set_conf_ver(1);
        header->mutable_range_epoch()->set_version(1);

        auto idxStatsReq = req.mutable_index_stats();
        
        idxStatsReq->set_bucket_max(3);
        
        idxStatsReq->add_columns_info()->CopyFrom(ptr_table_account->GetColumnInfo("name"));

        idxStatsReq->set_cmsketch_depth(4);
        idxStatsReq->set_cmsketch_width(32);
        idxStatsReq->set_unique(true);
        idxStatsReq->mutable_range()->set_start_key(start_key);
        idxStatsReq->mutable_range()->set_end_key(end_key);

        dspb::RangeResponse resp;
        auto s = testRangeReq(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error());
        auto idxResp = resp.idx_stats();
        // std::cout << "idxResp:\n" << idxResp.DebugString() << std::endl;
        auto hist = idxResp.hist();
        ASSERT_EQ(hist.num_dist(), (int64_t)default_table_data_count);
        ASSERT_EQ(hist.buckets_size(), 3);
        ASSERT_EQ(idxResp.null_count(), (uint64_t)0);
        auto rows = idxResp.cms().rows();
        ASSERT_EQ(rows.size(), 4);
        for (int i = 0; i < 4; ++i) {
            uint32_t sum = 0;
            for (auto val : rows[i].counters()) {
                sum += val;
            }
            // std::cout << "rows[" << i
            //           << "] sum:" << sum
            //           << std::endl;
            ASSERT_EQ(sum, (uint32_t)default_table_data_count);
        }
    }
}


TEST_F(StatsTest, idxUniqueStatsAnalyze_1_col_have_null) {
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

        genKvUniqueIndexKeyData_1_col_have_null(mp_kv);

        for (auto kv : mp_kv) {
            // put data to db 
            dspb::RangeRequest req;
            req.mutable_header()->set_range_id(default_range_id+1);
            req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
            req.mutable_header()->mutable_range_epoch()->set_version(1);
            req.mutable_kv_put()->set_key(kv.first);
            req.mutable_kv_put()->set_value(kv.second);

            dspb::RangeResponse resp;
            auto s = testRangeReq(req, resp);
            ASSERT_TRUE(s.ok()) << s.ToString();
            ASSERT_FALSE(resp.header().has_error());
        }

    }

    {   
        dspb::RangeRequest req;
        auto header = req.mutable_header();
        header->set_range_id(default_range_id+1);
        header->mutable_range_epoch()->set_conf_ver(1);
        header->mutable_range_epoch()->set_version(1);

        auto idxStatsReq = req.mutable_index_stats();
        
        idxStatsReq->set_bucket_max(3);
        
        idxStatsReq->add_columns_info()->CopyFrom(ptr_table_account->GetColumnInfo("name"));
        idxStatsReq->set_cmsketch_depth(4);
        idxStatsReq->set_cmsketch_width(32);
        idxStatsReq->set_unique(true);
        idxStatsReq->mutable_range()->set_start_key(start_key);
        idxStatsReq->mutable_range()->set_end_key(end_key);

        dspb::RangeResponse resp;
        auto s = testRangeReq(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error());
        auto idxResp = resp.idx_stats();
        // std::cout << "idxResp:\n" << idxResp.DebugString() << std::endl; 
        auto hist = idxResp.hist();
        ASSERT_EQ(hist.num_dist(), (int64_t)default_table_data_count/2 + 1);
        ASSERT_EQ(hist.buckets_size(), 3);
        ASSERT_EQ(idxResp.null_count(), (uint64_t)1);
        auto rows = idxResp.cms().rows();
        ASSERT_EQ(rows.size(), 4);
        for (int i = 0; i < 4; ++i) {
            uint32_t sum = 0;
            for (auto val : rows[i].counters()) {
                sum += val;
            }
            // std::cout << "rows[" << i
            //           << "] sum:" << sum
            //           << std::endl; 
            ASSERT_EQ(sum, (uint32_t)default_table_data_count/2);
        }
    }
}

TEST_F(StatsTest, idxUniqueStatsAnalyze_2_col) {
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

        genKvUniqueIndexKeyData_2_col(mp_kv);

        for (auto kv : mp_kv) {
            // put data to db
            dspb::RangeRequest req;
            req.mutable_header()->set_range_id(default_range_id+1);
            req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
            req.mutable_header()->mutable_range_epoch()->set_version(1);
            req.mutable_kv_put()->set_key(kv.first);
            req.mutable_kv_put()->set_value(kv.second);

            dspb::RangeResponse resp;
            auto s = testRangeReq(req, resp);
            ASSERT_TRUE(s.ok()) << s.ToString();
            ASSERT_FALSE(resp.header().has_error());
        }

    }

    {   
        dspb::RangeRequest req;
        auto header = req.mutable_header();
        header->set_range_id(default_range_id+1);
        header->mutable_range_epoch()->set_conf_ver(1);
        header->mutable_range_epoch()->set_version(1);

        auto idxStatsReq = req.mutable_index_stats();
        
        idxStatsReq->set_bucket_max(3);
        
        idxStatsReq->add_columns_info()->CopyFrom(ptr_table_account->GetColumnInfo("name"));
        idxStatsReq->add_columns_info()->CopyFrom(ptr_table_account->GetColumnInfo("balance"));
        idxStatsReq->set_cmsketch_depth(4);
        idxStatsReq->set_cmsketch_width(32);
        idxStatsReq->set_unique(true);
        idxStatsReq->mutable_range()->set_start_key(start_key);
        idxStatsReq->mutable_range()->set_end_key(end_key);

        dspb::RangeResponse resp;
        auto s = testRangeReq(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error());
        auto idxResp = resp.idx_stats();
        // std::cout << "idxResp:\n" << idxResp.DebugString() << std::endl;
        auto hist = idxResp.hist();
        ASSERT_EQ(hist.num_dist(), (int64_t)default_table_data_count);
        ASSERT_EQ(hist.buckets_size(), 3);
        ASSERT_EQ(idxResp.null_count(), (uint64_t)0);
        auto rows = idxResp.cms().rows();
        ASSERT_EQ(rows.size(), 4);
        for (int i = 0; i < 4; ++i) {
            uint32_t sum = 0;
            for (auto val : rows[i].counters()) {
                sum += val;
            }
            // std::cout << "rows[" << i
            //           << "] sum:" << sum
            //           << std::endl;
            ASSERT_EQ(sum, (uint32_t)default_table_data_count*2);
        }
    }
}

TEST_F(StatsTest, idxUniqueStatsAnalyze_2_col_have_null) {
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

        genKvUniqueIndexKeyData_2_col_have_null(mp_kv);

        for (auto kv : mp_kv) {
            // put data to db
            dspb::RangeRequest req;
            req.mutable_header()->set_range_id(default_range_id+1);
            req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
            req.mutable_header()->mutable_range_epoch()->set_version(1);
            req.mutable_kv_put()->set_key(kv.first);
            req.mutable_kv_put()->set_value(kv.second);

            dspb::RangeResponse resp;
            auto s = testRangeReq(req, resp);
            ASSERT_TRUE(s.ok()) << s.ToString();
            ASSERT_FALSE(resp.header().has_error());
        }

    }

    {   
        dspb::RangeRequest req;
        auto header = req.mutable_header();
        header->set_range_id(default_range_id+1);
        header->mutable_range_epoch()->set_conf_ver(1);
        header->mutable_range_epoch()->set_version(1);

        auto idxStatsReq = req.mutable_index_stats();
        
        idxStatsReq->set_bucket_max(3);
        
        idxStatsReq->add_columns_info()->CopyFrom(ptr_table_account->GetColumnInfo("name"));
        idxStatsReq->add_columns_info()->CopyFrom(ptr_table_account->GetColumnInfo("balance"));
        idxStatsReq->set_cmsketch_depth(4);
        idxStatsReq->set_cmsketch_width(32);
        idxStatsReq->set_unique(true);
        idxStatsReq->mutable_range()->set_start_key(start_key);
        idxStatsReq->mutable_range()->set_end_key(end_key);

        dspb::RangeResponse resp;
        auto s = testRangeReq(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error());
        auto idxResp = resp.idx_stats();
        // std::cout << "idxResp:\n" << idxResp.DebugString() << std::endl;
        auto hist = idxResp.hist();
        ASSERT_EQ(hist.num_dist(), (int64_t)default_table_data_count/2 + 1);
        ASSERT_EQ(hist.buckets_size(), 3);
        ASSERT_EQ(idxResp.null_count(), (uint64_t)0);
        auto rows = idxResp.cms().rows();
        ASSERT_EQ(rows.size(), 4);
        for (int i = 0; i < 4; ++i) {
            uint32_t sum = 0;
            for (auto val : rows[i].counters()) {
                sum += val;
            }
            // std::cout << "rows[" << i
            //           << "] sum:" << sum
            //           << std::endl;
            ASSERT_EQ(sum, (uint32_t)default_table_data_count);
        }
    }
}
TEST_F(StatsTest, idxNonUniqueStatsAnalyze_1_col) {
    std::string start_key;
    std::string end_key;

    std::map<std::string, std::string> mp_kv;

    { // create table
        EncodeKeyPrefix(&start_key, default_account_index_id2);
        EncodeKeyPrefix(&end_key, default_account_index_id2+1);
    }


    { // begin create range
        dspb::SchRequest req;
        req.mutable_create_range()->set_allocated_range(genNonUniqueIndexKeyRange1Col(default_account_index_id2));

        // create range
        auto rpc = NewMockRPCRequest(req);
        range_server_->dispatchSchedule(rpc.first);
        ASSERT_FALSE(range_server_->ranges_.Empty());
        ASSERT_TRUE(range_server_->Find(default_range_id+2) != nullptr);

        // check meta
        std::vector<basepb::Range> metas;
        auto ret = range_server_->meta_store_->GetAllRange(&metas);
        ASSERT_TRUE(metas.size() == 1) << metas.size();

        // raft
        auto raft = static_cast<RaftMock *>(range_server_->ranges_.Find(default_range_id + 2)->raft_.get());
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_.Find(default_range_id + 2)->is_leader_ = true;

    }

    { // put data

        genKvNonUniqueIndexKeyData_1_col(mp_kv);

        for (auto kv : mp_kv) {
            // put data to db
            dspb::RangeRequest req;
            req.mutable_header()->set_range_id(default_range_id+2);
            req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
            req.mutable_header()->mutable_range_epoch()->set_version(1);
            req.mutable_kv_put()->set_key(kv.first);
            req.mutable_kv_put()->set_value(kv.second);

            dspb::RangeResponse resp;
            auto s = testRangeReq(req, resp);
            ASSERT_TRUE(s.ok()) << s.ToString();
            ASSERT_FALSE(resp.header().has_error());
        }

    }

    {   
        dspb::RangeRequest req;
        auto header = req.mutable_header();
        header->set_range_id(default_range_id+2);
        header->mutable_range_epoch()->set_conf_ver(1);
        header->mutable_range_epoch()->set_version(1);

        auto idxStatsReq = req.mutable_index_stats();
        
        idxStatsReq->set_bucket_max(3);
        
        idxStatsReq->add_columns_info()->CopyFrom(ptr_table_account->GetColumnInfo("name"));
        
        idxStatsReq->set_cmsketch_depth(4);
        idxStatsReq->set_cmsketch_width(32);
        idxStatsReq->set_unique(false);
        idxStatsReq->mutable_range()->set_start_key(start_key);
        idxStatsReq->mutable_range()->set_end_key(end_key);

        dspb::RangeResponse resp;
        auto s = testRangeReq(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error());
        auto idxResp = resp.idx_stats();
        // std::cout << "idxResp:\n" << idxResp.DebugString() << std::endl;
        auto hist = idxResp.hist();
        ASSERT_EQ(hist.num_dist(), (int64_t)1);
        ASSERT_EQ(hist.buckets_size(), 1);
        ASSERT_EQ(idxResp.null_count(), (uint64_t)0);
        auto rows = idxResp.cms().rows();
        ASSERT_EQ(rows.size(), 4);
        for (int i = 0; i < 4; ++i) {
            uint32_t sum = 0;
            for (auto val : rows[i].counters()) {
                sum += val;
            }
            // std::cout << "rows[" << i
            //           << "] sum:" << sum
            //           << std::endl;
            ASSERT_EQ(sum, (uint32_t)default_table_data_count);
        }
    }
}

TEST_F(StatsTest, idxNonUniqueStatsAnalyze_1_col_have_null) {
    std::string start_key;
    std::string end_key;

    std::map<std::string, std::string> mp_kv;

    { // create table
        EncodeKeyPrefix(&start_key, default_account_index_id2);
        EncodeKeyPrefix(&end_key, default_account_index_id2+1);
    }


    { // begin create range
        dspb::SchRequest req;
        req.mutable_create_range()->set_allocated_range(genNonUniqueIndexKeyRange1Col(default_account_index_id2));

        // create range
        auto rpc = NewMockRPCRequest(req);
        range_server_->dispatchSchedule(rpc.first);
        ASSERT_FALSE(range_server_->ranges_.Empty());
        ASSERT_TRUE(range_server_->Find(default_range_id+2) != nullptr);

        // check meta
        std::vector<basepb::Range> metas;
        auto ret = range_server_->meta_store_->GetAllRange(&metas);
        ASSERT_TRUE(metas.size() == 1) << metas.size();

        // raft
        auto raft = static_cast<RaftMock *>(range_server_->ranges_.Find(default_range_id + 2)->raft_.get());
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_.Find(default_range_id + 2)->is_leader_ = true;

    }

    { // put data

        genKvNonUniqueIndexKeyData_1_col_have_null(mp_kv);

        for (auto kv : mp_kv) {
            // put data to db
            dspb::RangeRequest req;
            req.mutable_header()->set_range_id(default_range_id+2);
            req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
            req.mutable_header()->mutable_range_epoch()->set_version(1);
            req.mutable_kv_put()->set_key(kv.first);
            req.mutable_kv_put()->set_value(kv.second);

            dspb::RangeResponse resp;
            auto s = testRangeReq(req, resp);
            ASSERT_TRUE(s.ok()) << s.ToString();
            ASSERT_FALSE(resp.header().has_error());
        }

    }

    {   
        dspb::RangeRequest req;
        auto header = req.mutable_header();
        header->set_range_id(default_range_id+2);
        header->mutable_range_epoch()->set_conf_ver(1);
        header->mutable_range_epoch()->set_version(1);

        auto idxStatsReq = req.mutable_index_stats();
        
        idxStatsReq->set_bucket_max(3);
        
        idxStatsReq->add_columns_info()->CopyFrom(ptr_table_account->GetColumnInfo("name"));
        idxStatsReq->set_cmsketch_depth(4);
        idxStatsReq->set_cmsketch_width(32);
        idxStatsReq->set_unique(false);
        idxStatsReq->mutable_range()->set_start_key(start_key);
        idxStatsReq->mutable_range()->set_end_key(end_key);

        dspb::RangeResponse resp;
        auto s = testRangeReq(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error());
        auto idxResp = resp.idx_stats();
        // std::cout << "idxResp:\n" << idxResp.DebugString() << std::endl;
        auto hist = idxResp.hist();
        ASSERT_EQ(hist.num_dist(), (int64_t)1);
        ASSERT_EQ(hist.buckets_size(), 1);
        ASSERT_EQ(idxResp.null_count(), (uint64_t)default_table_data_count/2);
        auto rows = idxResp.cms().rows();
        ASSERT_EQ(rows.size(), 4);
        for (int i = 0; i < 4; ++i) {
            uint32_t sum = 0;
            for (auto val : rows[i].counters()) {
                sum += val;
            }
            // std::cout << "rows[" << i
            //           << "] sum:" << sum
            //           << std::endl;
            ASSERT_EQ(sum, (uint32_t)default_table_data_count/2);
        }
    }
}

TEST_F(StatsTest, idxNonUniqueStatsAnalyze_2_col) {
    std::string start_key;
    std::string end_key;
    std::map<std::string, std::string> mp_kv;
    { // create table
        EncodeKeyPrefix(&start_key, default_account_index_id2);
        EncodeKeyPrefix(&end_key, default_account_index_id2+1);
    }

    { // begin create range
        dspb::SchRequest req;
        req.mutable_create_range()->set_allocated_range(genNonUniqueIndexKeyRange2Cols(default_account_index_id2));

        // create range
        auto rpc = NewMockRPCRequest(req);
        range_server_->dispatchSchedule(rpc.first);
        ASSERT_FALSE(range_server_->ranges_.Empty());
        ASSERT_TRUE(range_server_->Find(default_range_id+2) != nullptr);

        // check meta
        std::vector<basepb::Range> metas;
        auto ret = range_server_->meta_store_->GetAllRange(&metas);
        ASSERT_TRUE(metas.size() == 1) << metas.size();

        // raft
        auto raft = static_cast<RaftMock *>(range_server_->ranges_.Find(default_range_id + 2)->raft_.get());
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_.Find(default_range_id + 2)->is_leader_ = true;

    }

    { // put data

        genKvNonUniqueIndexKeyData_2_col(mp_kv);

        for (auto kv : mp_kv) {
            // put data to db
            dspb::RangeRequest req;
            req.mutable_header()->set_range_id(default_range_id+2);
            req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
            req.mutable_header()->mutable_range_epoch()->set_version(1);
            req.mutable_kv_put()->set_key(kv.first);
            req.mutable_kv_put()->set_value(kv.second);

            dspb::RangeResponse resp;
            auto s = testRangeReq(req, resp);
            ASSERT_TRUE(s.ok()) << s.ToString();
            ASSERT_FALSE(resp.header().has_error());
        }

    }

    {   
        dspb::RangeRequest req;
        auto header = req.mutable_header();
        header->set_range_id(default_range_id+2);
        header->mutable_range_epoch()->set_conf_ver(1);
        header->mutable_range_epoch()->set_version(1);

        auto idxStatsReq = req.mutable_index_stats();
        
        idxStatsReq->set_bucket_max(3);
        
        idxStatsReq->add_columns_info()->CopyFrom(ptr_table_account->GetColumnInfo("name"));
        idxStatsReq->add_columns_info()->CopyFrom(ptr_table_account->GetColumnInfo("balance"));
        idxStatsReq->set_cmsketch_depth(4);
        idxStatsReq->set_cmsketch_width(32);
        idxStatsReq->set_unique(false);
        idxStatsReq->mutable_range()->set_start_key(start_key);
        idxStatsReq->mutable_range()->set_end_key(end_key);

        dspb::RangeResponse resp;
        auto s = testRangeReq(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error());
        auto idxResp = resp.idx_stats();
        // std::cout << "idxResp:\n" << idxResp.DebugString() << std::endl;
        auto hist = idxResp.hist();
        ASSERT_EQ(hist.num_dist(), (int64_t)1);
        ASSERT_EQ(hist.buckets_size(), 1);
        ASSERT_EQ(idxResp.null_count(), (uint64_t)0);
        auto rows = idxResp.cms().rows();
        ASSERT_EQ(rows.size(), 4);
        for (int i = 0; i < 4; ++i) {
            uint32_t sum = 0;
            for (auto val : rows[i].counters()) {
                sum += val;
            }
            // std::cout << "rows[" << i
            //           << "] sum:" << sum
            //           << std::endl;
            ASSERT_EQ(sum, (uint32_t)default_table_data_count*2);
        }
    }
}
TEST_F(StatsTest, idxNonUniqueStatsAnalyze_2_col_have_null) {
    std::string start_key;
    std::string end_key;

    std::map<std::string, std::string> mp_kv;

    { // create table
        EncodeKeyPrefix(&start_key, default_account_index_id2);
        EncodeKeyPrefix(&end_key, default_account_index_id2+1);
    }


    { // begin create range
        dspb::SchRequest req;
        req.mutable_create_range()->set_allocated_range(genNonUniqueIndexKeyRange2Cols(default_account_index_id2));

        // create range
        auto rpc = NewMockRPCRequest(req);
        range_server_->dispatchSchedule(rpc.first);
        ASSERT_FALSE(range_server_->ranges_.Empty());
        ASSERT_TRUE(range_server_->Find(default_range_id+2) != nullptr);

        // check meta
        std::vector<basepb::Range> metas;
        auto ret = range_server_->meta_store_->GetAllRange(&metas);
        ASSERT_TRUE(metas.size() == 1) << metas.size();

        // raft
        auto raft = static_cast<RaftMock *>(range_server_->ranges_.Find(default_range_id + 2)->raft_.get());
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_.Find(default_range_id + 2)->is_leader_ = true;

    }

    { // put data

        genKvNonUniqueIndexKeyData_2_col_have_null(mp_kv);

        for (auto kv : mp_kv) {
            // put data to db
            dspb::RangeRequest req;
            req.mutable_header()->set_range_id(default_range_id+2);
            req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
            req.mutable_header()->mutable_range_epoch()->set_version(1);
            req.mutable_kv_put()->set_key(kv.first);
            req.mutable_kv_put()->set_value(kv.second);

            dspb::RangeResponse resp;
            auto s = testRangeReq(req, resp);
            ASSERT_TRUE(s.ok()) << s.ToString();
            ASSERT_FALSE(resp.header().has_error());
        }

    }

    {   
        dspb::RangeRequest req;
        auto header = req.mutable_header();
        header->set_range_id(default_range_id+2);
        header->mutable_range_epoch()->set_conf_ver(1);
        header->mutable_range_epoch()->set_version(1);

        auto idxStatsReq = req.mutable_index_stats();
        
        idxStatsReq->set_bucket_max(3);
        
        idxStatsReq->add_columns_info()->CopyFrom(ptr_table_account->GetColumnInfo("name"));
        idxStatsReq->add_columns_info()->CopyFrom(ptr_table_account->GetColumnInfo("balance"));
        idxStatsReq->set_cmsketch_depth(4);
        idxStatsReq->set_cmsketch_width(32);
        idxStatsReq->set_unique(false);
        idxStatsReq->mutable_range()->set_start_key(start_key);
        idxStatsReq->mutable_range()->set_end_key(end_key);

        dspb::RangeResponse resp;
        auto s = testRangeReq(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error());
        auto idxResp = resp.idx_stats();
        // std::cout << "idxResp:\n" << idxResp.DebugString() << std::endl;
        auto hist = idxResp.hist();
        ASSERT_EQ(hist.num_dist(), (int64_t)1);
        ASSERT_EQ(hist.buckets_size(), 1);
        ASSERT_EQ(idxResp.null_count(), (uint64_t)0);
        auto rows = idxResp.cms().rows();
        ASSERT_EQ(rows.size(), 4);
        for (int i = 0; i < 4; ++i) {
            uint32_t sum = 0;
            for (auto val : rows[i].counters()) {
                sum += val;
            }
            // std::cout << "rows[" << i
            //           << "] sum:" << sum
            //           << std::endl;
            ASSERT_EQ(sum, (uint32_t)default_table_data_count);
        }
    }
}
