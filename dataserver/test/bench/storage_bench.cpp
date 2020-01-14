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

#include "base/byte_order.h"
#include "base/util.h"
#include "base/status.h"

#include "storage/store.h"
#include "storage/internal_tag.pb.h"

#include "common/server_config.h"
#include "common/ds_encoding.h"

#include "db/db_manager.h"
#include "db/db.h"

#include "basepb/basepb.pb.h"
#include "basepb/metapb.pb.h"
#include "dspb/txn.pb.h"
#include "dspb/processorpb.pb.h"


#include <sstream>
#include <iostream>
#include <chrono>
#include <iomanip>

#include "../helper/helper_util.h"

using namespace jim;
using namespace jim::test;
using namespace jim::test::helper;
using namespace jim::ds;
using namespace jim::ds::db;
using namespace jim::ds::storage;

/*
 *
 * db
 *      db_id 1
 *      db_name "db_1"
 *
 * table
 *      table_id 10000
 *      table_name "table_10000"
 *
 * Table Info
 * ====================================
 * | t_bigint | t_smallint | t_varcahr|
 * |  1000001 |      1     |    "111" |
 * ...
 *
 * */


static const int db_id = 1;
static const std::string db_name = "db_1";

static const int table_id = 10000;
static const std::string table_name = "table_10000";

static const int range_id = 1234;

void make_table(basepb::TableInfo& t)
{
    t.set_name(table_name);
    t.set_db_id(db_id);
    t.set_id(table_id);
    t.set_comment("this is table.");

    auto col_t_bigint = t.add_columns();
    col_t_bigint->set_name("t_bigint");
    col_t_bigint->set_id(1);
    auto sql_type_bigint = col_t_bigint->mutable_sql_type();
    sql_type_bigint->set_type(basepb::DataType::BigInt);
    col_t_bigint->set_primary(true);

    auto col_t_smallint = t.add_columns();
    col_t_smallint->set_name("t_smallint");
    col_t_smallint->set_id(2);
    auto sql_type_smallint = col_t_smallint->mutable_sql_type();
    sql_type_smallint ->set_type(basepb::DataType::SmallInt);

    auto col_t_varchar = t.add_columns();
    col_t_varchar->set_name("t_varchar");
    col_t_varchar->set_id(3);
    auto sql_type_varchar = col_t_varchar->mutable_sql_type();
    sql_type_varchar->set_type(basepb::DataType::Varchar);


    auto index_pri = t.add_indices();
    index_pri->set_id(1);
    index_pri->set_table_id(table_id);
    index_pri->set_name("index_1");
    index_pri->set_unique(true);
    index_pri->set_primary(true);
    index_pri->add_columns(1);

    t.set_type(basepb::StoreType::Store_Hot);
}

void make_server_config()
{

    std::string path = "/tmp/jim_store_test_";

    ds_config.engine_type = EngineType::kMassTree;
    ds_config.meta_path = path + "/meta_data";

    ds_config.masstree_config.data_path = path + "/data";
    ds_config.masstree_config.wal_disabled = true;
    ds_config.masstree_config.rcu_interval_ms = 1000;
    ds_config.masstree_config.checkpoint_opt.disabled = true;
    ds_config.masstree_config.checkpoint_opt.work_threads = 1;
    ds_config.masstree_config.checkpoint_opt.checksum = false;
    ds_config.masstree_config.checkpoint_opt.max_history = 1;
    ds_config.masstree_config.checkpoint_opt.threshold_bytes = 64*1024*1024;

    ds_config.logger_config.path = path + "/log";
    ds_config.logger_config.name = "tmp_bench.log";
    ds_config.logger_config.level = "error";

    LoggerInit(ds_config.logger_config);
}

void make_range( const basepb::TableInfo & t, basepb::Range & r)
{

    r.set_id(range_id);
    EncodeKeyPrefix(r.mutable_start_key(), table_id);
    EncodeKeyPrefix(r.mutable_end_key(), table_id+1);

    auto range_epoch = r.mutable_range_epoch();
    range_epoch->set_conf_ver(1);
    range_epoch->set_version(1);

    r.set_db_id(db_id);
    r.set_table_id(table_id);

    auto index_v1 = t.indices(0);

    auto ks = r.mutable_key_schema();


    for (auto & col_id: index_v1.columns()) {
        for (auto col : t.columns()) {
            if (col_id == col.id()) {
                auto key_cols = ks->add_key_cols();
                key_cols->set_id(col.id());
                key_cols->set_type(col.sql_type().type());
                key_cols->set_unsigned_(col.sql_type().unsigned_());
            }
        }
    }

    r.set_range_type(basepb::RangeType::RNG_Data);
    r.set_store_type(basepb::StoreType::Store_Hot);
}

void make_selectflow_requet_table_read( const basepb::TableInfo &t, const basepb::Range &r, dspb::SelectFlowRequest &req)
{
    auto table_read_processor = req.add_processors();
    table_read_processor->set_type(dspb::ProcessorType::TABLE_READ_TYPE);
    auto table_read = table_read_processor->mutable_table_read();
    auto col_t_bigint = table_read->add_columns();
    col_t_bigint->set_id(r.key_schema().key_cols(0).id());
    col_t_bigint->set_typ(r.key_schema().key_cols(0).type());
    col_t_bigint->set_unsigned_(r.key_schema().key_cols(0).unsigned_());

    table_read->set_type(dspb::KeyType::KEYS_RANGE_TYPE);
    auto t_range = table_read->mutable_range();
    t_range->set_start_key(r.start_key());
    t_range->set_end_key(r.end_key());

    req.add_output_offsets(0);
    req.set_gather_trace(true);
}

void make_selectflow_requet_count_1(const basepb::TableInfo &t, const basepb::Range &r, dspb::SelectFlowRequest &req)
{
    auto table_read_processor = req.add_processors();
    table_read_processor->set_type(dspb::ProcessorType::TABLE_READ_TYPE);
    auto table_read = table_read_processor->mutable_table_read();

//    auto col_t_bigint = table_read->add_columns();
//    col_t_bigint->set_id(r.key_schema().key_cols(0).id());
//    col_t_bigint->set_typ(r.key_schema().key_cols(0).sql_type().type());
//    col_t_bigint->set_unsigned_(r.key_schema().key_cols(0).sql_type().unsigned_());

    table_read->set_type(dspb::KeyType::KEYS_RANGE_TYPE);
    auto t_range = table_read->mutable_range();
    t_range->set_start_key(r.start_key());
    t_range->set_end_key(r.end_key());

    auto aggregation_processor = req.add_processors();
    aggregation_processor->set_type(dspb::ProcessorType::AGGREGATION_TYPE);
    auto aggregation = aggregation_processor->mutable_aggregation();

    auto func = aggregation->add_func();
    func->set_expr_type(dspb::ExprType::Count);
    auto func_child = func->add_child();
    func_child->set_expr_type(dspb::ExprType::Const_Int);
    auto value = std::to_string(1);
    func_child->set_value(value);

//    req.add_output_offsets(0);
    req.set_gather_trace(true);
}

void make_selectflow_requet_count_col(const basepb::TableInfo &t, const basepb::Range &r, dspb::SelectFlowRequest &req)
{
    auto table_read_processor = req.add_processors();
    table_read_processor->set_type(dspb::ProcessorType::TABLE_READ_TYPE);
    auto table_read = table_read_processor->mutable_table_read();
    auto col_t_bigint = table_read->add_columns();
    col_t_bigint->set_id(r.key_schema().key_cols(0).id());
    col_t_bigint->set_typ(r.key_schema().key_cols(0).type());
    col_t_bigint->set_unsigned_(r.key_schema().key_cols(0).unsigned_());

    table_read->set_type(dspb::KeyType::KEYS_RANGE_TYPE);
    auto t_range = table_read->mutable_range();
    t_range->set_start_key(r.start_key());
    t_range->set_end_key(r.end_key());

    auto aggregation_processor = req.add_processors();
    aggregation_processor->set_type(dspb::ProcessorType::AGGREGATION_TYPE);
    auto aggregation = aggregation_processor->mutable_aggregation();

    auto func = aggregation->add_func();
    func->set_expr_type(dspb::ExprType::Count);
    auto func_child = func->add_child();
    func_child->set_expr_type(dspb::ExprType::Column);

    auto col = func_child->mutable_column();
    col->set_id(r.key_schema().key_cols(0).id());
    col->set_typ(r.key_schema().key_cols(0).type());
    col->set_unsigned_(r.key_schema().key_cols(0).unsigned_());

    req.add_output_offsets(0);
    req.set_gather_trace(true);
}

void make_selectflow_requet_sum_col(const basepb::TableInfo &t, const basepb::Range &r, dspb::SelectFlowRequest &req)
{
    auto table_read_processor = req.add_processors();
    table_read_processor->set_type(dspb::ProcessorType::TABLE_READ_TYPE);
    auto table_read = table_read_processor->mutable_table_read();
    auto col_t_bigint = table_read->add_columns();
    col_t_bigint->set_id(r.key_schema().key_cols(0).id());
    col_t_bigint->set_typ(r.key_schema().key_cols(0).type());
    col_t_bigint->set_unsigned_(r.key_schema().key_cols(0).unsigned_());

    table_read->set_type(dspb::KeyType::KEYS_RANGE_TYPE);
    auto t_range = table_read->mutable_range();
    t_range->set_start_key(r.start_key());
    t_range->set_end_key(r.end_key());

    auto aggregation_processor = req.add_processors();
    aggregation_processor->set_type(dspb::ProcessorType::AGGREGATION_TYPE);
    auto aggregation = aggregation_processor->mutable_aggregation();

    auto func = aggregation->add_func();
    func->set_expr_type(dspb::ExprType::Sum);
    auto func_child = func->add_child();
    func_child->set_expr_type(dspb::ExprType::Column);

    auto col = func_child->mutable_column();
    col->set_id(r.key_schema().key_cols(0).id());
    col->set_typ(r.key_schema().key_cols(0).type());
    col->set_unsigned_(r.key_schema().key_cols(0).unsigned_());

    req.add_output_offsets(0);
    req.set_gather_trace(true);
}

void make_selectflow_requet_orderby_col(const basepb::TableInfo &t, const basepb::Range &r, dspb::SelectFlowRequest &req)
{
    auto table_read_processor = req.add_processors();
    table_read_processor->set_type(dspb::ProcessorType::TABLE_READ_TYPE);
    auto table_read = table_read_processor->mutable_table_read();
    auto col_t_bigint = table_read->add_columns();
    col_t_bigint->set_id(r.key_schema().key_cols(0).id());
    col_t_bigint->set_typ(r.key_schema().key_cols(0).type());
    col_t_bigint->set_unsigned_(r.key_schema().key_cols(0).unsigned_());

    table_read->set_type(dspb::KeyType::KEYS_RANGE_TYPE);
    auto t_range = table_read->mutable_range();
    t_range->set_start_key(r.start_key());
    t_range->set_end_key(r.end_key());

    auto ordering_processor = req.add_processors();
    ordering_processor->set_type(dspb::ProcessorType::ORDER_BY_TYPE);
    auto ordering = ordering_processor->mutable_ordering();

    auto ordering_col = ordering->add_columns();
    auto expr = ordering_col->mutable_expr();
    expr->set_expr_type(dspb::ExprType::Column);
    auto expr_col = expr->mutable_column();
    expr_col->set_id(r.key_schema().key_cols(0).id());
    expr_col->set_typ(r.key_schema().key_cols(0).type());
    expr_col->set_unsigned_(r.key_schema().key_cols(0).unsigned_());

    ordering_col->set_asc(true);
    ordering->set_count(10000);

    req.add_output_offsets(0);
    req.set_gather_trace(true);
}

void make_selectflow_requet_limit(const basepb::TableInfo &t, const basepb::Range &r, dspb::SelectFlowRequest &req)
{
    auto table_read_processor = req.add_processors();
    table_read_processor->set_type(dspb::ProcessorType::TABLE_READ_TYPE);
    auto table_read = table_read_processor->mutable_table_read();
    auto col_t_bigint = table_read->add_columns();
    col_t_bigint->set_id(r.key_schema().key_cols(0).id());
    col_t_bigint->set_typ(r.key_schema().key_cols(0).type());
    col_t_bigint->set_unsigned_(r.key_schema().key_cols(0).unsigned_());

    table_read->set_type(dspb::KeyType::KEYS_RANGE_TYPE);
    auto t_range = table_read->mutable_range();
    t_range->set_start_key(r.start_key());
    t_range->set_end_key(r.end_key());

    auto limit_processor = req.add_processors();
    limit_processor->set_type(dspb::ProcessorType::LIMIT_TYPE);
    auto limit = limit_processor->mutable_limit();

    limit->set_offset(1);
    limit->set_count(5);

    req.add_output_offsets(0);
    req.set_gather_trace(true);
}

void make_selectflow_requet_distinct(const basepb::TableInfo &t, const basepb::Range &r, dspb::SelectFlowRequest &req)
{
    auto table_read_processor = req.add_processors();
    table_read_processor->set_type(dspb::ProcessorType::TABLE_READ_TYPE);
    auto table_read = table_read_processor->mutable_table_read();
    auto col_t_smallint = table_read->add_columns();
    col_t_smallint->set_id(t.columns(1).id());
    col_t_smallint->set_typ(t.columns(1).sql_type().type());
    col_t_smallint->set_unsigned_(t.columns(1).sql_type().unsigned_());

    table_read->set_type(dspb::KeyType::KEYS_RANGE_TYPE);
    auto t_range = table_read->mutable_range();
    t_range->set_start_key(r.start_key());
    t_range->set_end_key(r.end_key());

    auto aggregation_processor = req.add_processors();
    aggregation_processor->set_type(dspb::ProcessorType::AGGREGATION_TYPE);
    auto aggregation = aggregation_processor->mutable_aggregation();

    auto func = aggregation->add_func();
    func->set_expr_type(dspb::ExprType::Distinct);
    auto func_child=func->add_child();

    func_child->set_expr_type(dspb::ExprType::Column);
    auto func_col = func_child->mutable_column();
    func_col->set_id(t.columns(1).id());
    func_col->set_typ(t.columns(1).sql_type().type());
    func_col->set_unsigned_(t.columns(1).sql_type().unsigned_());

    auto group_by = aggregation->add_group_by();
    group_by->set_expr_type(dspb::ExprType::Column);
    auto group_by_col = group_by->mutable_column();
    group_by_col->set_id(t.columns(1).id());
    group_by_col->set_typ(t.columns(1).sql_type().type());
    group_by_col->set_unsigned_(t.columns(1).sql_type().unsigned_());

    req.add_output_offsets(0);
    req.set_gather_trace(true);
}

// db value:
// | user_value | tag_value | tag_size(2 byte) | version(8 byte) |
//
Status assembleValue(std::string& db_value, const std::string& user_value,
                     uint64_t version, const pb::InternalTag* tag) {
    std::string tag_value;
    uint16_t tag_size = 0;
    if (tag != nullptr) {
        if (!tag->SerializeToString(&tag_value)) {
            return Status(Status::kCorruption, "serialize internal tag", "");
        }
        if (tag_value.size() > static_cast<size_t>(std::numeric_limits<uint16_t>::max())) {
            return Status(Status::kInvalidArgument, "tag too large", std::to_string(tag_value.size()));
        }
        tag_size = static_cast<uint16_t>(tag_value.size());
    }

    db_value.resize(user_value.size() + tag_size + sizeof(tag_size) + sizeof(version));
    auto p = const_cast<char*>(db_value.data());
    // append user value
    if (!user_value.empty()) {
        memcpy(p, user_value.c_str(), user_value.size());
        p += user_value.size();
    }
    // append tag value
    if (!tag_value.empty()) {
        memcpy(p, tag_value.c_str(), tag_size);
        p += tag_size;
    }
    // append tag length
    *((uint16_t *)p) = htobe16(tag_size);;
    p += sizeof(tag_size);
    // append version
    *((uint64_t *)p) = htobe64(version);;

    return Status(Status::OK());
}

Status disassembleValue(const std::string& db_value, size_t& user_value_size,
                        uint64_t& version, pb::InternalTag* tag) {
    uint16_t tag_size = 0;
    if (db_value.size() < sizeof(tag_size) + sizeof(version)) {
        return Status(Status::kInvalidArgument, "insufficient length", std::to_string(db_value.size()));
    }
    auto p = db_value.c_str() + db_value.size() - sizeof(version);
    version = be64toh(*((uint64_t *)p));
    p -= sizeof(tag_size);
    tag_size = be16toh(*((uint16_t *)p));

    user_value_size = db_value.size() - sizeof(version)- sizeof(tag_size) - tag_size;
    if (tag_size > 0 && tag != nullptr) {
        if (!tag->ParseFromArray(db_value.c_str() + user_value_size, tag_size)) {
            return Status(Status::kCorruption, "parse internal tag", EncodeToHexString(db_value));
        }
    }
    return Status::OK();
}

bool make_data(const basepb::TableInfo & t, std::unique_ptr<jim::ds::storage::Store>& store )
{
    const auto num_start = 1000000;
    const auto num_limit = 2000000;
    for ( auto i = num_start; i < num_limit; i++) {
        auto t_bigint = std::to_string(i);
        auto t_smallint = std::to_string(i % 1000);
        auto t_varchar = std::string("num") + std::to_string(i);

        std::string kv_k = "";
        std::string kv_v = "";

        EncodeKeyPrefix(&kv_k, table_id);
        EncodeKvKey(&kv_k, t.columns(0), std::move(t_bigint));

        std::string tmp_kv_v = "";
        EncodeKvValue(&tmp_kv_v, t.columns(1), std::move(t_smallint));
        EncodeKvValue(&tmp_kv_v, t.columns(2), std::move(t_varchar));

        assembleValue(kv_v, tmp_kv_v, i, nullptr);

        auto s = store->Put(kv_k, kv_v, i);
        if (!s.ok()) {
            std::cout << s.ToString() << std::endl;
            return false;
        }
    }
    return true;
}

typedef enum {
    TABLE_READ,
    COUNT_1,
    COUNT_COL,
    SUM_COL,
    ORDERBY_COL,
    LIMIT,
    DISTINCT,
} bench_type;


int main(int argc, char **argv) {

    if (argc < 2 ) {
        std::cerr << "========================" << std::endl;
        std::cerr << "./storage_bench table_read" << std::endl;
        std::cerr << "./storage_bench count_1" << std::endl;
        std::cerr << "./storage_bench count_col"<< std::endl;
        std::cerr << "./storage_bench sum_col"<< std::endl;
        std::cerr << "./storage_bench orderby_col"<< std::endl;
        std::cerr << "./storage_bench limit"<< std::endl;
        std::cerr << "./storage_bench distinct"<< std::endl;
        std::cerr << "========================" << std::endl;
        return 0;
    }

    bench_type tp;

    if ( std::string(argv[1]) == std::string("table_read")) {
        tp = TABLE_READ;
    } else if ( std::string(argv[1]) == std::string("count_1")) {
        tp = COUNT_1;
    } else if ( std::string(argv[1]) == std::string("count_col")) {
        tp = COUNT_COL;
    } else if ( std::string(argv[1]) == std::string("sum_col")) {
        tp = SUM_COL;
    } else if ( std::string(argv[1]) == std::string("orderby_col")) {
        tp = ORDERBY_COL;
    } else if ( std::string(argv[1]) == std::string("limit")) {
        tp = LIMIT;
    } else if ( std::string(argv[1]) == std::string("distinct")) {
        tp = DISTINCT;
    } else {
        tp = COUNT_1;
    }

    make_server_config();

    // table
    basepb::TableInfo t;
    make_table(t);

    // range
    basepb::Range r;
    make_range(t, r);

    std::unique_ptr<DBManager> db_manager = nullptr;
    std::unique_ptr<DB> db = nullptr;
    std::unique_ptr<jim::ds::storage::Store> store = nullptr;

    auto s = jim::ds::db::NewDBManager(db_manager);
    if (!s.ok()) {
        std::cout << s.ToString() << std::endl;
        return -1;
    }

    s = db_manager->CreateDB(r.id(), r.start_key(), r.end_key(), db);
    if (!s.ok()) {
        std::cout << s.ToString() << std::endl;
        return -1;
    }

    store.reset(new jim::ds::storage::Store(r, std::move(db)));

    auto b = make_data(t, store);
    if (!b) {
        std::cout << "make data error." << std::endl;
        return 0;
    }

    dspb::SelectFlowRequest req;
    switch (tp) {
        case TABLE_READ: {
            std::cout << "TABLE_READ bench test." << std::endl;
            make_selectflow_requet_table_read( t, r, req);
            break;
        }
        case COUNT_1: {
            std::cout << "COUNT_1 bench test." << std::endl;
            make_selectflow_requet_count_1( t, r, req);
            break;
        }
        case COUNT_COL: {
            std::cout << "COUNT_COL bench test." << std::endl;
            make_selectflow_requet_count_col( t, r, req);
            break;
        }
        case SUM_COL: {
            std::cout << "SUM_COL bench test." << std::endl;
            make_selectflow_requet_sum_col( t, r, req);
            break;
        }
        case ORDERBY_COL: {
            std::cout << "ORDERBY_COL bench test." << std::endl;
            make_selectflow_requet_orderby_col( t, r, req);
            break;
        }
        case LIMIT: {
            std::cout << "limit bench test." << std::endl;
            make_selectflow_requet_limit( t, r, req);
            break;
        }
        case DISTINCT: {
            std::cout << "distinct bench test." << std::endl;
            make_selectflow_requet_distinct( t, r, req);
            break;
        }
        default: {
            break;
        }
    }
    std::cout << "========================" << std::endl;
    std::cout<< "info req:" << req.DebugString() << std::endl;
    std::cout << "========================" << std::endl;
    dspb::SelectFlowResponse resp;
    s = store->TxnSelectFlow(req, &resp);
//    std::cout<< "info resp:" << resp.DebugString() << std::endl;
    std::cout << "info resp.code():" << resp.code() << std::endl;
    std::cout << "info resp.rows_size():" << resp.rows_size() << std::endl;
    if (resp.rows_size() < 10) {
        for (auto & ro : resp.rows()) {
            std::cout << ro.DebugString() << std::endl;
        }
    }
    std::cout << "info resp.last_key():" << EncodeToHexString(resp.last_key()) << std::endl;
    std::cout << "info resp.traces_size(): " << resp.traces_size() << std::endl;
    if (resp.traces_size() < 1002) {
        for (auto & tr : resp.traces()) {
            std::cout << tr.DebugString() << std::endl;
        }
    }
    std::cout << "========================" << std::endl;
    if (!s.ok()) {
        std::cout << s.ToString() << std::endl;
    }

    store.reset();
    db.reset();
    db_manager.reset();

    return 0;
}



