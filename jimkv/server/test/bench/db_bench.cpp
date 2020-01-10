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

#include <base/util.h>
#include <base/status.h>
#include "storage/store.h"
#include "../helper/helper_util.h"
#include "base/fs_util.h"


#include "db/db_manager.h"
#include "db/db.h"
#include "db/mass_tree_impl/manager_impl.h"
#include "db/mass_tree_impl/db_impl.h"
#include "db/rocksdb_impl/manager_impl.h"

#include <sstream>
#include <iostream>
#include <chrono>
#include <iomanip>

using namespace jimkv;
using namespace jimkv::test;
using namespace jimkv::test::helper;
using namespace jimkv::ds;
using namespace jimkv::ds::db;

int main(int argc, char **argv) {

    if ( argc < 2 ){
        std::cerr << "./db_bench rocksdb" << std::endl;
        std::cerr << "or" << std::endl;
        std::cerr << "./db_bench masstree" << std::endl;
        exit(0);
    }

    if ( std::string(argv[1]) == "rocksdb" ) {
        ::setenv("ENGINE", "rocksdb", 1);
    } else {
        ::setenv("ENGINE", "masstree", 1);
    }

    int test_num = 1000000;
    if ( argc >= 3 ) {
        auto tmp = std::stol(argv[2]);
        if (tmp > test_num) {
            test_num  = tmp;
        }
    }

    const uint64_t range_id = 1234;
    std::string start_key, end_key;

    std::string data_path;
    std::chrono::system_clock::time_point time_begin;
    uint64_t time_processed_ns = 0;

    std::unique_ptr<DBManager> manager = nullptr;
    std::unique_ptr<DB> db = nullptr;
    uint64_t index_id = 5678;

    char path[] = "/tmp/jimkv_dsdb_test_XXXXXX";
    char *tmp = mkdtemp(path);
    data_path = tmp;

    InitLog();

    auto engine = ::getenv("ENGINE");

    if (engine != nullptr && strcmp(engine, "rocksdb") == 0) {
        std::cout << "Engine: rocksdb ...." << std::endl;
        RocksDBConfig opt;
        opt.path = path;
        opt.enable_txn_cache = false;
        opt.default_cf_config.write_buffer_size = 3UL*1024*1024*1024;
        opt.txn_cf_config.write_buffer_size = 2UL*1024*1024*1024;
        opt.meta_cf_config.write_buffer_size = 2UL*1024*1024*1024;
        manager.reset(new RocksDBManager(opt));
    } else {
        std::cout << "Engine: masstree...." << std::endl;
        MasstreeOptions opt;
        opt.rcu_interval_ms = 2000;
        opt.wal_disabled = false;
        opt.data_path = path;
        opt.rcu_interval_ms = 1000;
        opt.checkpoint_opt.disabled = false;
        opt.checkpoint_opt.checksum = true;
        opt.checkpoint_opt.work_threads = 1;
        opt.checkpoint_opt.threshold_bytes = 4096;
        opt.checkpoint_opt.max_history = 2;
        manager.reset(new MasstreeDBManager(opt));
    }
    auto s = manager->Init();
    if (!s.ok()) {
        return 0;
    }

    /* data format description
     * =======================================
     * | prefix  |   area(6)    |   id       |
     * |  0x01   |   000000     |   100      |
     */
    start_key = '\x01';
    end_key = '\x02';

    s = manager->CreateDB(range_id, start_key, end_key, db);
    if (!s.ok()) {
        return 0;
    }

    if (engine != nullptr && strcmp(engine, "rocksdb") == 0) {
        ;
    } else {
        auto p = dynamic_cast<MasstreeDBImpl*>(db.get());
        if (p) {
            p->TEST_Run_Checkpoint();
            p->TEST_Disable_Checkpoint();
        }
    }


    std::string kv_k;
    std::string kv_value;
    int area = -1;
    std::string kv_v = jimkv::randomString(64);
    const auto start_num =1000000;
    const auto end_num = start_num + test_num ;
    for (int i = start_num; i < end_num; i++) {

        if (i%1000 == 0) {
            area++;
        }

        std::ostringstream os;
        os << '\x01' << std::setfill('0') << std::setw(6) << area << i;

        kv_k = os.str();
        kv_v = jimkv::randomString(64);

//        std::cout << "put, kv_k:" << EncodeToHex(kv_k) << ",kv_v:" << EncodeToHex(kv_v) << std::endl;

        time_begin = std::chrono::system_clock::now();
        auto s = db->Put(kv_k, kv_v, i);
        time_processed_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now() - time_begin).count();
        if (!s.ok()) {
            std::cout << "db->Put error." << std::endl;
            return 0;
        }
    }
    std::cout << "db->Put, num:" << end_num - start_num<< ",cost(ns):" << time_processed_ns << std::endl;

    time_processed_ns = 0;
    area = -1;


    std::vector<std::string> str_vec;
    str_vec.reserve(end_num - start_num);

    for (int i = start_num ; i < end_num; i++) {

        if (i%1000 == 0) {
            area++;
        }

        std::ostringstream os;
        os << '\x01' << std::setfill('0') << std::setw(6) << area << i;



        kv_k = os.str();
        str_vec.push_back(kv_k);

    }

    time_begin = std::chrono::system_clock::now();
    for (auto &tk : str_vec) {
        auto s = db->Get(kv_k, kv_v);
        if (!s.ok()) {
            std::cout << "db->Get error." << s.ToString() << std::endl;
            return 0;
        }
//        std::cout << "get, kv_k:" << EncodeToHex(kv_k) << ",kv_v:" << EncodeToHex(kv_v) << std::endl;
    }
    time_processed_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now() - time_begin).count();
    std::cout << "db->Get, num:" << end_num - start_num << ",cost(ns):" << time_processed_ns << std::endl;

    std::ostringstream os1, os2;
    os1 << '\x01';
    os2 << '\x02';

    std::string scope_start = os1.str();
    std::string scope_end = os2.str();
    time_processed_ns = 0;
    uint64_t row_num = 0;
    auto itr = db->NewIterator( scope_start, scope_end);
    time_begin = std::chrono::system_clock::now();

    while(itr->Valid()) {
//        std::cout << "get, kv_k: " << EncodeToHex(itr->Key()) << ",kv_v:" << EncodeToHex(itr->Value()) << std::endl;
        row_num++;
        itr->Next();
    }
    time_processed_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now() - time_begin).count();

    std::cout  << "db->Iterator,"
                <<  "scop:< " << EncodeToHex(scope_start) << "," << EncodeToHex(scope_end) << ">"
             <<  "num:" << row_num << ",cost(ns):" << time_processed_ns << std::endl;
    if (!itr->status().ok()) {
        std::cout << "itr->stats" << itr->status().ToString() << std::endl;
        return 0;
    }
    itr.reset();

    for (auto j = 0; j < test_num/1000/100; j++){
        std::ostringstream os1;
        os1 << '\x01' << std::setfill('0') << std::setw(6) << j*100;
        auto scope_start = os1.str();

        std::ostringstream os2;
        os2 << '\x01' << std::setfill('0') << std::setw(6) <<(j+1)*100;
        auto scope_end = os2.str();

        auto itr_j = db->NewIterator(scope_start, scope_end);
        time_processed_ns = 0;
        row_num = 0;
        time_begin = std::chrono::system_clock::now();
        while(itr_j->Valid()) {
//            std::cout << "get, j:" << j << ", kv_k: " << EncodeToHex(itr_j->Key()) << ",kv_v:" << EncodeToHex(itr_j->Value()) << std::endl;
            row_num++;
            itr_j->Next();
        }
        time_processed_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now() - time_begin).count();
        std::cout  << "db->Iterator,j:" << j
                   <<  ",scop:< " << EncodeToHex(scope_start) << "," << EncodeToHex(scope_end) << " >"
                   <<  "num:" << row_num << ",cost(ns):" << time_processed_ns << std::endl;
        if (!itr_j->status().ok()) {
            std::cout << "itr_j->stats" << itr_j->status().ToString() << std::endl;
            return 0;
        }

        itr_j.reset();
    }

    for (auto j = 0; j < test_num/1000/10; j++){
        std::ostringstream os1;
        os1 << '\x01' << std::setfill('0') << std::setw(6) << j*10;
        auto scope_start = os1.str();

        std::ostringstream os2;
        os2 << '\x01' << std::setfill('0') << std::setw(6) <<(j+1)*10;
        auto scope_end = os2.str();

        auto itr_j = db->NewIterator(scope_start, scope_end);
        time_processed_ns = 0;
        row_num = 0;
        time_begin = std::chrono::system_clock::now();
        while(itr_j->Valid()) {
//            std::cout << "get, j:" << j << ", kv_k: " << EncodeToHex(itr_j->Key()) << ",kv_v:" << EncodeToHex(itr_j->Value()) << std::endl;
            row_num++;
            itr_j->Next();
        }
        time_processed_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now() - time_begin).count();
        std::cout  << "db->Iterator,j:" << j
                   <<  ",scop:< " << EncodeToHex(scope_start) << "," << EncodeToHex(scope_end) << " >"
                   <<  "num:" << row_num << ",cost(ns):" << time_processed_ns << std::endl;
        if (!itr_j->status().ok()) {
            std::cout << "itr_j->stats" << itr_j->status().ToString() << std::endl;
            return 0;
        }
        itr_j.reset();
    }

    db.reset();
    manager.reset();
    return 0;
}

