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

_Pragma("once");

#include "common/config_util.h"

namespace jim {

struct RocksDBCFConfig {
    // table options
    size_t block_cache_size = 1024UL * 1048576; // default: 1024MB
    size_t block_size = 16U * 1024; // default: 16K
    bool cache_index_and_filter_blocks = true;
    bool pin_l0_filter_and_index_blocks_in_cache = true;

    // write buffer options
    size_t write_buffer_size = 128UL * 1048576; // default: 128MB
    int max_write_buffer_number = 6;
    int min_write_buffer_number_to_merge = 1;

    // level options
    size_t max_bytes_for_level_base = 256UL * 1048576;
    int max_bytes_for_level_multiplier = 10;
    size_t target_file_size_base = 64UL * 1048576;
    int target_file_size_multiplier = 1;

    int level0_file_num_compaction_trigger = 8;
    int level0_slowdown_writes_trigger = 24;
    int level0_stop_writes_trigger = 36;

    bool disable_auto_compactions = false;
    // TODO:
    // int compression = 0;

    static RocksDBCFConfig NewDefaultCFConfig();
    static RocksDBCFConfig NewTxnCFConfig();
    static RocksDBCFConfig NewMetaCFConfig();

    bool Load(const INIReader& reader, const char* section);
};


struct RocksDBConfig {
    std::string path;

    bool enable_txn_cache = true;

    // db options
    size_t row_cache_size = 0;
    int max_open_files = 1024;

    size_t bytes_per_sync = 0;
    bool read_checksum = true;
    bool disable_wal = false;
    bool unordered_write = true;
    int ttl = 0;

    int max_background_jobs = 8;
    uint32_t max_subcompactions = 1;
    size_t background_rate_limit = 0;

    bool enable_stats = true;
    unsigned int stats_dump_period_sec = 60 * 5;
    bool enable_debug_log = false;

    RocksDBCFConfig default_cf_config = RocksDBCFConfig::NewDefaultCFConfig();
    RocksDBCFConfig txn_cf_config = RocksDBCFConfig::NewTxnCFConfig();
    RocksDBCFConfig meta_cf_config = RocksDBCFConfig::NewMetaCFConfig();

    bool Load(const INIReader& reader, const std::string& base_data_path);
    void Print() const;
};

} // namespace jim
