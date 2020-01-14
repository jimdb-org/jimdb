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

#include "rocksdb_config.h"

namespace jim {

static const char *kDefaultCFSection = "rocksdbcf.default";
static const char *kTxnCFSection = "rocksdbcf.txn";
static const char *kMetaCFSection = "rocksdbcf.meta";

RocksDBCFConfig RocksDBCFConfig::NewDefaultCFConfig() {
    RocksDBCFConfig cfg;
    return cfg;
}

RocksDBCFConfig RocksDBCFConfig::NewTxnCFConfig() {
    RocksDBCFConfig cfg;
    return cfg;
}

RocksDBCFConfig RocksDBCFConfig::NewMetaCFConfig() {
    RocksDBCFConfig cfg;
    // disable cache
    cfg.cache_index_and_filter_blocks = false;
    cfg.pin_l0_filter_and_index_blocks_in_cache = false;
    cfg.block_cache_size = 0;

    // small write buffer
    cfg.max_write_buffer_number = 2;
    cfg.write_buffer_size = 4UL * 1048576; // default: 128MB
    cfg.target_file_size_base = 4UL * 1048576;

    cfg.level0_file_num_compaction_trigger = 4;
    return cfg;
}

bool RocksDBCFConfig::Load(const INIReader& reader, const char* section) {
    bool ret = true;

    std::tie(block_cache_size, ret) =
            loadNeBytesValue(reader, section, "block_cache_size", block_cache_size);
    if (!ret) return false;

    std::tie(block_size, ret)  = loadNeBytesValue(reader, section, "block_size", block_size);
    if (!ret) return false;

    cache_index_and_filter_blocks =
            reader.GetBoolean(section, "cache_index_and_filter_blocks", cache_index_and_filter_blocks);

    pin_l0_filter_and_index_blocks_in_cache =
            reader.GetBoolean(section, "pin_l0_filter_and_index_blocks_in_cache", pin_l0_filter_and_index_blocks_in_cache);

    std::tie(write_buffer_size, ret) =
            loadNeBytesValue(reader, section, "write_buffer_size", write_buffer_size);
    if (!ret) return false;

    std::tie(max_write_buffer_number, ret)  =
            loadIntegerAtLeast(reader, section, "max_write_buffer_number", max_write_buffer_number, 2);
    if (!ret) return false;

    std::tie(min_write_buffer_number_to_merge, ret)  =
            loadIntegerAtLeast(reader, section, "min_write_buffer_number_to_merge", min_write_buffer_number_to_merge, 1);
    if (!ret) return false;

    std::tie(max_bytes_for_level_base, ret) =
            loadNeBytesValue(reader, section, "max_bytes_for_level_base", max_bytes_for_level_base);
    if (!ret) return false;

    std::tie(max_bytes_for_level_multiplier, ret) =
            loadIntegerAtLeast(reader, section, "max_bytes_for_level_multiplier", max_bytes_for_level_multiplier, 3);
    if (!ret) return false;

    std::tie(target_file_size_base, ret) =
            loadNeBytesValue(reader, section, "target_file_size_base", target_file_size_base);
    if (!ret) return false;
    std::tie(target_file_size_multiplier, ret) =
            loadIntegerAtLeast(reader, section, "target_file_size_multiplier", target_file_size_multiplier, 1);
    if (!ret) return false;

    std::tie(level0_file_num_compaction_trigger, ret) =
            loadIntegerAtLeast(reader, section, "level0_file_num_compaction_trigger", level0_file_num_compaction_trigger, 1);
    if (!ret) return false;

    std::tie(level0_slowdown_writes_trigger, ret) =
            loadIntegerAtLeast(reader, section, "level0_slowdown_writes_trigger", level0_slowdown_writes_trigger, 1);
    if (!ret) return false;

    std::tie(level0_stop_writes_trigger, ret) =
            loadIntegerAtLeast(reader, section, "level0_stop_writes_trigger", level0_stop_writes_trigger, 1);
    if (!ret) return false;

    disable_auto_compactions = reader.GetBoolean(section, "disable_auto_compactions", false);

    return ret;
}


bool RocksDBConfig::Load(const INIReader& reader, const std::string& base_data_path) {
    const char *section = "rocksdb";

    path = reader.Get(section, "path", "");
    if (!validateDataPath(base_data_path, kRocksdbPathSuffix, path)) {
        return false;
    }

    enable_txn_cache = reader.GetBoolean(section, "enable_txn_cache", true);
    if (!enable_txn_cache) {
        txn_cf_config.block_cache_size = 1024UL * 1048576;
        txn_cf_config.pin_l0_filter_and_index_blocks_in_cache = true;
        txn_cf_config.cache_index_and_filter_blocks = true;
    }

    bool ret = false;
    std::tie(row_cache_size, ret) =
            loadNeBytesValue(reader, section, "row_cache_size", row_cache_size);
    if (!ret) return false;

    std::tie(max_open_files, ret) =
            loadIntegerAtLeast(reader, section, "max_open_files", max_open_files, 100);
    if (!ret) return false;

    std::tie(bytes_per_sync, ret) =
            loadNeBytesValue(reader, section, "bytes_per_sync", bytes_per_sync);
    if (!ret) return false;


    std::tie(max_background_jobs, ret) =
            loadIntegerAtLeast(reader, section, "max_background_jobs", max_background_jobs, 1);
    if (!ret) return false;

    std::tie(max_subcompactions, ret) =
            loadIntegerAtLeast(reader, section, "max_subcompactions", 1, 1);
    if (!ret) return false;

    std::tie(background_rate_limit, ret) =
            loadNeBytesValue(reader, section, "background_rate_limit", 0);
    if (!ret) return false;

    read_checksum = reader.GetBoolean(section, "read_checksum", true);
    disable_wal = reader.GetBoolean(section, "disable_wal", false);
    unordered_write = reader.GetBoolean(section, "unordered_write", true);

    std::tie(ttl, ret) = loadIntegerAtLeast(reader, section, "ttl", 0, 0);
    if (!ret) return false;
    if (ttl > 0) {
        FLOG_WARN("rocksdb ttl enabled. ttl={}", ttl);
    }

    enable_stats = reader.GetBoolean(section, "enable_stats", true);
    stats_dump_period_sec = reader.GetInteger(section, "stats_dump_period_sec", stats_dump_period_sec);
    enable_debug_log = reader.GetBoolean(section, "enable_debug_log", false);

    if (!default_cf_config.Load(reader, kDefaultCFSection)) {
        return false;
    }
    if (!txn_cf_config.Load(reader, kTxnCFSection)) {
        return false;
    }
    return meta_cf_config.Load(reader, kMetaCFSection);
}

void RocksDBConfig::Print() const {
}

} // namespace jim
