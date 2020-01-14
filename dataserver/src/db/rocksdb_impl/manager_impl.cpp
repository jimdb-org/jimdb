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

#include "manager_impl.h"

#include <sstream>

#include <rocksdb/version.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/table.h>
#include <rocksdb/rate_limiter.h>

#include "base/fs_util.h"
#include "common/masstree_env.h"
#include "db/mass_tree_impl/mass_tree_wrapper.h"

#include "db_impl.h"

namespace jim {
namespace ds {
namespace db {

static const char* kTxnRocksdbCFName = "txn";
static const char* kMetaRocksdbCFName = "meta"; // meta cf to save applied index

RocksDBManager::RocksDBManager(const RocksDBConfig& config) :
    db_path_(config.path),
    config_(config) {
    if (config.enable_txn_cache) {
        txn_cache_ = new MasstreeWrapper();
    }
}

RocksDBManager::~RocksDBManager() {
    for (auto handle: cf_handles_) {
        delete handle;
    }
    delete db_;
    delete txn_cache_;
}


void RocksDBManager::buildDBOptions(const RocksDBConfig& config, rocksdb::DBOptions& db_opt) {
    db_opt.create_if_missing = true;
    db_opt.create_missing_column_families = true;

    // db log level
    db_opt.info_log_level = config.enable_debug_log ? rocksdb::DEBUG_LEVEL : rocksdb::INFO_LEVEL;

    // db stats
    if (config.enable_stats) {
        db_stats_ = rocksdb::CreateDBStatistics();;
        db_opt.statistics = db_stats_;
        db_opt.stats_dump_period_sec = config.stats_dump_period_sec;
    }

    // row_cache
    if (config.row_cache_size > 0){
        row_cache_ = rocksdb::NewLRUCache(config.row_cache_size);;
        db_opt.row_cache = row_cache_;
    }

    db_opt.max_open_files = config.max_open_files;
    db_opt.use_adaptive_mutex = true;
    db_opt.bytes_per_sync = config.bytes_per_sync;
#if (ROCKSDB_MAJOR > 5)
    db_opt.unordered_write = config.unordered_write;
#endif

    if (config.background_rate_limit > 0) {
        db_opt.rate_limiter = std::shared_ptr<rocksdb::RateLimiter>(
                rocksdb::NewGenericRateLimiter(static_cast<int64_t>(config.background_rate_limit)));
    }
    db_opt.max_background_jobs = config.max_background_jobs;
    db_opt.max_subcompactions = config.max_subcompactions;
}

void RocksDBManager::buildCFOptions(const RocksDBCFConfig& config, rocksdb::ColumnFamilyOptions& cf_opt) {
    // table options include block_size, block_cache_size, etc
    rocksdb::BlockBasedTableOptions table_options;
    table_options.block_size = config.block_size;
    if (config.block_cache_size > 0) {
        auto block_cache = rocksdb::NewLRUCache(config.block_cache_size);
        block_caches_.push_back(block_cache);
        table_options.block_cache = block_cache;
    }
    table_options.cache_index_and_filter_blocks = config.cache_index_and_filter_blocks;
    table_options.pin_l0_filter_and_index_blocks_in_cache = config.pin_l0_filter_and_index_blocks_in_cache;
    cf_opt.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

    // memtables
    cf_opt.write_buffer_size = config.write_buffer_size;
    cf_opt.max_write_buffer_number = config.max_write_buffer_number;
    cf_opt.min_write_buffer_number_to_merge = config.min_write_buffer_number_to_merge;

    // level 0 & write pause
    cf_opt.level0_file_num_compaction_trigger = config.level0_file_num_compaction_trigger;
    cf_opt.level0_slowdown_writes_trigger = config.level0_slowdown_writes_trigger;
    cf_opt.level0_stop_writes_trigger = config.level0_stop_writes_trigger;

    // level & sst file size
    cf_opt.max_bytes_for_level_base = config.max_bytes_for_level_base;
    cf_opt.max_bytes_for_level_multiplier = config.max_bytes_for_level_multiplier;
    cf_opt.target_file_size_base = config.target_file_size_base;
    cf_opt.target_file_size_multiplier = config.target_file_size_multiplier;

    // compactions and flushes
    if (config.disable_auto_compactions) {
        cf_opt.disable_auto_compactions = true;
    }
}

Status RocksDBManager::open() {
    std::vector<rocksdb::ColumnFamilyDescriptor> column_families;

    rocksdb::DBOptions db_opt;
    buildDBOptions(config_, db_opt);

    rocksdb::ColumnFamilyOptions default_cf_opt, txn_cf_opt, meta_cf_opt;
    buildCFOptions(config_.default_cf_config, default_cf_opt);
    buildCFOptions(config_.txn_cf_config, txn_cf_opt);
    buildCFOptions(config_.meta_cf_config, meta_cf_opt);

    // default column family
    column_families.emplace_back(rocksdb::kDefaultColumnFamilyName, default_cf_opt);
    column_families.emplace_back(kTxnRocksdbCFName, txn_cf_opt);
    column_families.emplace_back(kMetaRocksdbCFName, meta_cf_opt);

    rocksdb::Status ret;
    ret = rocksdb::DB::Open(db_opt, db_path_, column_families, &cf_handles_, &db_);
    // check open ret
    if (!ret.ok()) {
        return Status(Status::kIOError, "open db", ret.ToString());
    }

    return Status::OK();
}

Status RocksDBManager::Init() {
    if (!MakeDirAll(db_path_, 0755)) {
        return Status(Status::kIOError, "create rocksdb path", strErrno(errno));
    }
    return open();
}

Status RocksDBManager::createDB(uint64_t range_id, const std::string& start_key,
                const std::string& end_key, bool from_split,
                std::unique_ptr<DB>& db) {
    RocksDBOptions db_opt;
    db_opt.id = range_id;
    db_opt.start_key = start_key;
    db_opt.end_key = end_key;
    db_opt.wal_disabled = config_.disable_wal;
    db_opt.db = db_;

    db.reset(new RocksDBImpl(db_opt, this, cf_handles_[0], cf_handles_[1], cf_handles_[2], txn_cache_));

    return db->Open();
}

Status RocksDBManager::CreateDB(uint64_t range_id, const std::string& start_key,
              const std::string& end_key, std::unique_ptr<DB>& db) {
    return createDB(range_id, start_key, end_key, false, db);
}

Status RocksDBManager::CreatSplit(uint64_t range_id, const std::string& start_key,
                  const std::string& end_key, std::unique_ptr<DB>& db) {
    return createDB(range_id, start_key, end_key, true, db);
}

Status RocksDBManager::GetUsage(DBUsage& usage) {
    uint64_t total = 0, available = 0;
    auto ret = GetFileSystemUsage(db_path_.c_str(), &total, &available);
    if (!ret) {
        return Status(Status::kIOError, "collect disk usage", strErrno(errno));
    }

    usage.total_size = total;
    usage.free_size = available;
    usage.used_size = total - available;
    if (total > 0 && available <= total) {
        return Status::OK();
    } else {
        return Status(Status::kInvalid, "db usage", usage.ToString());
    }
}

std::string RocksDBManager::MetricInfo(bool verbose) {
    std::ostringstream ss;

    std::string tr_mem_usage;
    db_->GetProperty("rocksdb.estimate-table-readers-mem", &tr_mem_usage);
    std::string mem_table_usage;
    db_->GetProperty("rocksdb.cur-size-all-mem-tables", &mem_table_usage);

    size_t block_cache_usage = 0, block_cache_pinned = 0;
    for (auto& cache: block_caches_) {
        block_cache_usage += cache->GetUsage();
        block_cache_pinned += cache->GetPinnedUsage();
    }

    ss << "rocksdb memory usages: table-readers=" << tr_mem_usage;
    ss << ", memtables=" << mem_table_usage;
    ss << ", block-cache=" << block_cache_usage;
    ss << ", pinned=" << block_cache_pinned;
    ss << ", row-cache=" << (row_cache_ ? row_cache_->GetUsage() : 0);
    ss << std::endl;

    if (txn_cache_) {
        ss << "txn cache: " << txn_cache_->CollectTreeCouter() << std::endl;
    }

    auto stat = db_stats_;
    if (stat) {
        ss << "rocksdb row-cache stats: hit="  << stat->getAndResetTickerCount(rocksdb::ROW_CACHE_HIT);
        ss << ", miss="  << stat->getAndResetTickerCount(rocksdb::ROW_CACHE_MISS);
        ss << std::endl;

        ss << "rocksdb block-cache stats: hit="  << stat->getAndResetTickerCount(rocksdb::BLOCK_CACHE_HIT);
        ss << ", miss="  << stat->getAndResetTickerCount(rocksdb::BLOCK_CACHE_MISS);
        ss << std::endl;

        ss << "rockdb get histograms: " << stat->getHistogramString(rocksdb::DB_GET) << std::endl;
        ss << "rockdb write histograms: " <<  stat->getHistogramString(rocksdb::DB_WRITE) << std::endl;

        stat->Reset();
    }

    return ss.str();
}

Status RocksDBManager::CompactRange(const rocksdb::CompactRangeOptions& ops,
                    rocksdb::Slice* start, rocksdb::Slice* end) {
    for (auto cf : cf_handles_) {
        auto s = db_->CompactRange(ops, cf, start, end);
        if (!s.ok()) {
            return Status(Status::kIOError, "compaction db", s.ToString());
        }
    }
    return Status::OK();
}

Status RocksDBManager::Flush(const rocksdb::FlushOptions& ops) {
    auto s = db_->Flush(ops);
    if (!s.ok()) {
        return Status(Status::kIOError, "flush db", s.ToString());
    } else {
        return Status::OK();
    }
}

Status RocksDBManager::SetPerfLevel(rocksdb::PerfLevel level) {
    rocksdb::SetPerfLevel(level);
    if (level == rocksdb::PerfLevel::kDisable) {
        FLOG_INFO("perf result: {}", rocksdb::get_perf_context()->ToString());
    }
    rocksdb::get_perf_context()->Reset();
    return Status::OK();
}

void RocksDBManager::RCUFree(bool wait) {
    if (txn_cache_ == nullptr) {
        return;
    }
    RunRCUFree(wait);
}

bool RocksDBManager::GetProperty(const std::string& property, std::ostringstream& os) {
    for (auto cf : cf_handles_) {
        std::string value;
        if (!db_->GetProperty(cf, property, &value)) {
            return false;
        }
        os << "[CF/" << cf->GetName() << "]:" << std::endl;
        os << value << std::endl;
    }
    return true;
}

} // namespace db
} // namespace ds
} // namespace jim
