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

#include <rocksdb/db.h>

#include "common/server_config.h"

#include "db/mass_tree_impl/mass_tree_wrapper.h"
#include "db/db.h"
#include "iterator_impl.h"
#include "write_batch_impl.h"

namespace jim {
namespace ds {
namespace db {

class RocksDBManager;
class RocksWriteBatch;
class MasstreeWrapper;

struct RocksDBOptions {
    uint64_t id = 0;
    std::string start_key;
    std::string end_key;
    bool wal_disabled = false;
    bool read_checksum = true;

    rocksdb::DB *db = nullptr;
};

class RocksDBImpl: public DB {
public:
    RocksDBImpl(const RocksDBOptions& ops, RocksDBManager *manager,
            rocksdb::ColumnFamilyHandle *data_cf, rocksdb::ColumnFamilyHandle *txn_cf,
            rocksdb::ColumnFamilyHandle *meta_cf, MasstreeWrapper* txn_cache);

    ~RocksDBImpl();

public:
    Status Open() override;
    Status Close() override;
    Status Destroy() override;

    uint64_t PersistApplied() override;

    Status Get(CFType cf, const std::string& key, std::string& value) override;
    Status Put(CFType cf, const std::string& key, const std::string& value, uint64_t raft_index) override;
    Status Delete(CFType cf, const std::string& key, uint64_t raft_index) override;
    Status DeleteRange(CFType cf, const std::string& start, const std::string& limit, uint64_t raft_index) override;

    WriteBatchPtr NewWriteBatch() override;
    Status Write(WriteBatch* batch, uint64_t raft_index) override;

    Status ApplySnapshotStart(uint64_t raft_index) override;
    Status ApplySnapshotData(WriteBatch *batch) override;
    Status ApplySnapshotFinish(uint64_t raft_index) override;

    IteratorPtr NewIterator(const std::string& start, const std::string& limit) override;
    Status NewIterators(const std::string& start, const std::string& limit,
            IteratorPtr& data_iter, IteratorPtr& txn_iter) override;

    Status SplitDB(uint64_t split_range_id, const std::string& split_key,
            uint64_t raft_index, std::unique_ptr<DB>& split_db) override;

    Status ApplySplit(const std::string& split_key, uint64_t raft_index) override;

public:
    rocksdb::ColumnFamilyHandle *getColumnFamily(CFType cf) {
        return cf == CFType::kData ? data_cf_ : txn_cf_;
    }

    // applied funcs
    Status loadApplied();
    Status deleteApplied();
    Status saveApplied(uint64_t index);

    Status recoverTxnCache();

    Status truncate();

    // append applied_index to batch and write batch to db
    Status writeBatch(RocksWriteBatch* batch, uint64_t raft_index);

private:
    const uint64_t id_;
    const std::string applied_key_;
    const std::string start_key_;
    std::string end_key_;

    std::atomic<uint64_t> applied_index_ = {0};

    rocksdb::DB* db_ = nullptr;
    rocksdb::ReadOptions read_options_;
    rocksdb::WriteOptions write_options_;

    RocksDBManager* manager_ = nullptr;
    rocksdb::ColumnFamilyHandle* data_cf_ = nullptr;
    rocksdb::ColumnFamilyHandle* txn_cf_ = nullptr;
    rocksdb::ColumnFamilyHandle* meta_cf_ = nullptr;
    MasstreeWrapper* txn_cache_ = nullptr;
};

} // namespace db
} // namespace ds
} // namespace jim
