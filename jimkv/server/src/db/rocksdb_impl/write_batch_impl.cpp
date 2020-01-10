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

#include "write_batch_impl.h"

#include <rocksdb/write_batch.h>

#include "db/mass_tree_impl/mass_tree_wrapper.h"

namespace jimkv {
namespace ds {
namespace db {

class TxnCacheWriter: public rocksdb::WriteBatch::Handler {
public:
    TxnCacheWriter(MasstreeWrapper* cache, uint32_t txn_cf_id) :
        cache_(cache),
        txn_cf_id_(txn_cf_id) {
    }

    rocksdb::Status PutCF(uint32_t cf_id, const rocksdb::Slice& key, const rocksdb::Slice& value) override {
        if (cf_id != txn_cf_id_) {
            return rocksdb::Status::OK();
        }
        auto s = cache_->Put(key.ToString(), value.ToString());
        if (!s.ok()) {
            return rocksdb::Status::IOError("put txn cache", s.ToString());
        }
        return rocksdb::Status::OK();
    }

    rocksdb::Status DeleteCF(uint32_t cf_id, const rocksdb::Slice& key) override {
        if (cf_id != txn_cf_id_) {
            return rocksdb::Status::OK();
        }
        auto s = cache_->Delete(key.ToString());
        if (!s.ok()) {
            return rocksdb::Status::IOError("delete txn cache", s.ToString());
        }
        return rocksdb::Status::OK();
    }

private:
    MasstreeWrapper* const cache_ = nullptr;
    const uint32_t txn_cf_id_ = 0;
};

RocksWriteBatch::RocksWriteBatch(rocksdb::ColumnFamilyHandle* data_cf, rocksdb::ColumnFamilyHandle* txn_cf) :
    data_cf_(data_cf),
    txn_cf_(txn_cf) {
}

Status RocksWriteBatch::Put(rocksdb::ColumnFamilyHandle *cf, const std::string& key, const std::string& value) {
    if (!has_txn_changes_ && cf == txn_cf_) {
        has_txn_changes_ = true;
    }

    auto s = batch_.Put(cf, key, value);
    if (s.ok()) {
        return Status(Status::OK());
    } else {
        return Status(Status::kIOError, "put to batch", s.ToString());
    }
}

Status RocksWriteBatch::Put(CFType cf, const std::string& key, const std::string& value) {
    return this->Put(cf == CFType::kData? data_cf_ : txn_cf_, key, value);
}

Status RocksWriteBatch::Delete(CFType cf, const std::string& key) {
    if (!has_txn_changes_ && cf == CFType::kTxn) {
        has_txn_changes_ = true;
    }

    auto s = batch_.Delete(cf == CFType::kData? data_cf_ : txn_cf_, key);
    if (s.ok()) {
        return Status(Status::OK());
    } else {
        return Status(Status::kIOError, "delete to batch", s.ToString());
    }
}

Status RocksWriteBatch::WriteTxnCache(MasstreeWrapper *cache) {
    if (!has_txn_changes_ || cache == nullptr) {
        return Status::OK();
    }

    TxnCacheWriter handle(cache, txn_cf_->GetID());
    auto s = batch_.Iterate(&handle);
    if (!s.ok()) {
        return Status(Status::kIOError, "write batch to txn cache", s.ToString());
    } else {
        return Status::OK();
    }
}

} // namespace db
} // namespace ds
} // namespace jimkv

