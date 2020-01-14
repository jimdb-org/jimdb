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
#include <rocksdb/write_batch.h>

#include "db/db.h"

namespace jim {
namespace ds {
namespace db {

class MasstreeWrapper;

class RocksWriteBatch: public WriteBatch {
public:
    RocksWriteBatch(rocksdb::ColumnFamilyHandle* data_cf, rocksdb::ColumnFamilyHandle* txn_cf);

    ~RocksWriteBatch() {}

    Status Put(CFType cf, const std::string& key, const std::string& value) override;
    Status Delete(CFType cf, const std::string& key) override;

    Status Put(rocksdb::ColumnFamilyHandle *cf, const std::string& key, const std::string& value);

    Status WriteTxnCache(MasstreeWrapper *cache);

public:
    rocksdb::WriteBatch* getBatch() { return &batch_; }

private:
    rocksdb::ColumnFamilyHandle* const data_cf_;
    rocksdb::ColumnFamilyHandle* const txn_cf_;

    rocksdb::WriteBatch batch_;
    bool has_txn_changes_ = false;
};

} // namespace db
} // namespace ds
} // namespace jim

