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
#include "base/status.h"
#include "db/db.h"

namespace jim {
namespace ds {
namespace db {

class RocksIterator: public Iterator {
public:
    ~RocksIterator();

    bool Valid() override;
    void Next() override;

    Status status() override;

    std::string Key() override;
    std::string Value() override;

    uint64_t KeySize() override;
    uint64_t ValueSize() override;

    static Status Create(rocksdb::DB *db,
            const std::vector<rocksdb::ColumnFamilyHandle*>& cfs,
            const std::string& start,
            const std::string& limit,
            const rocksdb::ReadOptions& options,
            std::vector<IteratorPtr>& iterators
            );

private:
    RocksIterator(rocksdb::Iterator* it, const std::string& start,
                  const std::shared_ptr<rocksdb::Slice>& limit);
private:
    rocksdb::Iterator* rit_ = nullptr;
    std::shared_ptr<rocksdb::Slice> upper_bound_;
};

} // namespace db
} // namespace ds
} // namespace jim
