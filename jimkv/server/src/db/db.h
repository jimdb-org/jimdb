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

_Pragma("once");

#include <memory>
#include <unordered_map>
#include "base/status.h"

namespace jimkv {
namespace ds {
namespace db {

static const uint32_t kMaxDBKeySize = 1024 * 1024 * 4;
static const uint32_t kMaxDBValueSize = 1024 * 1024 * 10;

// CFType: column family or namespace
enum class CFType {
    kData = 0,
    kTxn = 1,
};

class WriteBatch {
public:
    WriteBatch() = default;
    virtual ~WriteBatch() = default;

    virtual Status Put(CFType cf, const std::string& key, const std::string& value) = 0;
    virtual Status Delete(CFType cf, const std::string& key) = 0;

    Status Put(const std::string& key, const std::string& value) {
        return Put(CFType::kData, key, value);
    }

    Status Delete(const std::string& key) {
        return Delete(CFType::kData, key);
    }
};

class Iterator {
public:
    Iterator() = default;
    virtual ~Iterator() = default;

    virtual bool Valid() = 0;
    virtual void Next() = 0;
    virtual Status status() = 0;
    virtual std::string Key() = 0;
    virtual std::string Value() = 0;
    virtual uint64_t KeySize() = 0;
    virtual uint64_t ValueSize() = 0;
};

using IteratorPtr= std::unique_ptr<Iterator>;
using WriteBatchPtr= std::unique_ptr<WriteBatch>;

class DB {
public:
    DB() = default;
    virtual ~DB() = default;

    DB(const DB&) = default;
    DB& operator=(const DB&) = default;

    virtual Status Open() = 0;
    virtual Status Close() = 0;
    virtual Status Destroy() = 0;

    virtual uint64_t PersistApplied() = 0;

    virtual Status Get(CFType cf, const std::string& key, std::string& value) = 0;
    virtual Status Put(CFType cf, const std::string& key, const std::string& value, uint64_t raft_index) = 0;
    virtual Status Delete(CFType cf, const std::string& key, uint64_t raft_index) = 0;
    virtual Status DeleteRange(CFType cf, const std::string& start, const std::string& limit, uint64_t raft_index) = 0;

    virtual WriteBatchPtr NewWriteBatch() = 0;
    virtual Status Write(WriteBatch* batch, uint64_t raft_index) = 0;

    virtual Status ApplySnapshotStart(uint64_t raft_index) = 0;
    virtual Status ApplySnapshotData(WriteBatch* batch) = 0;
    virtual Status ApplySnapshotFinish(uint64_t raft_index) = 0;

    virtual IteratorPtr NewIterator(const std::string& start, const std::string& limit) = 0;
    virtual Status NewIterators(const std::string& start, const std::string& limit,
            IteratorPtr& data_iter, IteratorPtr& txn_iter) = 0;

    virtual Status SplitDB(uint64_t split_range_id, const std::string& split_key,
            uint64_t raft_index, std::unique_ptr<DB>& split_db) = 0;
    virtual Status ApplySplit(const std::string& split_key, uint64_t raft_index) = 0;

    Status Get(const std::string& key, std::string& value) {
        return Get(CFType::kData, key, value);
    }

    Status Put(const std::string& key, const std::string& value, uint64_t raft_index) {
        return Put(CFType::kData, key, value, raft_index);
    }

    Status Delete(const std::string& key, uint64_t raft_index) {
        return Delete(CFType::kData, key, raft_index);
    }

    Status DeleteRange(const std::string& start, const std::string& limit, uint64_t raft_index) {
        return DeleteRange(CFType::kData, start, limit, raft_index);
    }
};

} // namespace db
} // namespace ds
} // namespace jimkv
