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
#include <vector>
#include <functional>

#include "db/db.h"

namespace jimkv {
namespace ds {
namespace db {

static const size_t kMaxKvBatchEntryCount = 10000;

enum class KvEntryType {
    kPut = 0,
    kDelete = 1,
};

struct KvEntry {
    KvEntryType type = KvEntryType::kPut;
    CFType cf = CFType::kData;
    std::string key;
    std::string value;

    std::string DebugString() const;
};

class MassTreeBatch: public WriteBatch {
public:
    explicit MassTreeBatch(uint64_t raft_index = 0);
    virtual ~MassTreeBatch() = default;

    const std::vector<KvEntry>& Entries() const { return entries_; }
    size_t BytesSize() const { return bytes_; }

    void SetIndex(uint64_t index) { raft_index_ = index; }
    uint64_t GetIndex() const { return raft_index_; }

    void Reserve(size_t size);

    Status Put(CFType cf, const std::string& key, const std::string& value) override;
    Status Delete(CFType cf, const std::string& key) override;
    Status Append(KvEntryType type, CFType cf, const std::string& key, const std::string& value);

    Status AssertEuqalWith(const MassTreeBatch& other);

private:
    std::vector<KvEntry> entries_;
    uint64_t raft_index_ = 0;
    size_t bytes_ = 0;
};

} // namespace db
} // namespace ds
} // namespace jimkv
