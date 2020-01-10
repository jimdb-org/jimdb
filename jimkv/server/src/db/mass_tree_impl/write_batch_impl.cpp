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

#include <sstream>

namespace jimkv {
namespace ds {
namespace db {

std::string KvEntry::DebugString() const {
    std::ostringstream ss;
    ss << "{ type: " << (type == KvEntryType::kPut ? "PUT" : "DEL");
    ss << ", cf: " << (cf == CFType::kData? "default" : "txn");
    ss << ", key: " << key << ", value: " << value  << "}";
    return ss.str();
}


MassTreeBatch::MassTreeBatch(uint64_t raft_index) :
    raft_index_(raft_index) {
}

void MassTreeBatch::Reserve(size_t size) {
    entries_.reserve(size);
}

Status MassTreeBatch::Append(KvEntryType type, CFType cf, const std::string& key, const std::string& value) {
    if (entries_.size() >= kMaxKvBatchEntryCount) {
        return Status(Status::kResourceExhaust, "write batch too large",
                      std::to_string(entries_.size()));
    }

    entries_.emplace_back(KvEntry());
    auto& ent = entries_.back();
    ent.cf = cf;
    ent.type = type;
    ent.key = key;
    ent.value = value;

    bytes_ += key.size() + value.size();

    return Status::OK();
}

Status MassTreeBatch::Put(CFType cf, const std::string& key, const std::string& value) {
    return Append(KvEntryType::kPut, cf, key, value);
}

Status MassTreeBatch::Delete(CFType cf, const std::string& key) {
    return Append(KvEntryType::kDelete, cf,  key, "");
}

Status MassTreeBatch::AssertEuqalWith(const MassTreeBatch& other) {
    if (raft_index_ != other.raft_index_) {
        return Status(Status::kUnexpected, "raft index",
                std::to_string(raft_index_) + " vs " + std::to_string(other.raft_index_));
    }

    if (entries_.size() != other.entries_.size()) {
        return Status(Status::kUnexpected, "entries size",
                      std::to_string(entries_.size()) + " vs " + std::to_string(other.entries_.size()));
    }

    auto count = entries_.size();
    for (size_t i = 0; i < count; ++i) {
        const auto& lh = entries_[i].DebugString();
        const auto& rh = other.entries_[i].DebugString();
        if (lh != rh) {
            return Status(Status::kUnexpected, "entries at index " + std::to_string(i), lh + "\nvs:\n" + rh);
        }
    }

    return Status::OK();
}

} // namespace db
} // namespace ds
} // namespace jimkv
