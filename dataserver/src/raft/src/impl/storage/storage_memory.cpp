// Copyright 2015 The etcd Authors
// Portions Copyright 2019 The JIMDB Authors.
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

#include "storage_memory.h"

#include <string>

namespace jim {
namespace raft {
namespace impl {
namespace storage {

MemoryStorage::MemoryStorage(uint64_t id, uint64_t capacity)
    : id_(id), capacity_(capacity) {
        (void)id_;
}

MemoryStorage::~MemoryStorage() {}

Status MemoryStorage::Open() { return Status::OK(); }

Status MemoryStorage::InitialState(pb::HardState* hs) const {
    hs->CopyFrom(hardstate_);
    return Status::OK();
}

Status MemoryStorage::Entries(uint64_t lo, uint64_t hi, uint64_t max_size,
                              std::vector<EntryPtr>* ents, bool* is_compacted) const {
    if (lo <= trunc_index_) {
        *is_compacted = true;
        return Status::OK();
    }
    if (hi > lastIndex() + 1) {
        return Status(Status::kInvalidArgument, "index is out of bound lastindex",
                      std::to_string(hi));
    }
    if (entries_.empty()) {
        return Status(Status::kNotFound, "requested entry at index is unavailable", "");
    }
    if (hi <= lo) {
        return Status::OK();
    }

    ents->reserve(hi - lo);
    uint64_t size = 0;
    uint64_t count = 0;
    for (auto it = entries_.begin() + (lo - trunc_index_ - 1); it != entries_.end();
         ++it) {
        if ((*it)->index() >= hi) break;
        size += (*it)->ByteSizeLong();
        if (size > max_size) {
            if (count == 0) {  // atleast one
                ents->push_back(*it);
            }
            break;
        } else {
            ents->push_back(*it);
            ++count;
        }
    }
    return Status::OK();
}

Status MemoryStorage::Term(uint64_t index, uint64_t* term, bool* is_compacted) const {
    if (index < trunc_index_) {
        *is_compacted = true;
    } else if (index == trunc_index_) {
        *is_compacted = false;
        *term = trunc_term_;
    } else if (entries_.empty() || index > entries_.back()->index()) {
        return Status(Status::kInvalidArgument, "index is out of bound lastindex",
                      std::to_string(index));
    } else {
        *is_compacted = false;
        *term = entries_[index - trunc_index_ - 1]->term();
    }
    return Status::OK();
}

Status MemoryStorage::FirstIndex(uint64_t* index) const {
    *index = trunc_index_ + 1;
    return Status::OK();
}

uint64_t MemoryStorage::lastIndex() const { return trunc_index_ + entries_.size(); }

Status MemoryStorage::LastIndex(uint64_t* index) const {
    *index = lastIndex();
    return Status::OK();
}

Status MemoryStorage::StoreEntries(const std::vector<EntryPtr>& ents) {
    if (ents.empty()) {
        return Status::OK();
    }

    if (ents.size() > capacity_) {
        return Status(Status::kInvalidArgument, "too many entries to store",
                      std::to_string(ents.size()));
    }

    if (ents[0]->index() > lastIndex() + 1) {
        return Status(Status::kInvalidArgument, "missing log entry",
                      std::to_string(lastIndex() + 1));
    }

    // remove conflict
    while (!entries_.empty() && entries_.back()->index() >= ents[0]->index()) {
        entries_.pop_back();
    }

    // truncate to make room for ents
    while (entries_.size() + ents.size() > capacity_) {
        trunc_term_ = entries_.front()->term();
        trunc_index_ = entries_.front()->index();
        entries_.pop_front();
    }

    std::copy(ents.begin(), ents.end(), std::back_inserter(entries_));

    return Status::OK();
}

Status MemoryStorage::StoreHardState(const pb::HardState& hs) {
    hardstate_.CopyFrom(hs);
    return Status::OK();
}

void MemoryStorage::truncateTo(uint64_t index) {
    while (!entries_.empty() && entries_.front()->index() <= index) {
        trunc_term_ = entries_.front()->term();
        trunc_index_ = entries_.front()->index();
        entries_.pop_front();
    }
}

Status MemoryStorage::Truncate(uint64_t index) {
    if (index == 0 || index <= trunc_index_) {
        return Status(Status::kInvalidArgument,
                      "requested index is unavailable due to compaction", "");
    } else if (index > lastIndex()) {
        return Status(Status::kInvalidArgument, "compact is out of bound lastindex",
                      std::to_string(index));
    } else {
        truncateTo(index);
        return Status::OK();
    }
}

Status MemoryStorage::ApplySnapshot(const pb::SnapshotMeta& meta) {
    trunc_index_ = meta.index();
    trunc_term_ = meta.term();
    entries_.clear();
    return Status::OK();
}

Status MemoryStorage::Close() { return Status::OK(); }

Status MemoryStorage::Destroy(bool backup) { return Status::OK(); }

std::unique_ptr<LogReader> MemoryStorage::NewReader(uint64_t start_index) {
    // TODO:
    return nullptr;
}

Status MemoryStorage::InheritLog(const std::string& dest_dir, uint64_t last_index, bool only_index) {
    (void)last_index;
    (void)only_index;
    return Status(Status::kNotSupported);
}

} /* namespace storage */
} /* namespace impl */
} /* namespace raft */
} /* namespace jim */
