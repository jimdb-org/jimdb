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

#include "log_index.h"

namespace jim {
namespace raft {
namespace impl {
namespace storage {

LogIndex::LogIndex() {}

LogIndex::~LogIndex() {}

void LogIndex::CopyFrom(const LogIndex& from) {
    items_ = from.items_;
}

Status LogIndex::ParseFrom(const Record& rec, const std::vector<char>& payload) {
    if (rec.type != RecordType::kIndex) {
        return Status(Status::kCorruption, "invalid log index record type",
                      std::to_string(rec.type));
    }

    pb::LogIndex idx;
    if (!idx.ParseFromArray(payload.data(), payload.size())) {
        return Status(Status::kCorruption, "parse log index", "pb::ParseFromArray");
    }

    items_.clear();
    for (int i = 0; i < idx.items_size(); ++i) {
        items_.emplace(idx.items(i).index(), idx.items(i));
    }

    return Status::OK();
}

void LogIndex::Serialize(pb::LogIndex* pb_msg) {
    pb_msg->clear_items();
    for (const auto& pair : items_) {
        pb_msg->add_items()->CopyFrom(pair.second);
    }
}

uint64_t LogIndex::First() const {
    if (!items_.empty()) {
        return items_.cbegin()->first;
    } else {
        return 0;
    }
}

uint64_t LogIndex::Last() const {
    if (!items_.empty()) {
        return items_.crbegin()->first;
    } else {
        return 0;
    }
}

uint64_t LogIndex::Term(uint64_t index) const {
    auto it = items_.find(index);
    if (it != items_.cend()) {
        return it->second.term();
    } else {
        return 0;
    }
}

uint32_t LogIndex::Offset(uint64_t index) const {
    auto it = items_.find(index);
    if (it != items_.cend()) {
        return it->second.offset();
    } else {
        return 0;
    }
}

void LogIndex::Append(uint64_t index, uint64_t term, uint32_t offset) {
    assert(items_.empty() || Last() + 1 == index);
    pb::IndexItem item;
    item.set_index(index);
    item.set_term(term);
    item.set_offset(offset);
    items_.emplace(index, item);
}

void LogIndex::Truncate(uint64_t index) {
    auto it = items_.find(index);
    if (it != items_.end()) {
        items_.erase(it, items_.end());
    }
}

void LogIndex::Clear() { items_.clear(); }

} /* namespace storage */
} /* namespace impl */
} /* namespace raft */
} /* namespace jim */