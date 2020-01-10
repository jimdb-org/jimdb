// Copyright 2015 The etcd Authors
// Portions Copyright 2019 The JimDB Authors.
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

#include "raft_log_unstable.h"

#include <sstream>
#include "raft_exception.h"

namespace jimkv {
namespace raft {
namespace impl {

UnstableLog::UnstableLog(uint64_t offset) : offset_(offset) {}

UnstableLog::~UnstableLog() {}

bool UnstableLog::maybeLastIndex(uint64_t* last_index) const {
    if (!entries_.empty()) {
        *last_index = offset_ + entries_.size() - 1;
        return true;
    }
    return false;
}

bool UnstableLog::maybeTerm(uint64_t index, uint64_t* term) const {
    if (index < offset_) {
        return false;
    }

    uint64_t last_index = 0;
    if (maybeLastIndex(&last_index) && index <= last_index) {
        assert(index - offset_ < entries_.size() && entries_[index - offset_]);
        *term = entries_[index - offset_]->term();
        return true;
    }
    return false;
}

void UnstableLog::stableTo(uint64_t index, uint64_t term) {
    uint64_t gt = 0;
    if (!maybeTerm(index, &gt)) {
        return;
    }

    if (gt == term && index >= offset_) {
        entries_.erase(entries_.begin(), entries_.begin() + (index - offset_ + 1));
        offset_ = index + 1;
    }
}

void UnstableLog::restore(uint64_t index) {
    entries_.clear();
    offset_ = index + 1;
}

void UnstableLog::truncateAndAppend(const std::vector<EntryPtr>& ents) {
    uint64_t after = ents[0]->index();
    if (after == offset_ + static_cast<uint64_t>(entries_.size())) {
        std::copy(ents.begin(), ents.end(), std::back_inserter(entries_));
    } else if (after <= offset_) {
        entries_.clear();
        std::copy(ents.begin(), ents.end(), std::back_inserter(entries_));
        offset_ = after;
    } else {
        while (!entries_.empty() && entries_.back()->index() >= after) {
            entries_.pop_back();
        }
        std::copy(ents.begin(), ents.end(), std::back_inserter(entries_));
    }
}

void UnstableLog::slice(uint64_t lo, uint64_t hi, std::vector<EntryPtr>* ents) const {
    mustCheckOutOfBounds(lo, hi);
    std::copy(entries_.begin() + (lo - offset_), entries_.begin() + (hi - offset_),
              std::back_inserter(*ents));
}

void UnstableLog::entries(std::vector<EntryPtr>* ents) const {
    std::copy(entries_.begin(), entries_.end(), std::back_inserter(*ents));
}

void UnstableLog::mustCheckOutOfBounds(uint64_t lo, uint64_t hi) const {
    if (lo > hi) {
        std::ostringstream ss;
        ss << "invalid unstable.slice:"
           << "(" << lo << "," << hi << ")";
        throw RaftException(ss.str());
    }
    auto upper = offset_ + static_cast<uint64_t>(entries_.size());
    if (lo < offset_ || hi > upper) {
        std::ostringstream ss;
        ss << "unstable.slice:"
           << "(" << lo << "," << hi << ") out of bound [" << offset_ << "," << upper
           << ",]";
        throw RaftException(ss.str());
    }
}

} /* namespace impl */
} /* namespace raft */
} /* namespace jimkv */
