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

_Pragma("once");

#include <cstdint>
#include <deque>

#include "raft_types.h"

namespace jim {
namespace raft {
namespace impl {

class UnstableLog {
public:
    explicit UnstableLog(uint64_t offset);
    ~UnstableLog();

    UnstableLog(const UnstableLog&) = delete;
    UnstableLog& operator=(const UnstableLog&) = delete;

    uint64_t offset() const { return offset_; }

    bool maybeLastIndex(uint64_t* last_index) const;
    bool maybeTerm(uint64_t index, uint64_t* term) const;

    void stableTo(uint64_t index, uint64_t term);
    void restore(uint64_t index);

    void truncateAndAppend(const std::vector<EntryPtr>& ents);
    void slice(uint64_t lo, uint64_t hi, std::vector<EntryPtr>* ents) const;
    void entries(std::vector<EntryPtr>* ents) const;

private:
    void mustCheckOutOfBounds(uint64_t lo, uint64_t hi) const;

private:
    uint64_t offset_ = 0;
    std::deque<EntryPtr> entries_;
};

} /* namespace impl */
} /* namespace raft */
} /* namespace jim */
