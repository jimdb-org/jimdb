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

#include <deque>
#include "storage.h"

namespace jim {
namespace raft {
namespace impl {
namespace storage {

class MemoryStorage : public Storage {
public:
    MemoryStorage(uint64_t id, uint64_t capacity);
    ~MemoryStorage();

    Status Open() override;

    Status InitialState(pb::HardState* hs) const override;

    Status Entries(uint64_t lo, uint64_t hi, uint64_t max_size,
                   std::vector<EntryPtr>* entries, bool* is_compacted) const override;

    Status Term(uint64_t index, uint64_t* term, bool* is_compacted) const override;

    Status FirstIndex(uint64_t* index) const override;
    Status LastIndex(uint64_t* index) const override;

    Status StoreEntries(const std::vector<EntryPtr>& entries) override;

    Status StoreHardState(const pb::HardState& hs) override;

    Status Truncate(uint64_t index) override;

    Status ApplySnapshot(const pb::SnapshotMeta& meta) override;

    Status Close() override;

    Status Destroy(bool backup) override;

    std::unique_ptr<LogReader> NewReader(uint64_t start_index) override;

    Status InheritLog(const std::string& dest_dir, uint64_t last_index, bool only_index) override;
    uint64_t InheritIndex() override { return 0; }

private:
    uint64_t lastIndex() const;
    void truncateTo(uint64_t index);

private:
    const uint64_t id_ = 0;
    const uint64_t capacity_ = 0;

    uint64_t trunc_index_ = 0;
    uint64_t trunc_term_ = 0;

    std::deque<EntryPtr> entries_;

    pb::HardState hardstate_;
};

} /* namespace storage */
} /* namespace impl */
} /* namespace raft */
} /* namespace jim */
