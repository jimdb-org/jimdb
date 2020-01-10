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

#include <vector>
#include "base/status.h"
#include "raft/log_reader.h"

#include "../raft.pb.h"
#include "../raft_types.h"

namespace jimkv {
namespace raft {
namespace impl {
namespace storage {

class Storage {
public:
    Storage() = default;
    virtual ~Storage() = default;

    Storage(const Storage&) = delete;
    Storage& operator=(const Storage&) = delete;

public:
    virtual Status Open() = 0;

    // InitialState returns the saved HardState information to init the repl
    // state.
    virtual Status InitialState(pb::HardState* hs) const = 0;

    // Entries returns a slice of log entries in the range [lo,hi), the hi is
    // not
    // inclusive.
    // MaxSize limits the total size of the log entries returned, but Entries
    // returns at least one entry if any.
    // If lo <= CompactIndex,then return isCompact true.
    // If no entries,then return entries nil.
    // Note: math.MaxUint32 is no limit.
    virtual Status Entries(uint64_t lo, uint64_t hi, uint64_t max_size,
                           std::vector<EntryPtr>* entries, bool* is_compacted) const = 0;

    // Term returns the term of entry i, which must be in the range
    // [FirstIndex()-1, LastIndex()].
    // The term of the entry before FirstIndex is retained for matching purposes
    // even though the
    // rest of that entry may not be available.
    // If lo <= CompactIndex,then return isCompact true.
    virtual Status Term(uint64_t index, uint64_t* term, bool* is_compacted) const = 0;

    // FirstIndex returns the index of the first log entry that is possibly
    // available via Entries (older entries have been incorporated
    // into the latest Snapshot; if storage only contains the dummy entry the
    // first log entry is not available).
    virtual Status FirstIndex(uint64_t* index) const = 0;

    // LastIndex returns the index of the last entry in the log.
    virtual Status LastIndex(uint64_t* index) const = 0;

    // StoreEntries store the log entries to the repository.
    // If first index of entries > LastIndex,then append all entries,
    // Else write entries at first index and truncate the redundant log entries.
    virtual Status StoreEntries(const std::vector<EntryPtr>& entries) = 0;

    // StoreHardState store the raft state to the repository.
    virtual Status StoreHardState(const pb::HardState& hs) = 0;

    // Truncate the log to index,  The index is inclusive.
    virtual Status Truncate(uint64_t index) = 0;

    // Sync snapshot status.
    virtual Status ApplySnapshot(const pb::SnapshotMeta& meta) = 0;

    // Close the storage.
    virtual Status Close() = 0;

    // Delete all data
    virtual Status Destroy(bool backup) = 0;

    virtual std::unique_ptr<LogReader> NewReader(uint64_t start_index) = 0;

    virtual Status InheritLog(const std::string& dest_dir, uint64_t last_index, bool only_index) = 0;

    virtual uint64_t InheritIndex() = 0;
};

} /* namespace storage */
} /* namespace impl */
} /* namespace raft */
} /* namespace jimkv */