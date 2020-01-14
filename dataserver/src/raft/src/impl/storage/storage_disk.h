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

#include <atomic>
#include "meta_file.h"
#include "storage.h"

namespace jim {
namespace raft {
namespace impl {
namespace storage {

class LogFile;

class DiskStorage : public Storage {
public:
    struct Options {
        size_t log_file_size = 1024 * 1024 * 16;
        size_t max_log_files = std::numeric_limits<size_t>::max();
        size_t log_ttl_seconds = 0;
        bool allow_corrupt_startup = false;
        bool always_sync = false;
        bool readonly = false;
    };

    DiskStorage(uint64_t id, const std::string& path, const Options& ops);
    ~DiskStorage();

    DiskStorage(const DiskStorage&) = delete;
    DiskStorage& operator=(const DiskStorage&) = delete;

    Status Open() override;

    Status StoreHardState(const pb::HardState& hs) override;
    Status InitialState(pb::HardState* hs) const override;

    Status StoreEntries(const std::vector<EntryPtr>& entries) override;
    Status Term(uint64_t index, uint64_t* term, bool* is_compacted) const override;
    Status FirstIndex(uint64_t* index) const override;
    Status LastIndex(uint64_t* index) const override;
    Status Entries(uint64_t lo, uint64_t hi, uint64_t max_size,
                   std::vector<EntryPtr>* entries, bool* is_compacted) const override;

    Status Truncate(uint64_t index) override;

    Status ApplySnapshot(const pb::SnapshotMeta& meta) override;

    Status Close() override;
    Status Destroy(bool backup) override;

    size_t FilesCount() const { return log_files_.size(); }

    std::unique_ptr<LogReader> NewReader(uint64_t start_index) override;

    Status InheritLog(const std::string& dest_dir, uint64_t last_index, bool only_index) override;

    uint64_t InheritIndex() override { return inherit_index_; }

// for tests
#ifndef NDEBUG
    void TEST_Add_Corruption1();
    void TEST_Add_Corruption2();
    void TEST_Add_Corruption3();
#endif

private:
    Status initDir();
    Status initMeta();
    Status listLogs(std::map<uint64_t, uint64_t>* logs);
    Status openLogs();
    Status closeLogs();

    // truncate from old
    Status truncateOld(uint64_t index, uint64_t& trunc_index, uint64_t& trunc_term);
    // truncate from new 
    Status truncateNew(uint64_t index);
    Status truncateAll();

    Status rotate();
    Status checkRotate();
    Status save(const EntryPtr& e);

private:
    const uint64_t id_ = 0;
    const std::string path_;
    const Options ops_;

    MetaFile meta_file_;
    pb::HardState hard_state_;
    pb::TruncateMeta trunc_meta_;
    uint64_t inherit_index_ = 0;

    std::vector<LogFile*> log_files_;
    uint64_t last_index_ = 0;

    std::atomic<bool> destroyed_ = {false};
};

} /* namespace storage */
} /* namespace impl */
} /* namespace raft */
} /* namespace jim */
