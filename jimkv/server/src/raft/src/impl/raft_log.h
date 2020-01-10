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

_Pragma("once");

#include <memory>

#include "base/status.h"
#include "raft_log_unstable.h"
#include "raft_types.h"

namespace jimkv {
namespace raft {
namespace impl {

namespace storage {
class Storage;
};

class RaftLog {
public:
    explicit RaftLog(uint64_t id, const std::shared_ptr<storage::Storage>& s);
    ~RaftLog() = default;

    RaftLog(const RaftLog&) = delete;
    RaftLog& operator=(const RaftLog&) = delete;

    uint64_t committed() const { return committed_; }
    void set_committed(uint64_t commit) { committed_ = commit; }
    uint64_t applied() const { return applied_; }

    uint64_t firstIndex() const;
    uint64_t lastIndex() const;

    Status term(uint64_t index, uint64_t* term) const;
    uint64_t lastTerm() const;
    void lastIndexAndTerm(uint64_t* index, uint64_t* term) const;
    bool matchTerm(uint64_t index, uint64_t term) const;

    uint64_t findConfilct(const std::vector<EntryPtr>& ents) const;

    bool maybeAppend(uint64_t index, uint64_t log_term, uint64_t commit,
                     const std::vector<EntryPtr>& ents, uint64_t* lastnewi);
    uint64_t append(const std::vector<EntryPtr>& ents);

    // TODO: return const references
    void unstableEntries(std::vector<EntryPtr>* ents);

    void nextEntries(uint64_t max_size, std::vector<EntryPtr>* ents);

    Status entries(uint64_t index, uint64_t max_size, std::vector<EntryPtr>* ents) const;

    bool maybeCommit(uint64_t max_index, uint64_t term);

    void commitTo(uint64_t commit);

    void appliedTo(uint64_t index);

    void stableTo(uint64_t index, uint64_t term);

    bool isUpdateToDate(uint64_t lasti, uint64_t term);

    void restore(uint64_t index);

    Status slice(uint64_t lo, uint64_t hi, uint64_t max_size,
                 std::vector<EntryPtr>* ents) const;

    void allEntries(std::vector<EntryPtr>* ents) const;

private:
    static uint64_t zeroTermOnErrCompacted(uint64_t term, const Status& s);

    Status open();

    Status mustCheckOutOfBounds(uint64_t lo, uint64_t hi) const;

private:
    const uint64_t id_;
    std::shared_ptr<storage::Storage> storage_;
    std::unique_ptr<UnstableLog> unstable_;
    uint64_t committed_{0};
    uint64_t applied_{0};
};

} /* namespace impl */
} /* namespace raft */
} /* namespace jimkv */
