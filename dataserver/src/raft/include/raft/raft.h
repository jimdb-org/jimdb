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

#include "options.h"
#include "status.h"
#include "log_reader.h"
#include "entry_flags.h"

namespace jim {
namespace raft {

class Raft {
public:
    Raft() = default;
    virtual ~Raft() = default;

    Raft(const Raft&) = delete;
    Raft& operator=(const Raft&) = delete;

    virtual bool IsStopped() const = 0;

    virtual void GetLeaderTerm(uint64_t* leader, uint64_t* term) const = 0;
    virtual bool IsLeader() const = 0;

    virtual Status TryToLeader() = 0;

    // propose to replicate a log entry
    // when log is committed, Apply(entry_data) will be called
    virtual Status Propose(std::string& entry_data, uint32_t entry_flags) = 0;

    virtual Status ReadIndex(std::string& ctx) = 0;

    virtual Status ChangeMemeber(const ConfChange& conf) = 0;

    virtual void GetStatus(RaftStatus* status) const = 0;

    virtual void Truncate(uint64_t index) = 0;

    // fetch raft log entries since start_index
    virtual std::unique_ptr<LogReader> ReadLog(uint64_t start_index) = 0;

    // create a new raft log storage to inherit current raft logs, used by range split
    // 1) if only_index is false, hard link current log files(with entries.index <= last_index) to dest directories
    //    if only_index is true, no log files will be linked
    // 2) init new meta file, make the new raft next log index be last_index + 1
    virtual Status InheritLog(const std::string& dest_dir, uint64_t last_index, bool only_index) = 0;
};

} /* namespace raft */
} /* namespace jim */
