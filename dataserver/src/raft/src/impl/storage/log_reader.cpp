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

#include "log_reader.h"

namespace jim {
namespace raft {
namespace impl {
namespace storage {

// create a failed reader
LogReaderImpl::LogReaderImpl(Status&& failed_status) :
    last_status_(std::move(failed_status)) {
}

LogReaderImpl::LogReaderImpl(std::vector<LogFilePtr>&& log_files, uint64_t start_index) :
    log_files_(std::move(log_files)),
    current_index_(start_index) {
}

LogReaderImpl::~LogReaderImpl() {
}

Status LogReaderImpl::Next(uint64_t& index, std::string& data, bool& over) {
    if (!last_status_.ok() || reach_end_) {
        over = true;
        return last_status_;
    }

    // check reach file end
    if (log_files_.empty() || current_index_ > log_files_[file_index_]->LastIndex()) {
        ++file_index_;
        if (file_index_ >= log_files_.size()) {
            reach_end_ = true;
            over = true;
            return last_status_;
        }
    }

    assert(file_index_ < log_files_.size());

    // read entry
    EntryPtr entry;
    last_status_ = log_files_[file_index_]->Get(current_index_, &entry);
    if (!last_status_.ok()) {
        return last_status_;
    }

    ++current_index_;

    if (entry->type() != pb::ENTRY_NORMAL || entry->data().empty()) {
        return Next(index, data, over); // skip and continue
    } else {
        index = entry->index();
        data.swap(*entry->mutable_data());
        over = false;
    }
    return last_status_;
}

uint64_t LogReaderImpl::LastIndex() {
    return current_index_ >= 1 ? current_index_ - 1 : 0;
}

} /* namespace storage */
} /* namespace impl */
} /* namespace raft */
} /* namespace jim */
