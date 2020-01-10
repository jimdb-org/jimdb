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

#include "base/status.h"
#include "raft/log_reader.h"

#include "log_file.h"

namespace jimkv {
namespace raft {
namespace impl {
namespace storage {

class LogReaderImpl : public LogReader {
public:
    // create a failed reader
    explicit LogReaderImpl(Status&& failed_status);

    LogReaderImpl(std::vector<LogFilePtr>&& log_files, uint64_t start_index);

    ~LogReaderImpl();

    LogReaderImpl(const LogReaderImpl&) = delete;
    LogReaderImpl& operator=(const LogReaderImpl&) = delete;

    Status Next(uint64_t& index, std::string& data, bool& over) override;

    uint64_t LastIndex() override;

private:
    const std::vector<LogFilePtr> log_files_;
    size_t file_index_ = { 0 };
    uint64_t current_index_ = { 0 };
    bool reach_end_ = { false };
    Status last_status_;
};

} /* namespace storage */
} /* namespace impl */
} /* namespace raft */
} /* namespace jimkv */
