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

#include <stdint.h>
#include <string>
#include "base/status.h"

#include "../raft.pb.h"
#include "../raft_types.h"
#include "log_index.h"

namespace jimkv {
namespace raft {
namespace impl {
namespace storage {

class LogIndex;

class LogFile {
public:
    LogFile(const std::string& path, uint64_t seq, uint64_t index, bool readonly = false);
    virtual ~LogFile();

    LogFile(const LogFile&) = delete;
    LogFile& operator=(const LogFile&) = delete;

    Status Open(bool allow_corrupt, bool last_one = false);
    Status Sync();
    Status Close();
    Status Destroy();

    uint64_t Seq() const { return seq_; }
    uint64_t Index() const { return index_; }
    const std::string& Path() const { return file_path_; }
    uint64_t FileSize() const { return file_size_; }
    int LogSize() const { return log_index_.Size(); }
    uint64_t LastIndex() const { return log_index_.Last(); }

    Status Get(uint64_t index, EntryPtr* e) const;
    Status Term(uint64_t index, uint64_t* term) const;

    Status Append(const EntryPtr& e);
    Status Flush();
    Status Rotate();
    Status Truncate(uint64_t index);

    // clone (fd and index) to a new log file for read only
    Status CloneForRead(std::unique_ptr<LogFile>& new_file);

// for tests
#ifndef NDEBUG
    void TEST_Append_RandomData();
    void TEST_Truncate_RandomLen();
#endif

private:
    static std::string makeFilePath(const std::string& path, uint64_t seq,
                                    uint64_t index);

    Status loadIndexes();
    Status traverse(uint32_t& offset);
    Status backup();
    Status recover(bool allow_corrupt);

    Status readFooter(uint32_t* index_ofset) const;
    Status writeFooter(uint32_t index_offset);
    Status readRecord(off_t offset, Record* rec, std::vector<char>* payload) const;
    Status writeRecord(RecordType type, const ::google::protobuf::Message& msg);

private:
    const uint64_t seq_ = 0;
    const uint64_t index_ = 0;
    const std::string file_path_;
    const bool readonly_ = false;

    int fd_ = -1;
    off_t file_size_ = 0;
    FILE* writer_ = nullptr;
    std::vector<char> write_buf_;

    LogIndex log_index_;
};

using LogFilePtr = std::unique_ptr<LogFile>;

} /* namespace storage */
} /* namespace impl */
} /* namespace raft */
} /* namespace jimkv */
