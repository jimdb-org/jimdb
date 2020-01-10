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

#include <string>
#include <map>

#include "base/status.h"

#include "write_batch_impl.h"
#include "db/db.h"

namespace jimkv {
namespace ds {
namespace db {

struct WALOptions {
    bool disabled = false;
    size_t max_file_size = 32 * 1024 * 1024;
};

class WALWriter;

class WALManager {
public:
    WALManager(const WALOptions& opt, std::string path);

    Status Open(uint64_t start_index, std::vector<std::string>& wals);

    Status Append(const MassTreeBatch& batch);
    Status Sync();
    Status Rotate(uint64_t index);
    Status Compact(uint64_t index);

    Status Truncate();
    Status Destory();

    Status CopyTo(const std::string& dest_path);

private:
    Status listAll();

private:
    const WALOptions opt_;
    const std::string path_;

    std::vector<std::pair<uint64_t, std::string>> wal_files_;
    std::unique_ptr<WALWriter> writer_;
};

class WALWriter {
public:
    explicit WALWriter(std::string filepath, size_t buffer_size = 0);
    ~WALWriter();

    Status Open();
    Status Append(const MassTreeBatch& batch);

    Status Sync();

    size_t FileSize() const { return file_size_; }

private:
    const std::string path_;
    FILE *fp_ = nullptr;
    size_t file_size_ = 0;
};

class WALReader {
public:
    explicit WALReader(std::string filepath, size_t buffer_size = 0);
    ~WALReader();

    Status Open();
    Status Next(MassTreeBatch& batch, bool& over);

private:
    const std::string path_;
    FILE *fp_ = nullptr;
};

} // namespace db
} // namespace ds
} // namespace jimkv
