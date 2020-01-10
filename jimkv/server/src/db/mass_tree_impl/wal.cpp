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

#include "wal.h"

#include <errno.h>
#include <unistd.h>
#include <sstream>
#include <iomanip>
#include <dirent.h>

#include "base/fs_util.h"
#include "base/byte_order.h"

namespace jimkv {
namespace ds {
namespace db {

static const char* kWALFileSuffix = ".wal";
static constexpr size_t kWALFilePrefixLen = 16;

static std::string makeWALFileName(const std::string& path, uint64_t index) {
    std::stringstream ss;
    ss << std::hex << std::setfill('0') << std::setw(kWALFilePrefixLen) << index;
    ss << kWALFileSuffix;
    return JoinFilePath({path, ss.str()});
}

static bool parseWALFileName(const std::string& name, uint64_t& index) {
    std::size_t pos = name.find(kWALFileSuffix);
    if (pos == std::string::npos) {
        return false;
    }
    if (pos != kWALFilePrefixLen) {
        return false;
    }
    // TODO: check endptr
    index = std::stoull(name, nullptr, 16);
    return index != 0;
}

struct WALRecordHeader {
    uint64_t index = 0;
    uint32_t count = 0;

    void Encode();
    void Decode();
}__attribute__((packed));

void WALRecordHeader::Encode() {
    index = htobe64(index);
    count = htobe32(count);
}

void WALRecordHeader::Decode() {
    index = be64toh(index);
    count = be32toh(count);
}


struct WALEntryHeader {
    uint8_t type = 0;
    uint8_t cf = 0;
    uint32_t key_size = 0;
    uint32_t value_size = 0;

    void Encode();
    void Decode();
}__attribute__((packed));

void WALEntryHeader::Encode() {
    key_size = htobe32(key_size);
    value_size = htobe32(value_size);
}

void WALEntryHeader::Decode() {
    key_size = be32toh(key_size);
    value_size = be32toh(value_size);
}

static size_t RecordSize(const MassTreeBatch& batch) {
    size_t record_size = sizeof(WALRecordHeader);
    for (const auto& ent : batch.Entries()) {
        record_size += sizeof(WALEntryHeader);
        record_size += ent.key.size();
        record_size += ent.value.size();
    }
    return record_size;
}

static void EncodeRecord(const MassTreeBatch& batch, std::string& buf) {
    buf.reserve(RecordSize(batch));

    {
        WALRecordHeader header;
        header.index = batch.GetIndex();
        header.count = batch.Entries().size();
        header.Encode();
        buf.append((const char *)&header, sizeof(header));
    }

    for (const auto& ent : batch.Entries()) {
        WALEntryHeader eheader;
        eheader.type = static_cast<uint8_t>(ent.type);
        eheader.cf = static_cast<uint8_t>(ent.cf);
        eheader.key_size = ent.key.size();
        eheader.value_size = ent.value.size();
        eheader.Encode();
        buf.append((const char *)&eheader, sizeof(eheader));
        if (!ent.key.empty()) {
            buf.append(ent.key);
        }
        if (!ent.value.empty()) {
            buf.append(ent.value);
        }
    }
}

WALManager::WALManager(const WALOptions& opt, std::string path) :
    opt_(opt),
    path_(std::move(path)) {
}

Status WALManager::Open(uint64_t start_index, std::vector<std::string>& wals) {
    if (!MakeDirAll(path_, 0755)) {
        return Status(Status::kIOError, "init wal path " + path_, strErrno(errno));
    }

    auto s = listAll();
    if (!s.ok()) {
        return s;
    }
    s = Compact(start_index);
    if (!s.ok()) {
        return s;
    }

    for (const auto& m : wal_files_) {
        wals.push_back(m.second);
    }
    return Status::OK();
}

Status WALManager::listAll() {
    std::vector<std::string> files;
    if (!ListDirFiles(path_, files)) {
        return Status(Status::kIOError, "list wal files at " + path_, strErrno(errno));
    }

    std::map<uint64_t, std::string> unique_wals;
    for (const auto& file: files) {
        uint64_t index = 0;
        if (parseWALFileName(file, index)) {
            auto res = unique_wals.emplace(index, JoinFilePath({path_, file}));
            if (!res.second) {
                return Status(Status::kDuplicate, "same index with wal", res.first->second);
            }
        }
    }

    for (const auto& m : unique_wals) {
        wal_files_.emplace_back(m.first, m.second);
    }

    return Status::OK();
}

Status WALManager::Append(const MassTreeBatch& batch) {
    if (batch.GetIndex() == 0) {
        return Status(Status::kInvalidArgument, "wal batch index", "0");
    }

    Status s;
    if (writer_ == nullptr || writer_->FileSize() >= opt_.max_file_size) {
        s = Rotate(batch.GetIndex());
        if (!s.ok()) {
            return s;
        }
    }
    return writer_->Append(batch);
};

Status WALManager::Sync() {
    if (writer_ != nullptr) {
        return writer_->Sync();
    }
    return Status::OK();
}

Status WALManager::Rotate(uint64_t index) {
    Status s;
    if (writer_ != nullptr) {
        s = writer_->Sync();
        if (!s.ok()) {
            return s;
        }
        writer_.reset(nullptr);
    }
    auto filename = makeWALFileName(path_, index);
    wal_files_.emplace_back(index, filename);
    writer_.reset(new WALWriter(filename));
    return writer_->Open();
}

Status WALManager::Compact(uint64_t index) {
    while (wal_files_.size() > 1 && index >= wal_files_[1].first) {
        auto file = wal_files_.begin()->second;
        auto ret = std::remove(file.c_str());
        if (ret != 0) {
            return Status(Status::kIOError, "compact wal " + file, strErrno(errno));
        }
        wal_files_.erase(wal_files_.begin());
    }
    return Status::OK();
}

Status WALManager::Truncate() {
    writer_.reset(nullptr);
    for (const auto& wal: wal_files_) {
        auto file = wal.second;
        auto ret = std::remove(file.c_str());
        if (ret != 0) {
            return Status(Status::kIOError, "truncate wal " + file, strErrno(errno));
        }
    }
    wal_files_.clear();
    return Status::OK();
}

Status WALManager::Destory() {
    writer_.reset(nullptr);
    if (!RemoveDirAll(path_.c_str())) {
        return Status(Status::kIOError, "remove wal path " + path_, strErrno(errno));
    }
    wal_files_.clear();
    return Status::OK();
}

Status WALManager::CopyTo(const std::string& dest_path) {
    auto s = writer_->Sync();
    if (!s.ok()) {
        return s;
    }
    writer_.reset(nullptr);

    if (!MakeDirAll(dest_path, 0755)) {
        return Status(Status::kIOError, "create wal path " + dest_path, strErrno(errno));
    }

    for (const auto& wal: wal_files_) {
        auto new_path = makeWALFileName(dest_path, wal.first);
        int ret = ::link(wal.second.c_str(), new_path.c_str());
        if (ret != 0) {
            return Status(Status::kIOError, "link " + wal.second + " to " + new_path, strErrno(errno));
        }
    }
    return Status::OK();
}

WALWriter::WALWriter(std::string filepath, size_t buffer_size) :
    path_(std::move(filepath)) {
    (void)buffer_size;
}

WALWriter::~WALWriter() {
    if (fp_ != nullptr) {
        ::fclose(fp_);
    }
}

Status WALWriter::Open() {
    fp_ = ::fopen(path_.c_str(), "w");
    if (fp_ == nullptr) {
        return Status(Status::kIOError, "create wal " + path_, strErrno(errno));
    }
    return Status::OK();
}


Status WALWriter::Append(const MassTreeBatch& batch) {
    std::string buf;
    EncodeRecord(batch, buf);
    auto ret = ::fwrite(buf.c_str(), buf.size(), 1, fp_);
    if (ret != 1) {
        return Status(Status::kIOError, "write wal record", strErrno(errno));
    }
    ret = ::fflush(fp_);
    if (ret != 0) {
        return Status(Status::kIOError, "flush wal " + path_, strErrno(errno));
    }
    file_size_ += buf.size();
    return Status::OK();
}

Status WALWriter::Sync() {
    auto ret = ::fsync(::fileno(fp_));
    if (ret < 0) {
        return Status(Status::kIOError, "sync wal " + path_, strErrno(errno));
    }
    return Status::OK();
}


WALReader::WALReader(std::string filepath, size_t buffer_size) :
    path_(std::move(filepath)) {
    (void)buffer_size;
}

Status WALReader::Open() {
    fp_ = ::fopen(path_.c_str(), "r");
    if (fp_ == nullptr) {
        return Status(Status::kIOError, "open wal " + path_, strErrno(errno));
    }
    return Status::OK();
}

Status WALReader::Next(MassTreeBatch& batch, bool& over) {
    WALRecordHeader rec_header;
    auto ret = ::fread(&rec_header, sizeof(rec_header), 1, fp_);
    if (ret != 1) {
        if (::feof(fp_) != 0) {
            over = true;
            return Status::OK();
        } else {
            return Status(Status::kIOError, "read wal record header", strErrno(errno));
        }
    }
    rec_header.Decode();

    batch.SetIndex(rec_header.index);
    batch.Reserve(rec_header.count);

    for (size_t i = 0; i < rec_header.count; ++i) {
        WALEntryHeader entry_header;
        ret = ::fread(&entry_header, sizeof(entry_header), 1, fp_);
        if (ret != 1) {
            return Status(Status::kIOError, "read wal entry header", strErrno(errno));
        }
        entry_header.Decode();
        auto type = static_cast<KvEntryType>(entry_header.type);
        auto cf = static_cast<CFType>(entry_header.cf);
        std::string key, value;
        if (entry_header.key_size > 0) {
            key.resize(entry_header.key_size);
            ret = ::fread((char *)key.data(), key.size(), 1, fp_);
            if (ret != 1) {
                return Status(Status::kIOError, "read wal entry key", strErrno(errno));
            }
        }
        if (entry_header.value_size > 0) {
            value.resize(entry_header.value_size);
            ret = ::fread((char *)value.data(), value.size(), 1, fp_);
            if (ret != 1) {
                return Status(Status::kIOError, "read wal value key", strErrno(errno));
            }
        }
        auto s = batch.Append(type, cf, key, value);
        if (!s.ok()) {
            return s;
        }
    }

    over = false;

    return Status::OK();
}

WALReader::~WALReader() {
    if (fp_ != nullptr) {
        ::fclose(fp_);
    }
}

} // namespace db
} // namespace ds
} // namespace jimkv
