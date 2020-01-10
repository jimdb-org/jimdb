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

#include "meta_file.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <cstdio>

#include "base/byte_order.h"
#include "base/fs_util.h"

namespace jimkv {
namespace raft {
namespace impl {
namespace storage {

MetaFile::MetaFile(const std::string& path) : path_(JoinFilePath({path, "META"})) {}

MetaFile::~MetaFile() { Close(); }

Status MetaFile::Open(bool read_only) {
    int oflag = read_only ? O_RDONLY : (O_CREAT | O_RDWR);
    fd_ = ::open(path_.c_str(), oflag, 0644);
    if (-1 == fd_) {
        return Status(Status::kIOError, "open meta file", strErrno(errno));
    }
    return Status::OK();
}

Status MetaFile::Close() {
    if (fd_ > 0) {
        ::close(fd_);
        fd_ = -1;
    }
    return Status::OK();
}

Status MetaFile::Sync() {
    if (::fsync(fd_) == -1) {
        return Status(Status::kIOError, "sync meta file", strErrno(errno));
    }
    return Status::OK();
}

Status MetaFile::Destroy() {
    auto s = Close();
    if (!s.ok()) {
        return s;
    }
    int ret = std::remove(path_.c_str());
    if (ret != 0) {
        return Status(Status::kIOError, "remove meta file", std::to_string(ret));
    } else {
        return Status::OK();
    }
}

Status MetaFile::Load(pb::HardState* hs, pb::TruncateMeta* tm, uint64_t *inherit_index) {
    // get file size
    struct stat sb;
    memset(&sb, 0, sizeof(sb));
    int ret = ::fstat(fd_, &sb);
    if (-1 == ret) {
        return Status(Status::kIOError, "stat", strErrno(errno));
    }
    switch (sb.st_size) {
        case 0:
        case kHardStateSize:
        case kHardStateSize + kTruncateMetaSize:
        case kHardStateSize + kTruncateMetaSize + kInheritIndexSize:
            break;
        default:
            return Status(Status::kIOError, "invalid meta file size", std::to_string(sb.st_size));
    }

    char buf[kHardStateSize + kTruncateMetaSize + kInheritIndexSize] = {'\0'};
    ssize_t nbytes_read = ::pread(fd_, buf, kHardStateSize + kTruncateMetaSize + kInheritIndexSize, 0);
    if (nbytes_read != sb.st_size) {
        return Status(Status::kIOError, "load meta", strErrno(errno));
    }

    // parse hardstate
    uint64_t term = 0;
    uint64_t commit = 0;
    uint64_t vote = 0;
    memcpy(&term, buf, 8);
    memcpy(&commit, buf + 8, 8);
    memcpy(&vote, buf + 16, 8);
    hs->set_term(be64toh(term));
    hs->set_commit(be64toh(commit));
    hs->set_vote(be64toh(vote));

    // parse truncate meta
    uint64_t mindex = 0;
    uint64_t mterm = 0;
    memcpy(&mindex, buf + 24, 8);
    memcpy(&mterm, buf + 32, 8);
    tm->set_index(be64toh(mindex));
    tm->set_term(be64toh(mterm));

    // parse inherit index
    memcpy(inherit_index, buf + 40, 8);
    *inherit_index = be64toh(*inherit_index);
    return Status::OK();
}

Status MetaFile::SaveHardState(const pb::HardState& hs) {
    char buf[kHardStateSize] = {'\0'};
    uint64_t term = htobe64(hs.term());
    uint64_t commit = htobe64(hs.commit());
    uint64_t vote = htobe64(hs.vote());
    memcpy(buf, &term, 8);
    memcpy(buf + 8, &commit, 8);
    memcpy(buf + 16, &vote, 8);

    ssize_t ret = ::pwrite(fd_, buf, kHardStateSize, 0);
    if (ret != kHardStateSize) {
        return Status(Status::kIOError, "write hard state", strErrno(errno));
    }
    return Status::OK();
}

Status MetaFile::SaveTruncMeta(const pb::TruncateMeta& tm) {
    char buf[kTruncateMetaSize] = {'\0'};
    uint64_t index = htobe64(tm.index());
    uint64_t term = htobe64(tm.term());
    memcpy(buf, &index, 8);
    memcpy(buf + 8, &term, 8);

    ssize_t ret = ::pwrite(fd_, buf, kTruncateMetaSize, kHardStateSize);
    if (ret != kTruncateMetaSize) {
        return Status(Status::kIOError, "write trunc meta", strErrno(errno));
    }
    return Status::OK();
}

Status MetaFile::SaveInheritIndex(uint64_t index) {
    index = htobe64(index);
    auto ret = ::pwrite(fd_, &index, sizeof(index), kHardStateSize + kTruncateMetaSize);
    if (ret != sizeof(index)) {
        return Status(Status::kIOError, "write inherit index", strErrno(errno));
    }
    return Status::OK();
}

} /* namespace storage */
} /* namespace impl */
} /* namespace raft */
} /* namespace jimkv */
