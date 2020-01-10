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
#include "base/status.h"

#include "../raft.pb.h"

namespace jimkv {
namespace raft {
namespace impl {
namespace storage {

class MetaFile {
public:
    explicit MetaFile(const std::string& path);
    ~MetaFile();

    MetaFile(const MetaFile&) = delete;
    MetaFile& operator=(const MetaFile&) = delete;

    Status Open(bool readonly = false);
    Status Close();
    Status Sync();
    Status Destroy();

    Status Load(pb::HardState* hs, pb::TruncateMeta* tm, uint64_t *inherit_index);
    Status Load(pb::HardState* hs, pb::TruncateMeta* tm);
    Status SaveHardState(const pb::HardState& hs);
    Status SaveTruncMeta(const pb::TruncateMeta& tm);
    Status SaveInheritIndex(uint64_t index);

private:
    constexpr static size_t kHardStateSize = 8 * 3;     // term(8) + commit(8) + vote(8)
    constexpr static size_t kTruncateMetaSize = 8 * 2;  // index(8) + term(8)
    constexpr static size_t kInheritIndexSize = 8;  // index(8)

private:
    const std::string path_;
    int fd_ = -1;
};

} /* namespace storage */
} /* namespace impl */
} /* namespace raft */
} /* namespace jimkv */
