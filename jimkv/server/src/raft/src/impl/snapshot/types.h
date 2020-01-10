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

#include <functional>
#include "base/status.h"

namespace jimkv {
namespace raft {
namespace impl {

struct SnapContext {
    uint64_t id = 0;
    uint64_t from = 0;
    uint64_t to = 0;
    uint64_t term = 0;
    uint64_t uuid = 0;
};

struct SnapResult {
    Status status;
    size_t blocks_count = 0;
    size_t bytes_count = 0;
};

using SnapReporter = std::function<void(const SnapContext&, const SnapResult&)>;

} /* namespace impl */
} /* namespace raft */
} /* namespace jimkv */
