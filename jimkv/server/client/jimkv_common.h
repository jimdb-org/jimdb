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
//

_Pragma("once");

#include <memory>
#include <string>
#include <vector>

namespace jimkv {
namespace sdk {

enum JimStatus {
    OK = 0,
    ERROR,
    EPIPEMAX,
    EXPIRED,
    ENOSUPPORT,
    ONE_KEY,
};

enum CmdRWFlag {
    READ = 0,
    WRITE,
    OTHER,
};

const std::string JIMKV_ETIMEOUT = "ERR: Timeout";

using VoidPtr = std::shared_ptr<void>;
using ArrayVoidPtr = std::vector<VoidPtr>;

using ArrayString = std::vector<std::string>;

} // namespace sdk
} // namespace jimkv

