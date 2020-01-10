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

#include <atomic>
#include <map>

namespace jimkv {
namespace raft {
namespace impl {

class ServerMutableOptions {
public:
    Status Set(const std::map<std::string, std::string>& opt_map);

    bool Electable() const { return electable_.load(); }

private:
    Status setElectable(const std::string& val);

private:
    std::atomic<bool> electable_ = { true };
};

} /* namespace impl */
} /* namespace raft */
} /* namespace jimkv */
