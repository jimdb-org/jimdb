
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
//

_Pragma("once");

#include <atomic>

namespace jim {
namespace sdk {

class JimUuid {
public:
    static JimUuid& Instance() {
        static JimUuid uuid;
        return uuid;
    }

    ~JimUuid() = default;
    JimUuid(const JimUuid&) = delete;
    JimUuid& operator=(const JimUuid&) = delete;

    uint64_t id_generate() {
        return seq_++;
    }

private:
    JimUuid() = default;
private:
    std::atomic<uint64_t> seq_ = {0};
};
} //namespace sdk
} //namespace jim
