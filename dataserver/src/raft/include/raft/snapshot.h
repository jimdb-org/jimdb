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

_Pragma("once");

#include "base/status.h"

namespace jim {
namespace raft {

class Snapshot {
public:
    Snapshot() = default;
    virtual ~Snapshot() = default;

    Snapshot(const Snapshot&) = delete;
    Snapshot& operator=(const Snapshot&) = delete;

    virtual Status Next(std::string* data, bool* over) = 0;

    virtual Status Context(std::string* context) = 0;

    virtual uint64_t ApplyIndex() = 0;

    virtual void Close() = 0;
};

} /* namespace raft */
} /* namespace jim */
