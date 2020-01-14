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

namespace jim {
namespace raft {

enum EntryFlags : uint32_t {
    kForceRotate = 1 << 0,
};

// set a flag on a flags group
inline void SetEntryFlag(uint32_t& flags_group, EntryFlags flag) {
    flags_group |= flag;
}

// check if has a flag
inline bool HasEntryFlag(uint32_t flags_group, EntryFlags flag) {
    return (flags_group & flag) != 0;
}

} /* namespace raft */
} /* namespace jim */
