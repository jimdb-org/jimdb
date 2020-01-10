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
#include <string>
#include <vector>

namespace jimkv {
namespace raft {

enum class PeerType : char {
    kNormal, //normal type
    kLearner //learner type
};

std::string PeerTypeName(PeerType type);

struct Peer {
    PeerType type = PeerType::kNormal;
    uint64_t node_id = 0;
    uint64_t peer_id = 0;

    std::string ToString() const;
};

std::string PeersToString(const std::vector<Peer>& peers);

enum class ConfChangeType : char {
    kAdd,          //add raft member
    kRemove,       //remove raft member
    kPromote       //promote leaner member to normal member
};

std::string ConfChangeTypeName(ConfChangeType type);

struct ConfChange {
    ConfChangeType type = ConfChangeType::kAdd;
    Peer peer;
    std::string context;

    std::string ToString() const;
};

} /* namespace raft */
} /* namespace jimkv */
