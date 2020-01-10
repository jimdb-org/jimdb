// Copyright 2015 The etcd Authors
// Portions Copyright 2019 The JimDB Authors.
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

#include "raft/types.h"
#include "base/util.h"

#include <sstream>

namespace jimkv {
namespace raft {

std::string PeerTypeName(PeerType type) {
    switch (type) {
        case PeerType::kNormal:
            return "normal";
        case PeerType::kLearner:
            return "learner";
        default:
            return std::string("unknown(") + std::to_string(static_cast<int>(type)) + ")";
    }
}

std::string Peer::ToString() const {
    std::ostringstream ss;
    ss << "{";
    ss << "\"type\": \"" << PeerTypeName(type) << "\", ";
    ss << "\"node_id\": " << node_id << ", ";
    ss << "\"peer_id\": " << peer_id;
    ss << "}";
    return ss.str();
}

std::string PeersToString(const std::vector<Peer>& peers) {
    std::string ret = "[";
    for (size_t i = 0; i < peers.size(); ++i) {
        ret += peers[i].ToString();
        if (i != peers.size() - 1) {
            ret += ", ";
        }
    }
    ret += "]";
    return ret;
}

std::string ConfChangeTypeName(ConfChangeType type) {
    switch (type) {
        case ConfChangeType::kAdd:
            return "add";
        case ConfChangeType::kRemove:
            return "remove";
        case ConfChangeType::kPromote:
            return "promote";
        default:
            return std::string("unknown(") + std::to_string(static_cast<int>(type)) + ")";
    }
}

std::string ConfChange::ToString() const {
    std::ostringstream ss;
    ss << "{";
    ss << "\"type\": \"" << ConfChangeTypeName(type) << "\", ";
    ss << "\"peer\": " << peer.ToString() << ", ";
    ss << "\"context\": \"" << EncodeToHex(context) << "\"";
    ss << "}";
    return ss.str();
}

} /* namespace raft */
} /* namespace jimkv */
