// Copyright 2015 The etcd Authors
// Portions Copyright 2019 The JIMDB Authors.
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

#include "raft/status.h"

#include <sstream>

namespace jim {
namespace raft {

std::string ReplicaStatus::ToString() const {
    std::ostringstream ss;
    ss << "{";
    ss << "\"peer\": " << peer.ToString() << ", ";
    ss << "\"match\": " << match << ", ";
    ss << "\"commit\": " << commit << ", ";
    ss << "\"next\": " << next << ", ";
    ss << "\"inactive\": " << inactive_seconds << ", ";
    ss << "\"state\": \"" << state << "\"";
    ss << "}";
    return ss.str();
}

RaftStatus::RaftStatus(RaftStatus&& s) { *this = std::move(s); }

RaftStatus& RaftStatus::operator=(RaftStatus&& s) {
    if (this != &s) {
        node_id = std::move(s.node_id);
        leader = std::move(s.leader);
        term = std::move(s.term);
        index = std::move(s.index);
        commit = std::move(s.commit);
        applied = std::move(s.applied);
        state = std::move(s.state);
        replicas = std::move(s.replicas);
    }
    return *this;
}

std::string RaftStatus::ToString() const {
    std::ostringstream ss;
    ss << "{";
    ss << "\"node_id\": " << node_id << ", "
       << "\"leader\": " << leader << ", "
       << "\"term\": " << term << ", "
       << "\"index\": " << index << ", "
       << "\"commit\": " << commit << ", "
       << "\"applied\": " << applied << ", "
       << "\"state\": "
       << "\"" << state << "\", "
       << "\"replicas\":";
    ss << "[";
    for (auto it = replicas.cbegin(); it != replicas.cend(); ++it) {
        ss << "{\"" << it->first << "\": " << it->second.ToString() << "}";
        if (std::next(it) != replicas.cend()) {
            ss << ", ";
        }
    }
    ss << "]}";
    return ss.str();
}

} /* namespace raft */
} /* namespace jim */
