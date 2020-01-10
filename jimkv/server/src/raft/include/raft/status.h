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

_Pragma("once");

#include <stdint.h>
#include <string>
#include <map>

#include "raft/types.h"

namespace jimkv {
namespace raft {

struct ServerStatus {
    uint64_t total_snap_applying = 0;
    uint64_t total_snap_sending = 0;
    uint64_t total_rafts_count = 0;
};

struct ReplicaStatus {
    Peer peer;
    uint64_t match = 0;
    uint64_t commit = 0;
    uint64_t next = 0;
    int inactive_seconds = 0;
    bool snapshotting = false;
    std::string state;

    std::string ToString() const;
};

struct RaftStatus {
    uint64_t node_id = 0;
    uint64_t leader = 0;
    uint64_t term = 0;
    uint64_t index = 0;  // log index
    uint64_t commit = 0;
    uint64_t applied = 0;
    std::string state;
    // key: node_id
    std::map<uint64_t, ReplicaStatus> replicas;

    RaftStatus() = default;
    RaftStatus& operator=(const RaftStatus& s) = default;

    RaftStatus(RaftStatus&& s);
    RaftStatus& operator=(RaftStatus&& s);

    std::string ToString() const;
};

} /* namespace raft */
} /* namespace jimkv */
