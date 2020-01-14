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

_Pragma("once");

#include <chrono>
#include <memory>
#include "base/status.h"
#include "raft/types.h"

#include "raft.pb.h"

namespace jim {
namespace raft {
namespace impl {

static const uint64_t kNoLimit = std::numeric_limits<uint64_t>::max();

using EntryPtr = std::shared_ptr<pb::Entry>;
using MessagePtr = std::shared_ptr<pb::Message>;
using TimePoint = std::chrono::time_point<std::chrono::steady_clock>;

enum class FsmState { kFollower = 0, kCandidate, kLeader, kPreCandidate };

std::string FsmStateName(FsmState state);

enum class ReplicaState { kProbe = 0, kReplicate, kSnapshot };

std::string ReplicateStateName(ReplicaState state);

// encode to pb  or  decode from pb
Status EncodePeer(const Peer& peer, pb::Peer* pb_peer);
Status DecodePeer(const pb::Peer& pb_peer, Peer* peer);

// encode to pb string or  decode from pb string
Status EncodeConfChange(const ConfChange& cc, std::string* pb_str);
Status DecodeConfChange(const std::string& pb_str, ConfChange* cc);

bool IsLocalMsg(MessagePtr& msg);
bool IsResponseMsg(MessagePtr& msg);

} /* namespace impl */
} /* namespace raft */
} /* namespace jim */
