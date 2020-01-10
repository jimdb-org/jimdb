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

#include "raft_types.h"
#include "snapshot/apply_task.h"
#include "snapshot/send_task.h"

namespace jimkv {
namespace raft {
namespace impl {

struct Ready {
    // committed entries, can apply to statemachine
    std::vector<EntryPtr> committed_entries;
    // failed entries
    std::deque<EntryPtr> failed_entries;

    // msgs to send
    std::vector<MessagePtr> msgs;

    // snapshot to send
    std::shared_ptr<SendSnapTask> send_snap;

    // snapshot to apply
    std::shared_ptr<ApplySnapTask> apply_snap;

    /* // change list about peers */
    /* std::vector<Peer> pendings_peers; */
    /* std::vector<DownPeers> down_peers; */
};

} /* namespace impl */
} /* namespace raft */
} /* namespace jimkv */
