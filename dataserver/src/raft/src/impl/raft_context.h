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

#include "snapshot/manager.h"
#include "transport/transport.h"
#include "work_thread.h"
#include "mutable_options.h"
#include "server_impl.h"

namespace jim {
namespace raft {
namespace impl {

struct RaftContext {
    RaftServerImpl *server_ = nullptr;
    WorkThread *consensus_thread = nullptr;
    SnapshotManager *snapshot_manager = nullptr;
    transport::Transport *msg_sender = nullptr;
    ServerMutableOptions *mutable_options = nullptr;
};

} /* namespace impl */
} /* namespace raft */
} /* namespace jim */
