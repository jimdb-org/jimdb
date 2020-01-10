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

#include "db/db_manager.h"
#include "raft/server.h"

namespace jimkv {
namespace ds {

namespace storage {
class MetaStore;
}

namespace master {
class MasterClient;
}

namespace server {

class Worker;
class RunStatus;
class RangeServer;
class RunStatus;
class RPCServer;

struct ContextServer {
    uint64_t node_id = 0;

    Worker *worker = nullptr;

    RunStatus *run_status = nullptr;
    RangeServer *range_server = nullptr;
    master::MasterClient *master_client = nullptr;
    RPCServer *rpc_server = nullptr;

    db::DBManager* db_manager = nullptr;

    storage::MetaStore *meta_store = nullptr;

    raft::RaftServer *raft_server = nullptr;
};

}  // namespace server
}  // namespace ds
}  // namespace jimkv
