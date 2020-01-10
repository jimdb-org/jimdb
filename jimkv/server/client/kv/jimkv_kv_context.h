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
//

_Pragma("once");

#include "jimkv_kv_command.h"
#include "jimkv_kv_common.h"
#include "jimkv_kv_reply.h"
#include "jimkv_table_client.h"

namespace jimkv {
namespace sdk {
namespace kv {

class JimKVContext final {
public:
    static JimKVContext& Instance() {
        static JimKVContext instance;
        return instance;
    }

    ~JimKVContext() = default;
    JimKVContext(const JimKVContext&) = delete;
    JimKVContext& operator=(const JimKVContext&) = delete;

    JimStatus Start(int thread_num);
    JimStatus Stop();

    TableClientPtr AddTable(const JimKVOption&);

    void RemoveTable(uint64_t cluster_id, uint64_t db_id, uint64_t table_id);

    TableClientPtr TableClient(uint64_t cluster_id, uint64_t db_id, uint64_t table_id);

    KVReplyPtr CommandExec(uint64_t cluster_id, uint64_t db_id, uint64_t table_id,
        const ArrayString& args, int timeout,
        AsyncFn callback = {}, VoidPtr user_data = nullptr);

    KvCmdPtr PipeCommandCreate(uint64_t cluster_id,uint64_t db_id, uint64_t table_id,
        AsyncFn callback = nullptr, VoidPtr user_data = nullptr);

    JimStatus PipeCommandAppend(KvCmdPtr pipe_cmd, const ArrayString& args);

    JimStatus PipeCommandExec(KvCmdPtr pipe_cmd, ArrayKVReplyPtr& reply, int timeout = 0);

private:
    JimKVContext() = default;
private:
    std::map<std::string, TableClientPtr> table_clients_; //key is cluster_id-db_id-table_id
};
} //namespace kv
} //namespace sdk
} //namespace jimkv
