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

#include "jimkv_kv_context.h"

#include "jimkv_client.h"
#include "jimkv_cluster.h"
#include "jimkv_log.h"
#include "redis_cmd_table.h"

namespace jimkv {
namespace sdk {
namespace kv {

JimStatus JimKVContext::Start(int thread_num) {
    RedisCmdTable::Instance().Init();
    JimClient::Instance().Start(thread_num, std::string("kv"));
    return JimStatus::OK;
}

JimStatus JimKVContext::Stop() {
    JimClient::Instance().Stop();
    return JimStatus::OK;
}

TableClientPtr JimKVContext::AddTable(const JimKVOption& option) {
    std::string tl_key = std::to_string(option.cluster_id) + "-"
        + std::to_string(option.db_id) + "-"
        + std::to_string(option.table_id);

    auto it = table_clients_.find(tl_key);
    if (it != table_clients_.end()) {
        return it->second;
    }

    auto client = NewJimTableClient(option);
    JimStatus st = client->Init();
    if (st != JimStatus::OK) {
        CBLOG_ERROR("add table db_id: %" PRIu64 ", table_id: %" PRIu64 " failed",
            option.db_id, option.table_id);
        return nullptr;
    }
    table_clients_[tl_key] = client;
    return client;
}

void JimKVContext::RemoveTable(uint64_t cluster_id, uint64_t db_id, uint64_t table_id) {
    std::string tl_key = std::to_string(cluster_id) + "-"
        + std::to_string(db_id) + "-"
        + std::to_string(table_id);

    auto it = table_clients_.find(tl_key);
    if (it != table_clients_.end()) {
        //auto client = it->second;
        ClusterPtr cluster = JimClient::Instance().GetCluster(cluster_id);
        //TODO ....
        table_clients_.erase(it);
    }
}

TableClientPtr JimKVContext::TableClient(uint64_t cluster_id, uint64_t db_id, uint64_t table_id) {
    std::string tl_key = std::to_string(cluster_id) + "-"
        + std::to_string(db_id) + "-"
        + std::to_string(table_id);

    auto it = table_clients_.find(tl_key);
    if (it != table_clients_.end()) {
        return it->second;
    }
    return nullptr;
}

KVReplyPtr JimKVContext::CommandExec(uint64_t cluster_id, uint64_t db_id, uint64_t table_id,
    const ArrayString& args, int timeout, AsyncFn callback, VoidPtr user_data)
{
    KVReplyPtr reply;
    std::string tl_key = std::to_string(cluster_id) + "-"
        + std::to_string(db_id) + "-"
        + std::to_string(table_id);

    auto it = table_clients_.find(tl_key);
    if (it != table_clients_.end()) {
        auto client = it->second;
        JimStatus st = client->CommandExec(args, timeout, reply, callback, user_data);
        if (st != JimStatus::OK) {
            CBLOG_DEBUG("command exec error");
        }
    }
    return reply;
}

KvCmdPtr JimKVContext::PipeCommandCreate(uint64_t cluster_id, uint64_t db_id, uint64_t table_id,
    AsyncFn callback, VoidPtr user_data)
{
    std::string tl_key = std::to_string(cluster_id) + "-"
        + std::to_string(db_id) + "-"
        + std::to_string(table_id);

    auto it = table_clients_.find(tl_key);
    if (it != table_clients_.end()) {
        auto client = it->second;
        auto cmd = client->PipeCommandCreate(callback, user_data);
        cmd->set_table_client(std::static_pointer_cast<void>(client));
        return cmd;
    }
    return nullptr;
}

JimStatus JimKVContext::PipeCommandAppend(KvCmdPtr pipe_cmd, const ArrayString& args) {
    return pipe_cmd->append_sub_command(args);
}

JimStatus JimKVContext::PipeCommandExec(KvCmdPtr pipe_cmd, ArrayKVReplyPtr& reply, int timeout) {
    auto client = std::static_pointer_cast<JimTableClient>(pipe_cmd->table_client());
    return client->PipeCommandExec(pipe_cmd, reply, timeout);
}

} //namespace kv
} //namespace sdk
} //namespace jimkv
