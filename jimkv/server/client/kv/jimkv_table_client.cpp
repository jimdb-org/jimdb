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

#include "jimkv_table_client.h"

#include <chrono>

#include "jimkv_command.h"
#include "jimkv_log.h"
#include "jimkv_uuid.h"
#include "jimkv_string.h"
#include "redis_cmd_table.h"
#include "jimkv_client.h"
#include "jimkv_cluster.h"

#include "jimkv_kv_context.h"

namespace jimkv {
namespace sdk {
namespace kv {

JimTableClient::JimTableClient(const JimKVOption& option):
    cluster_id_(option.cluster_id),
    db_id_(option.db_id),
    table_id_(option.table_id),
    conn_count_(option.conn_count),
    master_address_(option.master_address),
    cache_api_init_(option.cache_api_init),
    cache_expire_(option.cache_expire),
    cache_max_memory_(option.cache_max_memory)
{

}

JimStatus JimTableClient::Init() {
    ClusterPtr cluster = JimClient::Instance().GetCluster(cluster_id_);
    if (cluster == nullptr) {
        cluster = JimClient::Instance().AddCluster(cluster_id_, master_address_, conn_count_);
        if (cluster == nullptr) {
            CBLOG_ERROR("add cluster: %" PRIu64 " failed", cluster_id_);
            return JimStatus::ERROR;
        }
    }

    if (!cluster->AddTable(db_id_, table_id_)) {
        CBLOG_ERROR("add table fail, db_id: %" PRIu64 ", table_id: %" PRIu64,
            db_id_, table_id_);
        return JimStatus::ERROR;
    }

    return JimStatus::OK;
}

JimStatus JimTableClient::CommandExec(const ArrayString &args, int timeout,
    KVReplyPtr &reply, AsyncFn callback, VoidPtr user_data)
{
    RedisCmdEntry *cmd_entry;
    if (!JimClient::Instance().IsRun()) {
        CBLOG_WARN("jimkv client is not start!");
        return JimStatus::ERROR;
    }

    auto& cmd_table = RedisCmdTable::Instance();
    cmd_entry = cmd_table.kv_cmd_get(args[0], args.size());
    if (cmd_entry == nullptr) {
        CBLOG_WARN("not support command: %s", args[0].c_str());
        print_args(args);
        return JimStatus::ERROR;
    }

    KvCmdPtr cmd = NewJimKVCommand(cluster_id_, db_id_, table_id_);
    if (cmd == nullptr) {
        CBLOG_ERROR("new command instance failed");
        print_args(args);
        return JimStatus::ERROR;
    }

    cmd->set_callback(callback, user_data);
    cmd->set_redis_cmd_entry(cmd_entry);

    if (cmd->encode(args) != JimStatus::OK) {
        CBLOG_ERROR("serialize failed");
        print_args(args);
        cmd->clear_sub_command();
        return JimStatus::ERROR;
    }

    if (!JimClient::Instance().SendCmd(cmd, timeout)) {
        CBLOG_WARN("%s command failed", args[0].c_str());
        cmd->clear_sub_command();
        return JimStatus::ERROR;
    }

    if (callback == nullptr) {
        auto ft = cmd->get_future();
        auto status = ft.wait_for(std::chrono::milliseconds(timeout));
        if (status != std::future_status::ready) {
            CBLOG_WARN("timeout for command: %s", args[0].c_str());
            print_args(args);
        }

        CBLOG_TRACE("command finished: %s", args[0].c_str());
        cmd->build_reply();
        auto replys = cmd->reply();
        if (!replys.empty()) {
            reply = std::static_pointer_cast<JimKVReply>(replys[0]);
        }
    }

    return JimStatus::OK;
}

KvCmdPtr JimTableClient::PipeCommandCreate(AsyncFn callback, VoidPtr user_data) {
    if (!JimClient::Instance().IsRun()) {
        CBLOG_WARN("jimkv client is not start!");
        return nullptr;
    }

    KvCmdPtr cmd = NewJimKVCommand(cluster_id_, db_id_, table_id_);
    cmd->set_relation(JimKVRelation::PARENT);
    cmd->set_callback(callback, user_data);
    return cmd;
}

JimStatus JimTableClient::PipeCommandAppend(KvCmdPtr pipe_command,
    const ArrayString& args)
{
    auto ret = pipe_command->append_sub_command(args);
    if (ret != JimStatus::OK) {
        pipe_command->clear_sub_command();
    }
    return ret;
}

JimStatus JimTableClient::PipeCommandExec(KvCmdPtr pipe_command,
    ArrayKVReplyPtr& reply, int timeout)
{
    if (!JimClient::Instance().SendCmd(pipe_command, timeout)) {
        CBLOG_ERROR("send command failed");
        pipe_command->clear_sub_command();
        return JimStatus::ERROR;
    }

    if (pipe_command->callback() == nullptr) {
        auto ft = pipe_command->get_future();
        auto status = ft.wait_for(std::chrono::milliseconds(timeout));
        if (status != std::future_status::ready) {
            CBLOG_WARN("pipeline timeout");
        }
        pipe_command->build_reply();

        auto replys = pipe_command->reply();
        for (auto &r: replys) {
            reply.push_back(std::static_pointer_cast<JimKVReply>(r));
        }
        pipe_command->clear_sub_command();
    }

    return JimStatus::OK;
}

} //namespace kv
} //namespace sdk
} //namespace jimkv
