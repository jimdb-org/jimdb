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
//
#include "jim_kv_command.h"

#include <arpa/inet.h>

#include "dspb/kv.pb.h"
#include "dspb/api.pb.h"
#include "dspb/function.pb.h"
#include "dspb/error.pb.h"

#include "jim_hash.h"
#include "jim_common.h"
#include "jim_log.h"
#include "jim_string.h"
#include "jim_cluster.h"
#include "jim_kv_serialize.h"

namespace jim {
namespace sdk {
namespace kv {

JimKVCommand::JimKVCommand(uint64_t cluster_id, uint64_t db_id, uint64_t table_id):
    JimCommand(cluster_id, db_id, table_id)
{

}

JimStatus JimKVCommand::encode(const ArrayString& args) {
    JimStatus res = JimStatus::ERROR;
    RedisCmdEntry* cmd_entry = RedisCmdTable::Instance().kv_cmd_get(args[0], static_cast<int>(args.size()));
    if (cmd_entry == nullptr) {
        CBLOG_ERROR("don't support command: %.*s", static_cast<int>(args[0].size()), args[0].c_str());
        return JimStatus::ERROR;
    }
    set_redis_cmd_entry(cmd_entry);
    std::vector<ArrayString> split_args;
    if (cmd_entry->split) {
        res = cmd_entry->split(args, split_args);
        if (res == JimStatus::ONE_KEY) {
            res = cmd_entry->encode(shared_from_this(), args);
            set_relation(JimKVRelation::CHILD);
        } else if (res == JimStatus::OK) {
            set_relation(JimKVRelation::PARENT);
            for (auto& arg: split_args) {
                KvCmdPtr child_cmd = NewJimKVCommand(cluster_id_, db_id_, table_id_);
                if (child_cmd == nullptr) {
                    CBLOG_ERROR("new JimKVCommand failed");
                    return JimStatus::ERROR;
                }

                res = child_cmd->encode(arg);
                if (res != JimStatus::OK) {
                    CBLOG_ERROR("new JimKVCommand failed");
                    return JimStatus::ERROR;
                }

                child_cmd->set_relation(JimKVRelation::CHILD);
                child_cmd->set_parent(shared_from_this());
                child_cmd->set_callback(callback_, user_data_);
                sub_commands_.push_back(child_cmd);
                sub_commands_count_ ++;
            }
        } else {
            return res;
        }
    } else {
        res = cmd_entry->encode(shared_from_this(), args);
        set_relation(JimKVRelation::CHILD);
    }
    return res;
}

JimStatus JimKVCommand::hmget_build_reply() {
    return mget_build_reply();
}

JimStatus JimKVCommand::hmset_build_reply() {
    KVReplyPtr reply = NewJimKVReply();

    for(CmdPtr cmd: sub_commands_) {
        KVReplyPtr child_reply = NewJimKVReply();
        KvCmdPtr kv_cmd = std::dynamic_pointer_cast<JimKVCommand>(cmd);
        kv_cmd->redis_cmd_entry()->build_reply(kv_cmd->response(), child_reply);

        if (child_reply->type() == JimKVReplyType::REPLY_ERROR) {//return error if one sub command error
            reply->set_type(JimKVReplyType::REPLY_ERROR);
            reply->set_buf(child_reply->buf());

            reply_.push_back(std::static_pointer_cast<void>(reply));
            return JimStatus::OK;
        }
    }

    reply->set_type(JimKVReplyType::REPLY_STATUS);
    reply->set_buf(std::string("OK"));

    reply_.push_back(std::static_pointer_cast<void>(reply));
    return JimStatus::OK;
}

JimStatus JimKVCommand::hdel_build_reply() {
    int hdel_count = 0;
    KVReplyPtr reply = NewJimKVReply();

    for(CmdPtr cmd: sub_commands_) {
        KVReplyPtr child_reply = NewJimKVReply();
        KvCmdPtr kv_cmd = std::dynamic_pointer_cast<JimKVCommand>(cmd);
        kv_cmd->redis_cmd_entry()->build_reply(kv_cmd->response(), child_reply);
        if (child_reply->type() != JimKVReplyType::REPLY_ERROR) {//return error if one sub command error
            ++hdel_count;
        }
    }

    reply->set_type(JimKVReplyType::REPLY_INTEGER);
    reply->set_integer(hdel_count);

    reply_.push_back(std::static_pointer_cast<void>(reply));
    return JimStatus::OK;
}

JimStatus JimKVCommand::mget_build_reply() {
    KVReplyPtr reply = NewJimKVReply();
    reply->set_type(JimKVReplyType::REPLY_ARRAY);

    for(CmdPtr cmd: sub_commands_) {
        KVReplyPtr child_reply = NewJimKVReply();
        KvCmdPtr kv_cmd = std::dynamic_pointer_cast<JimKVCommand>(cmd);

        kv_cmd->redis_cmd_entry()->build_reply(kv_cmd->response(), child_reply);
        reply->add_element(child_reply);
    }

    reply_.push_back(std::static_pointer_cast<void>(reply));
    return JimStatus::OK;
}

JimStatus JimKVCommand::mset_build_reply() {
    KVReplyPtr reply = NewJimKVReply();

    for(CmdPtr cmd: sub_commands_) {
        KVReplyPtr child_reply = NewJimKVReply();
        KvCmdPtr kv_cmd = std::dynamic_pointer_cast<JimKVCommand>(cmd);
        kv_cmd->redis_cmd_entry()->build_reply(kv_cmd->response(), child_reply);

        if (child_reply->type() == JimKVReplyType::REPLY_ERROR) {
            reply->set_type(JimKVReplyType::REPLY_ERROR);
            reply->set_buf(child_reply->buf());

            reply_.push_back(std::static_pointer_cast<void>(reply));
            return JimStatus::ERROR;
        }
    }

    reply->set_type(JimKVReplyType::REPLY_STATUS);
    reply->set_buf(std::string("OK"));

    reply_.push_back(std::static_pointer_cast<void>(reply));
    return JimStatus::OK;
}

JimStatus JimKVCommand::del_build_reply() {
    int del_count = 0;

    KVReplyPtr reply = NewJimKVReply();
    reply->set_type(JimKVReplyType::REPLY_INTEGER);

    for(CmdPtr cmd: sub_commands_) {
        KVReplyPtr child_reply = NewJimKVReply();
        KvCmdPtr kv_cmd = std::dynamic_pointer_cast<JimKVCommand>(cmd);
        kv_cmd->redis_cmd_entry()->build_reply(kv_cmd->response(), child_reply);

        if (child_reply->type() != JimKVReplyType::REPLY_ERROR) {
            ++del_count;
        }
    }

    reply->set_integer(del_count);
    reply_.push_back(std::static_pointer_cast<void>(reply));
    return JimStatus::OK;
}

JimStatus JimKVCommand::hgetall_build_reply() {
    KVReplyPtr reply = NewJimKVReply();
    redis_cmd_entry_->build_reply(response_, reply);
    reply_.push_back(std::static_pointer_cast<void>(reply));
    return JimStatus::OK;
}

JimStatus JimKVCommand::pipeline_build_reply() {
    for (CmdPtr cmd: sub_commands_) {
        KVReplyPtr child_reply = NewJimKVReply();

        KvCmdPtr kv_cmd = std::dynamic_pointer_cast<JimKVCommand>(cmd);
        kv_cmd->redis_cmd_entry()->build_reply(kv_cmd->response(), child_reply);
        reply_.push_back(std::static_pointer_cast<void>(child_reply));
    }
    return JimStatus::OK;
}

JimStatus JimKVCommand::one_key_build_reply() {
    KVReplyPtr pa_reply = nullptr;
    KVReplyPtr child_reply = NewJimKVReply();
    redis_cmd_entry()->build_reply(response_, child_reply);

    switch(redis_cmd_entry_->cmd_type) {
    case RedisCmdType::MGET:/*fall-thru*/
    case RedisCmdType::HMGET:
        pa_reply = NewJimKVReply();
        pa_reply->set_type(JimKVReplyType::REPLY_ARRAY);
        pa_reply->add_element(child_reply);
        reply_.push_back(std::static_pointer_cast<void>(pa_reply));
        break;
    default:
        reply_.push_back(std::static_pointer_cast<void>(child_reply));
        break;
    }
    return JimStatus::OK;
}

JimStatus JimKVCommand::build_reply() {
    JimStatus res = JimStatus::ERROR;
    if (relation_ == JimKVRelation::PARENT) {
        if (redis_cmd_entry_ == nullptr) {
            res = pipeline_build_reply();
        } else {
            switch (redis_cmd_entry_->cmd_type) {
            case RedisCmdType::HMGET:
                res = hmget_build_reply();
                break;
            case RedisCmdType::HMSET:
                res = hmset_build_reply();
                break;
            case RedisCmdType::HDEL:
                res = hdel_build_reply();
                break;
            case RedisCmdType::MGET:
                res = mget_build_reply();
                break;
            case RedisCmdType::MSET:
                res = mset_build_reply();
                break;
            case RedisCmdType::DEL:
                res = del_build_reply();
                break;
            default:
                CBLOG_ERROR("not expected cmd type: %d", redis_cmd_entry_->cmd_type);
                break;
            }
        }
    } else if (relation_ == JimKVRelation::CHILD) {
        res = one_key_build_reply();
    }
    return res;
}

void JimKVCommand::net_callback() {
    KvCmdPtr p = std::dynamic_pointer_cast<JimKVCommand>(parent());
    if (p != nullptr) {
        if (p->is_finished(1)) {
            if (p->callback()) {
                p->build_reply();
                p->callback()(p->reply(), p->user_data());
                p->clear_sub_command();
            } else {
                p->finished();
            }
            CBLOG_DEBUG("sub command finished, origin key: %s", origin_key_.c_str());
        } else {
            CBLOG_DEBUG("sub command finished, origin key: %s", origin_key_.c_str());
        }
    } else {
        if (callback_) {
            build_reply();
            callback_(reply(), user_data());
        } else {
            finished();
        }

        CBLOG_DEBUG("command finished, origin key: %s", origin_key_.c_str());
        sub_commands_.clear();
    }
}

JimStatus JimKVCommand::append_sub_command(const ArrayString& args) {
    JimStatus res = JimStatus::ERROR;

    auto& cmd_table = RedisCmdTable::Instance();
    auto cmd_entry = cmd_table.kv_cmd_get(args[0], static_cast<int>(args.size()));
    if (cmd_entry == nullptr) {
        CBLOG_WARN("not support command: %.*s",
            static_cast<int>(args[0].size()), args[0].c_str());
        print_args(args);
        return JimStatus::ERROR;
    }
    if(is_multi_key_cmd(cmd_entry)) {
        CBLOG_ERROR("not support multi key command for sub command");
        return JimStatus::ERROR;
    }
    KvCmdPtr child_cmd = NewJimKVCommand(cluster_id_, db_id_, table_id_);
    if (child_cmd == nullptr) {
        CBLOG_ERROR("new command instance failed");
        print_args(args);
        return JimStatus::ERROR;
    }
    res = child_cmd->encode(args);
    if (res != JimStatus::OK) {
        CBLOG_ERROR("encode command failed");
        print_args(args);
        child_cmd->clear_sub_command();
        return res;
    }
    child_cmd->set_relation(JimKVRelation::CHILD);
    //maybe compile question
    child_cmd->set_parent(shared_from_this());
    sub_commands_.push_back(child_cmd);
    sub_commands_count_ ++;
    return JimStatus::OK;
}

} //namespace kv
} //namespace sdk
} //namespace jim
