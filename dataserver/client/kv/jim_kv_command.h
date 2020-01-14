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

_Pragma("once");

#include <deque>
#include <memory>

#include "asio.hpp"

#include "dspb/kv.pb.h"
#include "dspb/api.pb.h"
#include "dspb/function.pb.h"
#include "dspb/error.pb.h"

#include "jim_command.h"
#include "jim_kv_common.h"
#include "jim_kv_reply.h"
#include "redis_cmd_table.h"

namespace jim {
namespace sdk {
namespace kv {

class JimKVCommand:
    public JimCommand,
    public std::enable_shared_from_this<JimKVCommand>
{
public:
    JimKVCommand(uint64_t cluster_id, uint64_t db_id, uint64_t table_id);

    ~JimKVCommand() = default;

    JimKVCommand(const JimKVCommand&) = delete;
    JimKVCommand& operator=(const JimKVCommand&) = delete;

    void set_relation(const JimKVRelation relation) {relation_ = relation;}
    JimKVRelation relation() {return relation_;}

    void set_redis_cmd_entry(const RedisCmdEntry* redis_cmd_entry) {
        redis_cmd_entry_ = redis_cmd_entry;
    }

    const RedisCmdEntry* redis_cmd_entry() {return redis_cmd_entry_;}

    void set_origin_key(const std::string& origin_key) {
        origin_key_ = origin_key;
    }
    const std::string& origin_key() {return origin_key_;}

    const ArrayVoidPtr& reply() {return reply_;};

    void set_table_client(VoidPtr table_client) {
        table_client_ = table_client;
    }
    VoidPtr table_client() {return table_client_;}

    JimStatus encode(const ArrayString& args);

    void net_callback() override;

    JimStatus build_reply();

    JimStatus append_sub_command(const ArrayString& args);

private:
    JimStatus hmget_build_reply();
    JimStatus hmset_build_reply();
    JimStatus hdel_build_reply();
    JimStatus mget_build_reply();
    JimStatus mset_build_reply();
    JimStatus del_build_reply();
    JimStatus hgetall_build_reply();
    JimStatus pipeline_build_reply();
    JimStatus one_key_build_reply();

private:
    JimKVRelation relation_;

    const RedisCmdEntry* redis_cmd_entry_ = nullptr;

    std::string origin_key_;

    VoidPtr table_client_;

    ArrayVoidPtr reply_;
};

using KvCmdPtr = std::shared_ptr<JimKVCommand>;

inline KvCmdPtr NewJimKVCommand(uint64_t cluster_id, uint64_t db_id, uint64_t table_id) {
    return std::make_shared<JimKVCommand>(cluster_id, db_id, table_id);
}

} //namespace kv
} //namespace sdk
} //namespace jim
