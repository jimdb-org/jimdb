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

#include "jimkv_kv_common.h"
#include "jimkv_kv_command.h"
#include "jimkv_kv_reply.h"

namespace jimkv {
namespace sdk {
namespace kv {

class JimTableClient {
public:
    JimTableClient(const JimKVOption& option);

    ~JimTableClient() = default;

    JimTableClient(const JimTableClient&) = delete;
    JimTableClient& operator=(const JimTableClient&) = delete;

    JimStatus Init();
    JimStatus CommandExec(const ArrayString &args, int timeout, KVReplyPtr &reply,
        AsyncFn callback = nullptr, VoidPtr user_data = nullptr);

    KvCmdPtr PipeCommandCreate(AsyncFn callback = nullptr, VoidPtr user_data = nullptr);

    JimStatus PipeCommandAppend(KvCmdPtr pipe_cmd, const ArrayString &args);

    JimStatus PipeCommandExec(KvCmdPtr pipe_cmd, ArrayKVReplyPtr& reply, int timeout = 0);

    uint64_t cluster_id() {return cluster_id_;};

    uint64_t db_id() {return db_id_;};

    uint64_t table_id() {return table_id_;};

    uint32_t conn_count() { return conn_count_;}

private:
    uint64_t cluster_id_;
    uint64_t db_id_;
    uint64_t table_id_;
    uint32_t conn_count_;
    std::string master_address_;

    int cache_api_init_;
    int cache_expire_;
    uint64_t cache_max_memory_;
};

using TableClientPtr = std::shared_ptr<JimTableClient>;

inline TableClientPtr NewJimTableClient(const JimKVOption& option) {
    return std::make_shared<JimTableClient>(option);
}

} //namespace kv
} // namespace sdk
} // namespace jimkv
