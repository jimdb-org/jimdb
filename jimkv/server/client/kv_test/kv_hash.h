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


_Pragma ("once");

#include <string>

#include "jimkv_kv_context.h"
#include "jimkv_kv_reply.h"

using namespace jimkv;
using namespace jimkv::sdk;
using namespace jimkv::sdk::kv;

class KvHashTest {
public:
    KvHashTest(uint64_t cluster_id, uint64_t db_id, uint64_t table_id) :
        cluster_id_(cluster_id), db_id_(db_id), table_id_(table_id)
    {}
    KVReplyPtr del(int timeout = 100);

    KVReplyPtr hget(int timeout = 100);
    KVReplyPtr hset(int timeout = 100);

    KVReplyPtr hmget(int timeout = 100);
    KVReplyPtr hmset(int timeout = 100);

    KVReplyPtr hgetall(int timeout = 100);
    KVReplyPtr hdel(int timeout = 100);

    KVReplyPtr exists(std::string key, int timeout = 100);

    void async_hget(int timeout, AsyncFn fn, VoidPtr data);
    void async_hset(int timeout, AsyncFn fn, VoidPtr data);

private:
    uint64_t cluster_id_;
    uint64_t db_id_;
    uint64_t table_id_;
};

