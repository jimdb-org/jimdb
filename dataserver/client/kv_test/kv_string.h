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


_Pragma ("once");

#include <string>

#include "jim_kv_context.h"
#include "jim_kv_reply.h"

using namespace jim;
using namespace jim::sdk;
using namespace jim::sdk::kv;

class KvStringTest {
public:
    KvStringTest(uint64_t cluster_id, uint64_t db_id, uint64_t table_id) :
        cluster_id_(cluster_id), db_id_(db_id), table_id_(table_id)
    {}

    KVReplyPtr del(ArrayString &keys, int timeout = 100);

    KVReplyPtr get(std::string key, int timeout = 100);
    KVReplyPtr set(std::string key, std::string value, int type = 0, uint64_t expire = 100000, int timeout = 100);

    KVReplyPtr setex(std::string key, std::string value, uint64_t expire, int timeout = 100);
    KVReplyPtr psetex(std::string key, std::string value, uint64_t expire, int timeout = 100);

    KVReplyPtr mget(int timeout = 100);
    KVReplyPtr mset(int timeout = 100);

    KVReplyPtr strlen(std::string key, int timeout = 100);
    KVReplyPtr exists(std::string key, int timeout = 100);

    KVReplyPtr ttl(std::string key, int timeout = 100);
    KVReplyPtr pttl(std::string key, int timeout = 100);

    void async_get(std::string key, int timeout, AsyncFn fn, VoidPtr data);
    void async_set(std::string key, std::string value, int timeout, AsyncFn fn, VoidPtr data);

    void async_mget(int timeout, AsyncFn fn, VoidPtr data);
    void async_mset(int timeout, AsyncFn fn, VoidPtr data);
private:
    uint64_t cluster_id_;
    uint64_t db_id_;
    uint64_t table_id_;
};
