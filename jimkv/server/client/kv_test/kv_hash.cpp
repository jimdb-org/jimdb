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


#include "kv_hash.h"

#include "jimkv_command.h"
#include "jimkv_kv_context.h"

KVReplyPtr KvHashTest::del(int timeout) {
    ArrayString args = {"del", "jimkv_kv_hash"};

    auto &table = JimKVContext::Instance();
    return table.CommandExec(cluster_id_, db_id_, table_id_, args, timeout);
}

KVReplyPtr KvHashTest::hget(int timeout) {
    ArrayString args = {"hget", "jimkv_kv_hash", "field_1"};

    auto &table = JimKVContext::Instance();
    return table.CommandExec(cluster_id_, db_id_, table_id_, args, timeout);
}

KVReplyPtr KvHashTest::hset(int timeout) {
    ArrayString args = {"hset", "jimkv_kv_hash", "field_1", "jimkv_kv_hash_field_1_value"};

    auto &table = JimKVContext::Instance();
    return table.CommandExec(cluster_id_, db_id_, table_id_, args, timeout);
}

KVReplyPtr KvHashTest::hmget(int timeout) {
    ArrayString args = {"hmget", "jimkv_kv_hash"};

    std::string field = "field_";
    for (int i=0; i<10; i++) {
        auto f = field + std::to_string(i);
        args.push_back(f);
    }

    auto &table = JimKVContext::Instance();
    return table.CommandExec(cluster_id_, db_id_, table_id_, args, timeout);
}

KVReplyPtr KvHashTest::hmset(int timeout) {
    ArrayString args = {"hmset", "jimkv_kv_hash"};

    std::string field = "field_";
    std::string value = "jimkv_kv_hash_field_";
    for (int i=0; i<10; i++) {
        auto f = field + std::to_string(i);
        auto v = value + std::to_string(i) + "_value";
        args.push_back(f);
        args.push_back(v);
    }

    auto &table = JimKVContext::Instance();
    return table.CommandExec(cluster_id_, db_id_, table_id_, args, timeout);
}

KVReplyPtr KvHashTest::hgetall(int timeout) {
    ArrayString args = {"hgetall", "jimkv_kv_hash"};

    auto &table = JimKVContext::Instance();
    return table.CommandExec(cluster_id_, db_id_, table_id_, args, timeout);
}

KVReplyPtr KvHashTest::hdel(int timeout) {
    ArrayString args = {"hdel", "jimkv_kv_hash", "field_5"};

    auto &table = JimKVContext::Instance();
    return table.CommandExec(cluster_id_, db_id_, table_id_, args, timeout);
}

KVReplyPtr KvHashTest::exists(std::string key, int timeout) {
    ArrayString args = {"exists", key};

    auto &table = JimKVContext::Instance();
    return table.CommandExec(cluster_id_, db_id_, table_id_, args, timeout);
}

void KvHashTest::async_hget(int timeout, AsyncFn fn, VoidPtr data) {
    ArrayString args = {"hget", "jimkv_kv_hash", "field_1"};

    auto &table = JimKVContext::Instance();
    table.CommandExec(cluster_id_, db_id_, table_id_, args, timeout, fn, data);
}

void KvHashTest::async_hset(int timeout, AsyncFn fn, VoidPtr data) {
    ArrayString args = {"hset", "jimkv_kv_hash", "field_1", "jimkv_kv_hash_field_1_value"};

    auto &table = JimKVContext::Instance();
    table.CommandExec(cluster_id_, db_id_, table_id_, args, timeout, fn, data);
}


