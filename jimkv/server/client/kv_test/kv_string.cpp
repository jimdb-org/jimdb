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


#include "kv_string.h"

#include "jimkv_command.h"
#include "jimkv_kv_context.h"

KVReplyPtr KvStringTest::del(ArrayString &keys, int timeout) {
    keys.insert(keys.begin(), "del");

    auto &table = JimKVContext::Instance();
    return table.CommandExec(cluster_id_, db_id_, table_id_, keys, timeout);
}

KVReplyPtr KvStringTest::get(std::string key, int timeout) {
    ArrayString args = {"get", key};

    auto &table = JimKVContext::Instance();
    return table.CommandExec(cluster_id_, db_id_, table_id_, args, timeout);
}

KVReplyPtr KvStringTest::set(std::string key, std::string value, int type, uint64_t expire, int timeout) {
    ArrayString args = {"set", key, value};
    if (type != 0) {
        if (type == 1) {
            args.push_back("EX");
        } else {
            args.push_back("PX");
        }
        args.push_back(std::to_string(expire));
    }

    auto &table = JimKVContext::Instance();
    return table.CommandExec(cluster_id_, db_id_, table_id_, args, timeout);
}

KVReplyPtr KvStringTest::setex(std::string key, std::string value, uint64_t expire, int timeout) {
    ArrayString args = {"setex", key, std::to_string(expire), value};

    auto &table = JimKVContext::Instance();
    return table.CommandExec(cluster_id_, db_id_, table_id_, args, timeout);
}

KVReplyPtr KvStringTest::psetex(std::string key, std::string value, uint64_t expire, int timeout) {
    ArrayString args = {"psetex", key, std::to_string(expire), value};

    auto &table = JimKVContext::Instance();
    return table.CommandExec(cluster_id_, db_id_, table_id_, args, timeout);
}

KVReplyPtr KvStringTest::mget(int timeout) {
    ArrayString args = {"mget"};
    std::string key = "jimkv_kv_mk_";
    for (int i=0; i<10; i++) {
        auto k = key + std::to_string(i);
        args.push_back(k);
    }

    auto &table = JimKVContext::Instance();
    return table.CommandExec(cluster_id_, db_id_, table_id_, args, timeout);
}

KVReplyPtr KvStringTest::mset(int timeout) {
    ArrayString args = {"mset"};

    std::string key = "jimkv_kv_mk_";
    std::string value = "jimkv_kv_mv_";
    for (int i=0; i<10; i++) {
        auto k = key + std::to_string(i);
        auto v = value + std::to_string(i);
        args.push_back(k);
        args.push_back(v);
    }

    auto &table = JimKVContext::Instance();
    return table.CommandExec(cluster_id_, db_id_, table_id_, args, timeout);
}

KVReplyPtr KvStringTest::strlen(std::string key, int timeout) {
    ArrayString args = {"strlen", key};

    auto &table = JimKVContext::Instance();
    return table.CommandExec(cluster_id_, db_id_, table_id_, args, timeout);
}

KVReplyPtr KvStringTest::exists(std::string key, int timeout) {
    ArrayString args = {"exists", key};

    auto &table = JimKVContext::Instance();
    return table.CommandExec(cluster_id_, db_id_, table_id_, args, timeout);
}

KVReplyPtr KvStringTest::ttl(std::string key, int timeout) {
    ArrayString args = {"ttl", key, };

    auto &table = JimKVContext::Instance();
    return table.CommandExec(cluster_id_, db_id_, table_id_, args, timeout);
}

KVReplyPtr KvStringTest::pttl(std::string key, int timeout) {
    ArrayString args = {"pttl", key, };

    auto &table = JimKVContext::Instance();
    return table.CommandExec(cluster_id_, db_id_, table_id_, args, timeout);
}


void KvStringTest::async_get(std::string key, int timeout, AsyncFn fn, VoidPtr data) {
    ArrayString args = {"get", key};

    auto &table = JimKVContext::Instance();
    table.CommandExec(cluster_id_, db_id_, table_id_, args, timeout, fn, data);
}

void KvStringTest::async_set(std::string key, std::string value, int timeout, AsyncFn fn, VoidPtr data) {
    ArrayString args = {"set", key, value};

    auto &table = JimKVContext::Instance();
    table.CommandExec(cluster_id_, db_id_, table_id_, args, timeout, fn, data);
}

void KvStringTest::async_mget(int timeout, AsyncFn fn, VoidPtr data) {
    ArrayString args = {"mget"};
    std::string key = "jimkv_kv_mk_";
    for (int i=0; i<10; i++) {
        auto k = key + std::to_string(i);
        args.push_back(k);
    }

    auto &table = JimKVContext::Instance();
    table.CommandExec(cluster_id_, db_id_, table_id_, args, timeout, fn, data);
}


void KvStringTest::async_mset(int timeout, AsyncFn fn, VoidPtr data) {
    ArrayString args = {"mset"};

    std::string key = "jimkv_kv_mk_";
    std::string value = "jimkv_kv_mv_";
    for (int i=0; i<10; i++) {
        auto k = key + std::to_string(i);
        auto v = value + std::to_string(i);
        args.push_back(k);
        args.push_back(v);
    }

    auto &table = JimKVContext::Instance();
    table.CommandExec(cluster_id_, db_id_, table_id_, args, timeout, fn, data);
}


