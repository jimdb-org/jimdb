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

#include "jimkv_kv_context.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "jimkv_log.h"
#include "kv_string.h"
#include "kv_hash.h"
#include "kv_reply.h"

using namespace jimkv;
using namespace jimkv::sdk;
using namespace jimkv::sdk::kv;

void kv_string_test(KvStringTest &test) {
    CBLOG_INFO("test SET");
    test.set(std::string("jimkv_kv_string"), std::string("jimkv_kv_value"));

    CBLOG_INFO("test GET");
    auto v = test.get(std::string("jimkv_kv_string"));
    print_reply(v);

    CBLOG_INFO("test MSET");
    test.mset();

    CBLOG_INFO("test MGET");
    auto v1 = test.mget();
    print_reply(v1);

    CBLOG_INFO("test STRLEN");
    auto v2 = test.strlen(std::string("jimkv_kv_string"));
    print_reply(v2);

    CBLOG_INFO("test EXISTS");
    auto v3 = test.exists(std::string("jimkv_kv_string"));
    print_reply(v3);

    CBLOG_INFO("test SET EX");
    test.set(std::string("jimkv_kv_string"), std::string("jimkv_kv_value"), 1, 1000);

    CBLOG_INFO("test TTL");
    auto t1 = test.ttl(std::string("jimkv_kv_string"));
    print_reply(t1);

    CBLOG_INFO("test PTTL");
    auto t2 = test.pttl(std::string("jimkv_kv_string"));
    print_reply(t2);

    CBLOG_INFO("test SETEX");
    test.setex(std::string("jimkv_kv_string"), std::string("jimkv_kv_value"), 3000);

    CBLOG_INFO("test TTL");
    auto t3 = test.ttl(std::string("jimkv_kv_string"));
    print_reply(t3);

    CBLOG_INFO("test PSETEX");
    test.psetex(std::string("jimkv_kv_string"), std::string("jimkv_kv_value"), 60000000);

    CBLOG_INFO("test PTTL");
    auto t4 = test.ttl(std::string("jimkv_kv_string"));
    print_reply(t4);

    CBLOG_INFO("test MSET");
    test.mset();

    CBLOG_INFO("test MGET");
    auto mv1 = test.mget();
    print_reply(mv1);

    CBLOG_INFO("test DEL MULTI KEY");
    ArrayString keys = {"jimkv_kv_mk_1", "jimkv_kv_mk_3","jimkv_kv_mk_5","jimkv_kv_mk_9"};
    auto dv1 = test.del(keys);
    print_reply(dv1);

    CBLOG_INFO("test MGET");
    auto mv2 = test.mget();
    print_reply(mv2);
}

void kv_hash_test(KvHashTest &test) {
    CBLOG_INFO("test HSET");
    test.hset();

    CBLOG_INFO("test HGET");
    auto v = test.hget();
    print_reply(v);

    CBLOG_INFO("test HMSET");
    test.hmset();

    CBLOG_INFO("test HMGET");
    auto v1 = test.hmget();
    print_reply(v1);

    CBLOG_INFO("test HGETALL");
    auto v2 = test.hgetall();
    print_reply(v2);

    CBLOG_INFO("test HDEL");
    test.hdel();

    CBLOG_INFO("test HGETALL");
    auto v3 = test.hgetall();
    print_reply(v3);

    CBLOG_INFO("test EXISTS");
    auto v5 = test.exists(std::string("jimkv_kv_hash"));
    print_reply(v5);

    CBLOG_INFO("test DEL");
    test.del();

    CBLOG_INFO("test HMGET");
    auto v4 = test.hmget();
    print_reply(v4);
}


int main(int argc, char **argv) {
    if (argc < 5) {
        fprintf(stderr, "Usage: %s cluster_id db_id table_id master_host [thread_num] [conn_num]\n", argv[0]);
        return 0;
    }
    char *endptr;

    int logfd = open("./test.log", O_CREAT | O_RDWR | O_TRUNC, 0644);
    dup2(logfd, STDERR_FILENO);
    dup2(logfd, STDOUT_FILENO);
    jimkv_log_level_set(JIMKV_LOG_DEBUG);

    uint64_t cluster_id = strtoull(argv[1], &endptr, 10);
    uint64_t db_id = strtoull(argv[2], &endptr, 10);
    uint64_t table_id = strtoull(argv[3], &endptr, 10);

    int thread_num = 4;
    uint32_t conn_num = 4;

    if (argc == 6) {
        thread_num = atoi(argv[5]);
    }

    if (argc == 7) {
        conn_num = atoi(argv[6]);
    }

    auto &context = JimKVContext::Instance();
    context.Start(thread_num);
    JimKVOption option = {argv[4], cluster_id, db_id, table_id, conn_num, 0, 0, 0};

    context.AddTable(option);

    KvStringTest kv_string(cluster_id, db_id, table_id);
    kv_string_test(kv_string);

    KvHashTest kv_hash(cluster_id, db_id, table_id);
    kv_hash_test(kv_hash);

    sleep(3);
    context.Stop();

    close(logfd);
    return 0;
}
