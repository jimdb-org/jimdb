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

#include <functional>
#include <string>

#include "jim_common.h"

namespace jim {
namespace sdk {
namespace kv {

enum JimKVReplyType {
    REPLY_STRING = 1,
    REPLY_ARRAY,
    REPLY_INTEGER,
    REPLY_NIL,
    REPLY_STATUS,
    REPLY_ERROR,
    REPLY_MAX,
};

enum RedisCmdType {
    DEL = 0,
    DUMP,
    EXISTS,
    EXPIRE,
    EXPIREAT,
    KEYS,
    MIGRATE,
    MOVE,
    OBJECT,
    PERSIST,
    PEXPIRE,
    PEXPIREAT,
    PTTL,
    RANDOMKEY,
    RENAME,
    RENAMENX,
    RESTORE,
    SORT,
    TTL,
    TYPE,
    SCAN,
    APPEND,
    BITCOUNT,
    BITOP,
    BITPOS,
    DECR,
    DECRBY,
    GET,
    GETBIT,
    GETRANGE,
    SUBSTR,
    GETSET,
    INCR,
    INCRBY,
    INCRBYFLOAT,
    MGET,
    MSET,
    MSETNX,
    PSETEX,
    SET,
    SETBIT,
    SETEX,
    SETNX,
    SETRANGE,
    STRLEN,
    HDEL,
    HEXISTS,
    HGET,
    HGETALL,
    HINCRBY,
    HINCRBYFLOAT,
    HKEYS,
    HLEN,
    HMGET,
    HMSET,
    HSET,
    HSETNX,
    HVALS,
    HSCAN,
    BLPOP,
    BRPOP,
    BRPOPLPUSH,
    LINDEX,
    LINSERT,
    LLEN,
    LPOP,
    LPUSH,
    LPUSHX,
    LRANGE,
    LREM,
    LSET,
    LTRIM,
    RPOP,
    RPOPLPUSH,
    RPUSH,
    RPUSHX,
    SADD,
    SCARD,
    SDIFF,
    SDIFFSTORE,
    SINTER,
    SINTERSTORE,
    SISMEMBER,
    SMEMBERS,
    SMOVE,
    SPOP,
    SRANDMEMBER,
    SREM,
    SUNION,
    SUNIONSTORE,
    SSCAN,
    ZADD,
    ZCARD,
    ZCOUNT,
    ZINCRBY,
    ZRANGE,
    ZRANGEBYSCORE,
    ZRANK,
    ZREM,
    ZREMRANGEBYRANK,
    ZREMRANGEBYSCORE,
    ZREVRANGE,
    ZREVRANGEBYSCORE,
    ZREVRANK,
    ZSCORE,
    ZUNIONSTORE,
    ZINTERSTORE,
    ZSCAN,
    ZRANGEBYLEX,
    ZLEXCOUNT,
    ZREMRANGEBYLEX,
    ZREVRANGEBYLEX,
    PFADD,
    PFCOUNT,
    PFMERGE,
    PFSELFTEST,
    PFDEBUG,
    PSUBSCRIBE,
    PUBLISH,
    PUBSUB,
    PUNSUBSCRIBE,
    SUBSCRIBE,
    UNSUBSCRIBE,
    DISCARD,
    EXEC,
    MULTI,
    UNWATCH,
    WATCH,
    EVAL,
    EVALSHA,
    SCRIPT,
    AUTH,
    //ECHO,
    PING,
    SELECT,
    BGREWRITEAOF,
    BGSAVE,
    CLIENT,
    CONFIG,
    DBSIZE,
    DEBUG,
    FLUSHDB,
    FLUSHALL,
    INFO,
    LASTSAVE,
    MONITOR,
    PSYNC,
    REPLCONF,
    ROLE,
    SAVE,
    SHUTDOWN,
    SLAVEOF,
    SLOWLOG,
    SYNC,
    TIME,
    ERROR,
};

enum JimKVRelation {
    PARENT,
    CHILD,
};

struct JimKVOption {
    std::string master_address;
    uint64_t cluster_id;
    uint64_t db_id;
    uint64_t table_id;
    uint32_t conn_count;
    int cache_api_init;
    int cache_expire;
    uint64_t cache_max_memory;
};

} //namespace kv
} //namespace sdk
} //namespace jim
