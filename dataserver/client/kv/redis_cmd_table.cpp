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

#include "redis_cmd_table.h"

#include "status.h"

#include "jim_log.h"
#include "jim_kv_serialize.h"

#include "base/util.h"
#include "dspb/api.pb.h"

namespace jim {
namespace sdk {
namespace kv {

using namespace std::placeholders;

static JimStatus del_split(const ArrayString& args, std::vector<ArrayString>& split_args) {
    int i = 0;
    if (args.size() == 2) {
        return JimStatus::ONE_KEY;
    }
    const std::string& cmd_name = args[0];
    for (auto& arg : args) {
        if (i++ == 0) {
            continue;
        }
        ArrayString res_arg;
        res_arg.push_back(cmd_name);
        res_arg.push_back(arg);
        split_args.push_back(res_arg);
    }
    return JimStatus::OK;
}

static JimStatus mget_split(const ArrayString& args, std::vector<ArrayString>& split_args) {
    int i = 0;
    int count = 0;
    count = args.size() - 1;
    if (count == 1) {
        return JimStatus::ONE_KEY;
    }
    RedisCmdTable& cmd_table = RedisCmdTable::Instance();
    RedisCmdEntry* cmd_entry = cmd_table.kv_cmd_get(args[0], args.size());
    if (cmd_entry == nullptr) {
        CBLOG_ERROR("not find command: %.*s", static_cast<int>(args[0].size()), args[0].c_str());
        return JimStatus::ERROR;
    }
    for (auto& arg : args) {
        if (i++ == 0) {
            continue;
        }
        ArrayString res_args;
        res_args.push_back(cmd_entry->name);
        res_args.push_back(arg);
        split_args.push_back(res_args);
    }
    return JimStatus::OK;
}

static JimStatus mset_split(const ArrayString& args, std::vector<ArrayString>& split_args) {
    int i = 0;
    int count = 0;

    count = (args.size() - 1) / 2;
    if (count == 1) {
        return JimStatus::ONE_KEY;
    }

    RedisCmdTable& cmd_table = RedisCmdTable::Instance();
    RedisCmdEntry* cmd_entry = cmd_table.kv_cmd_get(args[0], args.size());
    if (cmd_entry == nullptr || (args.size() % 2) == 0) {
        CBLOG_ERROR("not find command: %.*s or parameter error", static_cast<int>(args[0].size()), args[0].c_str());
        return JimStatus::ERROR;
    }
    for (i = 0; i < count; i++) {
        ArrayString res_args;
        res_args.push_back(cmd_entry->name);
        res_args.push_back(args[i*2+1]);
        res_args.push_back(args[i*2+2]);
        split_args.push_back(res_args);
    }
    return JimStatus::OK;
}

static JimStatus hmget_split(const ArrayString& args, std::vector<ArrayString>& split_args) {
    int i = 0;
    int count = 0;

    count = args.size() - 2;
    if (count == 1) {
        return JimStatus::ONE_KEY;
    }

    RedisCmdTable& cmd_table = RedisCmdTable::Instance();
    RedisCmdEntry* cmd_entry = cmd_table.kv_cmd_get(args[0], static_cast<int>(args.size()));
    if (cmd_entry == nullptr) {
        CBLOG_ERROR("not find command: %.*s", static_cast<int>(args[0].size()), args[0].c_str());
        return JimStatus::ERROR;
    }
    for (i = 0; i < count; i++) {
        ArrayString res_args;
        res_args.push_back(args[0]); //hmget
        res_args.push_back(args[1]); //hash key
        res_args.push_back(args[i + 2]);//hash field

        split_args.push_back(res_args);
    }
    return JimStatus::OK;
}

static JimStatus hmset_split(const ArrayString& args, std::vector<ArrayString>& split_args) {
    int i = 0;
    int count = 0;

    count = (args.size() - 2) / 2;
    if (count == 1) {
        return JimStatus::ONE_KEY;
    }

    RedisCmdTable& cmd_table = RedisCmdTable::Instance();

    RedisCmdEntry* cmd_entry = cmd_table.kv_cmd_get(args[0], static_cast<int>(args.size()));

    for (i = 0; i < count; i++) {
        ArrayString res_args;
        res_args.push_back(args[0]); //hmset
        res_args.push_back(args[1]); //hash key
        res_args.push_back(args[i*2 + 2]); //hash field
        res_args.push_back(args[i*2 + 3]); //field value

        split_args.push_back(res_args);
    }
    return JimStatus::OK;
}

static JimStatus hdel_split(const ArrayString& args, std::vector<ArrayString>& split_args) {
    int i = 0;
    int count = 0;

    count = args.size() - 2;
    if (count == 1) {
        return JimStatus::ONE_KEY;
    }

    RedisCmdTable& cmd_table = RedisCmdTable::Instance();

    RedisCmdEntry* cmd_entry = cmd_table.kv_cmd_get(args[0], args.size());

    for (i = 0; i < count; i++) {
        ArrayString res_args;
        res_args.push_back(args[0]);
        res_args.push_back(args[1]);
        res_args.push_back(args[i + 2]);

        split_args.push_back(res_args);
    }
    return JimStatus::OK;
}

static void del_build_reply(ArrayRespPtr &responses, KVReplyPtr &reply) {
    if (responses.empty()) {
        reply->set_type(JimKVReplyType::REPLY_ERROR);
        return reply->set_buf(JIMKV_ETIMEOUT);
    }

    for (auto response : responses) {
        if (response->header().has_error()) {
            std::string err_msg = response->header().error().ShortDebugString();
            CBLOG_ERROR("received error: %s", err_msg.c_str());

            reply->set_type(JimKVReplyType::REPLY_ERROR);
            return reply->set_buf(err_msg);
        }

        if (response->resp_case() == dspb::RangeResponse::kKvDelRange) {
            auto& kv_delrange_resp = response->kv_del_range();

            if (kv_delrange_resp.code() != Status::kOk) {
                Status s(static_cast<Status::Code>(kv_delrange_resp.code()));
                std::string err_msg = s.ToString();
                CBLOG_ERROR("del range received error: %s", err_msg.c_str());

                reply->set_type(JimKVReplyType::REPLY_ERROR);
                return reply->set_buf(err_msg);
            }
        } else {
            CBLOG_ERROR("invalid response type: %d for del", response->resp_case());
        }
    }

    reply->set_type(JimKVReplyType::REPLY_INTEGER);
    reply->set_integer(1);
}

static void strlen_build_reply(ArrayRespPtr &responses, KVReplyPtr &reply) {
    if (responses.size() != 1) {
        CBLOG_ERROR("responses is empty or more than 1");
        reply->set_type(JimKVReplyType::REPLY_ERROR);
        return reply->set_buf(JIMKV_ETIMEOUT);
    }

    auto response = responses[0];

    if (response->header().has_error()) {
        CBLOG_ERROR("received error: %d", response->header().error().err_case());
        reply->set_type(JimKVReplyType::REPLY_ERROR);
        return reply->set_buf(response->header().error().DebugString());
    }

    if (response->resp_case() == dspb::RangeResponse::kKvGet) {
        auto& kv_get_resp = response->kv_get();

        if (kv_get_resp.code() != Status::kOk) {
            if (kv_get_resp.code() == Status::kNotFound) {
                reply->set_type(JimKVReplyType::REPLY_INTEGER);
                reply->set_integer(0);
            } else {
                Status s(static_cast<Status::Code>(kv_get_resp.code()));
                std::string err_msg = s.ToString();
                CBLOG_ERROR("kv strlen received error: %s", err_msg.c_str());

                reply->set_type(JimKVReplyType::REPLY_ERROR);
                reply->set_buf(err_msg);
            }
        } else {
            std::string val;
            uint64_t expire;
            reply->set_type(JimKVReplyType::REPLY_INTEGER);
            if (value_decode(kv_get_resp.value(), val, expire) == JimStatus::EXPIRED) {
                reply->set_integer(0);
            } else {
                reply->set_integer(val.size());
            }
        }
    } else {
        CBLOG_ERROR("invalid response type: %d for mget", response->resp_case());
        reply->set_type(JimKVReplyType::REPLY_ERROR);
        reply->set_buf(std::string("ERR: Invalid response"));
    }
}

static void exists_build_reply(ArrayRespPtr &responses, KVReplyPtr &reply) {
    if (responses.empty()) {
        CBLOG_ERROR("responses is empty");
        reply->set_type(JimKVReplyType::REPLY_ERROR);
        return reply->set_buf(JIMKV_ETIMEOUT);
    }

    reply->set_type(JimKVReplyType::REPLY_ARRAY);
    for (auto response : responses) {
        if (response->header().has_error()) {
            std::string err_msg = response->header().error().ShortDebugString();
            CBLOG_ERROR("received error: %s", err_msg.c_str());

            reply = NewJimKVReply();
            reply->set_type(JimKVReplyType::REPLY_ERROR);
            return reply->set_buf(err_msg);
        }

        if (response->resp_case() == dspb::RangeResponse::kKvScan) {
            auto& kv_scan_resp = response->kv_scan();

            if (kv_scan_resp.code() != Status::kOk) {
                Status s(static_cast<Status::Code>(kv_scan_resp.code()));
                std::string err_msg = s.ToString();
                CBLOG_ERROR("scan range received error: %s", err_msg.c_str());

                reply = NewJimKVReply();
                reply->set_type(JimKVReplyType::REPLY_ERROR);
                return reply->set_buf(err_msg);
            }

            reply->set_type(JimKVReplyType::REPLY_INTEGER);
            reply->set_integer(kv_scan_resp.values_size() > 0 ? 1 : 0);
        } else {
            CBLOG_ERROR("invalid response type: %d for process", response->resp_case());

            reply = NewJimKVReply();
            reply->set_type(JimKVReplyType::REPLY_ERROR);
            return reply->set_buf(std::string("ERR: Invalid response"));
        }
    }
}

//static void expire_build_reply(ArrayRespPtr &responses, KVReplyPtr &reply) {
//
//}
//
//static void expireat_build_reply(ArrayRespPtr &responses, KVReplyPtr &reply) {
//
//}
//
//static void persist_build_reply(ArrayRespPtr &responses, KVReplyPtr &reply) {
//
//}
//
//static void pexpire_build_reply(ArrayRespPtr &responses, KVReplyPtr &reply) {
//
//}
//
//static void pexpireat_build_reply(ArrayRespPtr &responses, KVReplyPtr &reply) {
//
//}

static void pttl_build_reply(ArrayRespPtr &responses, KVReplyPtr &reply) {
    if (responses.size() != 1) {
        CBLOG_ERROR("responses is empty or more than 1, real: %zu", responses.size());
        reply->set_type(JimKVReplyType::REPLY_ERROR);
        return reply->set_buf(JIMKV_ETIMEOUT);
    }

    auto response = responses[0];
    if (response->header().has_error()) {
        std::string err_msg = response->header().error().ShortDebugString();
        CBLOG_ERROR("received error: %s", err_msg.c_str());
        reply->set_type(JimKVReplyType::REPLY_ERROR);
        return reply->set_buf(err_msg);
    }

    if (response->resp_case() == dspb::RangeResponse::kKvGet) {
        auto& kv_get_resp = response->kv_get();

        if (kv_get_resp.code() != Status::kOk) {
            if (kv_get_resp.code() == Status::kNotFound) {
                reply->set_type(JimKVReplyType::REPLY_INTEGER);
                reply->set_integer(-2);
            } else {
                Status s(static_cast<Status::Code>(kv_get_resp.code()));
                std::string err_msg = s.ToString();
                CBLOG_ERROR("received error: %s", err_msg.c_str());

                reply->set_type(JimKVReplyType::REPLY_ERROR);
                reply->set_buf(err_msg);
            }
        } else {
            std::string val;
            uint64_t expire;
            auto status = value_decode(kv_get_resp.value(), val, expire);
            if (status == JimStatus::EXPIRED) {
                reply->set_type(JimKVReplyType::REPLY_INTEGER);
                reply->set_integer(-2);
            } else if (status == JimStatus::ERROR){
                CBLOG_ERROR("received error: invalid value");
                reply->set_type(JimKVReplyType::REPLY_ERROR);
                reply->set_buf("ERR: Data error");
            } else {
                reply->set_type(JimKVReplyType::REPLY_INTEGER);
                if (expire == 0) {
                    reply->set_integer(-1);
                } else {
                    reply->set_integer(expire - NowMilliSeconds());
                }
            }
        }
    } else {
        CBLOG_ERROR("invalid response type: %d for get", response->resp_case());
        reply->set_type(JimKVReplyType::REPLY_ERROR);
        reply->set_buf(std::string("ERR: Invalid response"));
    }
}

//static void rename_build_reply(ArrayRespPtr &responses, KVReplyPtr &reply) {
//
//}
//
//static void renmaenx_build_reply(ArrayRespPtr &responses, KVReplyPtr &reply) {
//
//}
//
static void ttl_build_reply(ArrayRespPtr &responses, KVReplyPtr &reply) {
    if (responses.size() != 1) {
        CBLOG_ERROR("responses is empty or more than 1, real: %zu", responses.size());
        reply->set_type(JimKVReplyType::REPLY_ERROR);
        return reply->set_buf(JIMKV_ETIMEOUT);
    }

    auto response = responses[0];
    if (response->header().has_error()) {
        std::string err_msg = response->header().error().ShortDebugString();
        CBLOG_ERROR("received error: %s", err_msg.c_str());
        reply->set_type(JimKVReplyType::REPLY_ERROR);
        return reply->set_buf(err_msg);
    }

    if (response->resp_case() == dspb::RangeResponse::kKvGet) {
        auto& kv_get_resp = response->kv_get();

        if (kv_get_resp.code() != Status::kOk) {
            if (kv_get_resp.code() == Status::kNotFound) {
                reply->set_type(JimKVReplyType::REPLY_INTEGER);
                reply->set_integer(-2);
            } else {
                Status s(static_cast<Status::Code>(kv_get_resp.code()));
                std::string err_msg = s.ToString();
                CBLOG_ERROR("received error: %s", err_msg.c_str());

                reply->set_type(JimKVReplyType::REPLY_ERROR);
                reply->set_buf(err_msg);
            }
        } else {
            std::string val;
            uint64_t expire;
            auto status = value_decode(kv_get_resp.value(), val, expire);
            if (status == JimStatus::EXPIRED) {
                reply->set_type(JimKVReplyType::REPLY_INTEGER);
                reply->set_integer(-2);
            } else if (status == JimStatus::ERROR){
                CBLOG_ERROR("received error: invalid value");
                reply->set_type(JimKVReplyType::REPLY_ERROR);
                reply->set_buf("ERR: Data error");
            } else {
                reply->set_type(JimKVReplyType::REPLY_INTEGER);
                if (expire == 0) {
                    reply->set_integer(-1);
                } else {
                    reply->set_integer(expire / 1000 - NowSeconds());
                }
            }
        }
    } else {
        CBLOG_ERROR("invalid response type: %d for get", response->resp_case());
        reply->set_type(JimKVReplyType::REPLY_ERROR);
        reply->set_buf(std::string("ERR: Invalid response"));
    }
}
//
//static void type_build_reply(ArrayRespPtr &responses, KVReplyPtr &reply) {
//
//}
//
//static void append_build_reply(ArrayRespPtr &responses, KVReplyPtr &reply) {
//
//}
//
//static void bitcount_build_reply(ArrayRespPtr &responses, KVReplyPtr &reply) {
//
//}
//
//static void bitop_build_reply(ArrayRespPtr &responses, KVReplyPtr &reply) {
//
//}
//
//static void bitops_build_reply(ArrayRespPtr &responses, KVReplyPtr &reply) {
//
//}
//
//static void decr_build_reply(ArrayRespPtr& responses, KVReplyPtr* reply) {
//
//}
//
//static void decrby_build_reply(ArrayRespPtr& responses, KVReplyPtr* reply) {
//
//}
//
//static void incr_build_reply(ArrayRespPtr &responses, KVReplyPtr &reply) {
//
//}

static void get_build_reply(ArrayRespPtr &responses, KVReplyPtr &reply) {
    if (responses.size() != 1) {
        CBLOG_ERROR("responses is empty or more than 1, real: %zu", responses.size());
        reply->set_type(JimKVReplyType::REPLY_ERROR);
        return reply->set_buf(JIMKV_ETIMEOUT);
    }

    auto response = responses[0];
    if (response->header().has_error()) {
        std::string err_msg = response->header().error().ShortDebugString();
        CBLOG_ERROR("received error: %s", err_msg.c_str());
        reply->set_type(JimKVReplyType::REPLY_ERROR);
        return reply->set_buf(err_msg);
    }

    if (response->resp_case() == dspb::RangeResponse::kKvGet) {
        auto& kv_get_resp = response->kv_get();

        if (kv_get_resp.code() != Status::kOk) {
            if (kv_get_resp.code() == Status::kNotFound) {
                reply->set_type(JimKVReplyType::REPLY_NIL);
            } else {
                Status s(static_cast<Status::Code>(kv_get_resp.code()));
                std::string err_msg = s.ToString();
                CBLOG_ERROR("received error: %s", err_msg.c_str());

                reply->set_type(JimKVReplyType::REPLY_ERROR);
                reply->set_buf(err_msg);
            }
        } else {
            std::string val;
            uint64_t expire;
            auto status = value_decode(kv_get_resp.value(), val, expire);
            if (status == JimStatus::EXPIRED) {
                reply->set_type(JimKVReplyType::REPLY_NIL);
            } else if (status == JimStatus::ERROR){
                CBLOG_ERROR("received error: invalid value");
                reply->set_type(JimKVReplyType::REPLY_ERROR);
                reply->set_buf("ERR: Data error");
            } else {
                reply->set_type(JimKVReplyType::REPLY_STRING);
                reply->set_buf(val);
            }
        }
    } else {
        CBLOG_ERROR("invalid response type: %d for get", response->resp_case());
        reply->set_type(JimKVReplyType::REPLY_ERROR);
        reply->set_buf(std::string("ERR: Invalid response"));
    }
}

//static void getbit_build_reply(ArrayRespPtr &responses, KVReplyPtr &reply) {
//
//}
//
//static void getrange_build_reply(ArrayRespPtr &responses, KVReplyPtr &reply) {
//
//}
//
//static void substr_build_reply(ArrayRespPtr &responses, KVReplyPtr &reply) {
//
//}
//
//static void getset_build_reply(ArrayRespPtr &responses, KVReplyPtr &reply) {
//
//}

static void mget_build_reply(ArrayRespPtr &responses, KVReplyPtr &reply) {
    get_build_reply(responses, reply);
}

static void set_build_reply(ArrayRespPtr &responses, KVReplyPtr &reply) {
    if (responses.size() != 1) {
        CBLOG_ERROR("responses is empty or more than 1, real: %zu", responses.size());
        reply->set_type(JimKVReplyType::REPLY_ERROR);
        return reply->set_buf(JIMKV_ETIMEOUT);
    }

    auto response = responses[0];
    if (response->header().has_error()) {
        std::string err_msg = response->header().error().ShortDebugString();
        CBLOG_ERROR("received error: %s", err_msg.c_str());

        reply->set_type(JimKVReplyType::REPLY_ERROR);
        return reply->set_buf(err_msg);
    }

    if (response->resp_case() == dspb::RangeResponse::kKvPut) {
        auto& kv_put_resp = response->kv_put();

        if (kv_put_resp.code() != Status::kOk) {
            Status s(static_cast<Status::Code>(kv_put_resp.code()));
            std::string err_msg = s.ToString();
            CBLOG_ERROR("received error: %s", err_msg.c_str());

            reply->set_type(JimKVReplyType::REPLY_ERROR);
            reply->set_buf(err_msg);
        } else {
            reply->set_type(JimKVReplyType::REPLY_STATUS);
            reply->set_buf(std::string("OK"));
        }
    } else {
        CBLOG_ERROR("invalid response type: %d for get", response->resp_case());
        reply->set_type(JimKVReplyType::REPLY_ERROR);
        reply->set_buf(std::string("ERR: Invalid response"));
    }
}

static void setex_build_reply(ArrayRespPtr &responses, KVReplyPtr &reply) {
    set_build_reply(responses, reply);
}

static void psetex_build_reply(ArrayRespPtr &responses, KVReplyPtr &reply) {
    set_build_reply(responses, reply);
}

//
//static void setnx_build_reply(ArrayRespPtr &responses, KVReplyPtr &reply) {
//
//}

static void mset_build_reply(ArrayRespPtr &responses, KVReplyPtr &reply) {
    set_build_reply(responses, reply);
}

static void hget_build_reply(ArrayRespPtr &responses, KVReplyPtr &reply) {
    get_build_reply(responses, reply);
}

static void hexists_build_reply(ArrayRespPtr &responses, KVReplyPtr &reply) {
    get_build_reply(responses, reply);
    if (reply->type() == JimKVReplyType::REPLY_NIL) {
        reply->set_type(JimKVReplyType::REPLY_INTEGER);
        reply->set_integer(0);
    } else if (reply->type() != JimKVReplyType::REPLY_ERROR) {
        reply->set_type(JimKVReplyType::REPLY_INTEGER);
        reply->set_integer(1);
    }
}

static void hset_build_reply(ArrayRespPtr &responses, KVReplyPtr &reply) {
    set_build_reply(responses, reply);
    if (reply->type() != JimKVReplyType::REPLY_ERROR) {
        reply->set_type(JimKVReplyType::REPLY_INTEGER);
        reply->set_integer(1);
    }
}

static void hdel_build_reply(ArrayRespPtr &responses, KVReplyPtr &reply) {
    if (responses.empty()) {
        reply->set_type(JimKVReplyType::REPLY_ERROR);
        return reply->set_buf(JIMKV_ETIMEOUT);
    }

    for (auto response : responses) {
        if (response->header().has_error()) {
            std::string err_msg = response->header().error().ShortDebugString();
            CBLOG_ERROR("received error: %s", err_msg.c_str());

            reply->set_type(JimKVReplyType::REPLY_ERROR);
            return reply->set_buf(err_msg);
        }

        if (response->resp_case() == dspb::RangeResponse::kKvDelete) {
            auto& kv_delete_resp = response->kv_delete();

            if (kv_delete_resp.code() != Status::kOk) {
                Status s(static_cast<Status::Code>(kv_delete_resp.code()));
                std::string err_msg = s.ToString();
                CBLOG_ERROR("del range received error: %s", err_msg.c_str());

                reply->set_type(JimKVReplyType::REPLY_ERROR);
                return reply->set_buf(err_msg);
            }
        } else {
            CBLOG_ERROR("invalid response type: %d for del", response->resp_case());
        }
    }

    reply->set_type(JimKVReplyType::REPLY_INTEGER);
    reply->set_integer(1);
}

static void hmget_build_reply(ArrayRespPtr &responses, KVReplyPtr &reply) {
    mget_build_reply(responses, reply);
}

static void hmset_build_reply(ArrayRespPtr &responses, KVReplyPtr &reply) {
    mset_build_reply(responses, reply);
}

static void hgetall_build_reply(ArrayRespPtr &responses, KVReplyPtr &reply) {
    if (responses.empty()) {
        CBLOG_ERROR("responses is empty");
        reply->set_type(JimKVReplyType::REPLY_ERROR);
        return reply->set_buf(JIMKV_ETIMEOUT);
    }

    reply->set_type(JimKVReplyType::REPLY_ARRAY);
    for (auto response : responses) {
        if (response->header().has_error()) {
            std::string err_msg = response->header().error().ShortDebugString();
            CBLOG_ERROR("received error: %s", err_msg.c_str());

            reply = NewJimKVReply();
            reply->set_type(JimKVReplyType::REPLY_ERROR);
            return reply->set_buf(err_msg);
        }

        if (response->resp_case() == dspb::RangeResponse::kKvScan) {
            auto& kv_scan_resp = response->kv_scan();
            if (kv_scan_resp.code() != Status::kOk) {
                if (kv_scan_resp.code() == Status::kNotFound) {
                    continue;
                } else {
                    Status s(static_cast<Status::Code>(kv_scan_resp.code()));
                    std::string err_msg = s.ToString();
                    CBLOG_ERROR("scan range received error: %s", err_msg.c_str());
                    reply->set_type(JimKVReplyType::REPLY_ERROR);
                    reply->set_buf(err_msg);
                }
                return;
            }

            auto values = kv_scan_resp.values();
            for (auto &value: values) {
                auto child_reply_key = NewJimKVReply();
                auto child_reply_val = NewJimKVReply();

                std::string key;
                std::string val;
                uint64_t expire;
                if (key_decode(value.key(), key) != JimStatus::OK) {
                    reply->set_type(JimKVReplyType::REPLY_ERROR);
                    reply->set_buf("data error");
                    return;
                }

                if (value_decode(value.value(), val, expire) != JimStatus::OK) {
                    reply->set_type(JimKVReplyType::REPLY_ERROR);
                    reply->set_buf("ERR: Data error");
                    return;
                }

                child_reply_key->set_type(JimKVReplyType::REPLY_STRING);
                child_reply_key->set_buf(key);

                child_reply_val->set_type(JimKVReplyType::REPLY_STRING);
                child_reply_val->set_buf(val);

                reply->add_element(child_reply_key);
                reply->add_element(child_reply_val);
            }
        } else {
            CBLOG_ERROR("invalid response type: %d for process", response->resp_case());

            reply = NewJimKVReply();
            reply->set_type(JimKVReplyType::REPLY_ERROR);
            return reply->set_buf(std::string("ERR: Invalid response"));
        }
    }
}

RedisCmdEntry* RedisCmdTable::kv_cmd_get(const std::string& name, int argc) {
    RedisCmdEntry *entry;
    auto it = g_cmds_.find(name);
    if (it != g_cmds_.end()) {
        entry =  it->second;
    } else {
        return nullptr;
    }

    if ((entry->arity > 0 && entry->arity != argc) || (argc < -entry->arity)) {
        return nullptr;
    }
    return entry;
}

RedisCmdEntry* RedisCmdTable::multi_convert_single(const RedisCmdEntry* multi) {
    std::string convert_key;
    if (multi->name.compare("mget") == 0) {
        convert_key = "get";
    } else if (multi->name.compare("mset") == 0) {
        convert_key = "set";
    } else if (multi->name.compare("msetnx") == 0) {
        convert_key = "setnx";
    }else if (multi->name.compare("hmget") == 0) {
        convert_key = "hget";
    } else if (multi->name.compare("hmset") == 0) {
        convert_key = "hset";
    } else {
        return NULL;
    }
    auto it = g_cmds_.find(convert_key);
    if (it != g_cmds_.end()) {
        return it->second;
    }
    return nullptr;
}


CmdRWFlag kv_rw_flag(const RedisCmdEntry* cmd) {
    for (auto it = cmd->flags.cbegin(); it != cmd->flags.cend(); it++) {
        if (*it == 'w') {
            return CmdRWFlag::WRITE;
        } else if (*it == 'r') {
            return CmdRWFlag::READ;
        }
    }
    return CmdRWFlag::OTHER;
}

bool is_multi_key_cmd(const RedisCmdEntry* cmd) {
    switch (cmd->cmd_type) {
    case RedisCmdType::MGET:
        return true;
    case RedisCmdType::MSET:
        return true;
    case RedisCmdType::HMGET:
        return true;
    case RedisCmdType::HMSET:
        return true;
    default:
        return false;
    }
    return false;
}

#define bind_fn2(fn1, fn3)        \
    std::bind((fn1), _1, _2),     \
    nullptr,                      \
    std::bind((fn3), _1, _2)


#define bind_fn3(fn1, fn2, fn3)   \
    std::bind((fn1), _1, _2),     \
    std::bind((fn2), _1, _2),     \
    std::bind((fn3), _1, _2)

static RedisCmdEntry command_table[] = {
    {"del",      "wd",  -2, 1, -1, 1, RedisCmdType::DEL,       bind_fn3(del_encode, del_split, del_build_reply)},
    //{"dump",     "ar",   2, 1, 1, 1, RedisCmdType::DUMP,       bind_fn2()},
    {"exists",   "r",    2, 1, 1, 1, RedisCmdType::EXISTS,     bind_fn2(exists_encode, exists_build_reply)},
    //{"expire",   "w",    3, 1, 1, 1, RedisCmdType::EXPIRE,     bind_fn2()},
    //{"expireat", "w",    3, 1, 1, 1, RedisCmdType::EXPIREAT,   bind_fn2()},
    //{"keys",     "rS",   2, 0, 0, 0, RedisCmdType::KEYS,       bind_fn2()},
    //{"migrate",  "aw",   6, 0, 0, 0, RedisCmdType::MIGRATE,    bind_fn2()},
    //{"move",     "wd",   3, 1, 1, 1, RedisCmdType::MOVE,       bind_fn2()},
    //{"object",   "r",   -2, 2, 2, 2, RedisCmdType::OBJECT,     bind_fn2()},
    //{"persist",  "w",    2, 1, 1, 1, RedisCmdType::PERSIST,    bind_fn2()},
    //{"pexpire",  "w",    3, 1, 1, 1, RedisCmdType::PEXPIRE,    bind_fn2()},
    //{"pexpireat","w",    3, 1, 1, 1, RedisCmdType::PEXPIREAT,  bind_fn2()},
    {"pttl",     "r",    2, 1, 1, 1, RedisCmdType::PTTL,       bind_fn2(pttl_encode, pttl_build_reply)},
    //{"randomkey","rR",   1, 0, 0, 0, RedisCmdType::RANDOMKEY,  bind_fn2()},
    //{"rename",   "wd",   3, 1, 2, 1, RedisCmdType::RENAME,     bind_fn2()},
    //{"renamenx", "wd",   3, 1, 2, 1, RedisCmdType::RENAMENX,   bind_fn2()},
    //{"restore",  "awm",  4, 1, 1, 1, RedisCmdType::RESTORE,    bind_fn2()},
    //{"sort",     "wm",  -2, 1, 1, 1, RedisCmdType::SORT,       bind_fn2()},
    {"ttl",      "r",    2, 1, 1, 1, RedisCmdType::TTL,        bind_fn2(ttl_encode, ttl_build_reply)},
    //{"type",     "r",    2, 1, 1, 1, RedisCmdType::TYPE,       bind_fn2()},
    //{"scan",     "rR",  -2, 0, 0, 0, RedisCmdType::SCAN,       bind_fn2()},

    //string
    //{"append",      "wmd",  3, 1,  1, 1, RedisCmdType::APPEND,      bind_fn2()},
    //{"bitcount",    "r",   -2, 1,  1, 1, RedisCmdType::BITCOUNT,    bind_fn2()},
    //{"bitop",       "wm",  -4, 2, -1, 1, RedisCmdType::BITOP,       bind_fn2()},
    //{"bitpos",      "r",   -3, 1,  1, 1, RedisCmdType::BITPOS,      bind_fn2()},
    //{"decr",        "wmd",  2, 1,  1, 1, RedisCmdType::DECR,        bind_fn2()},
    //{"decrby",      "wmd",  3, 1,  1, 1, RedisCmdType::DECRBY,      bind_fn2()},
    {"get",         "r",    2, 1,  1, 1, RedisCmdType::GET,         bind_fn2(get_encode, get_build_reply)},
    //{"getbit",      "r",    3, 1,  1, 1, RedisCmdType::GETBIT,      bind_fn2()},
    //{"getrange",    "r",    4, 1,  1, 1, RedisCmdType::GETRANGE,    bind_fn2()},
    //{"substr",      "r",    4, 1,  1, 1, RedisCmdType::SUBSTR,      bind_fn2()},
    //{"getset",      "wmd",  3, 1,  1, 1, RedisCmdType::GETSET,      bind_fn2()},
    //{"incr",        "wmd",  2, 1,  1, 1, RedisCmdType::INCR,        bind_fn2()},
    //{"incrby",      "wmd",  3, 1,  1, 1, RedisCmdType::INCRBY,      bind_fn2()},
    //{"incrbyfloat", "wmd",  3, 1,  1, 1, RedisCmdType::INCRBYFLOAT, bind_fn2()},
    {"mget",        "r",    -2, 1,  -1, 1, RedisCmdType::MGET,      bind_fn3(mget_encode, mget_split, mget_build_reply)},
    {"mset",        "wm",   -3, 1,  -1, 2, RedisCmdType::MSET,      bind_fn3(mset_encode, mset_split, mset_build_reply)},
    //{"msetnx",      "wm",   3, 1,  1, 2, RedisCmdType::MSETNX,      bind_fn2()},
    {"psetex",      "wmd",  4, 1,  1, 1, RedisCmdType::PSETEX,      bind_fn2(psetex_encode, psetex_build_reply)},
    {"set",         "wmd",  -3, 1,  1, 1, RedisCmdType::SET,        bind_fn2(set_encode, set_build_reply)},
    //{"setbit",      "wmd",  4, 1,  1, 1, RedisCmdType::SETBIT,      bind_fn2()},
    {"setex",       "wmd",  4, 1,  1, 1, RedisCmdType::SETEX,       bind_fn2(setex_encode, setex_build_reply)},
    //{"setnx",       "wmd",  3, 1,  1, 1, RedisCmdType::SETNX,       bind_fn2()},
    //{"setrange",    "wmd",  4, 1,  1, 1, RedisCmdType::SETRANGE,    bind_fn2()},
    {"strlen",      "r",    2, 1,  1, 1, RedisCmdType::STRLEN,      bind_fn2(get_encode, strlen_build_reply)},

    //hash
    {"hdel",         "w",  -3, 1, 1, 1, RedisCmdType::HDEL,         bind_fn3(hdel_encode, hdel_split, hdel_build_reply)},
    {"hexists",      "r",   3, 1, 1, 1, RedisCmdType::HEXISTS,      bind_fn2(hexists_encode, hexists_build_reply)},
    {"hget",         "r",   3, 1, 1, 1, RedisCmdType::HGET,         bind_fn2(hget_encode, hget_build_reply)},
    {"hgetall",      "r",   2, 1, 1, 1, RedisCmdType::HGETALL,      bind_fn2(hgetall_encode, hgetall_build_reply)},
    //{"hincrby",      "wm",  4, 1, 1, 1, RedisCmdType::HINCRBY,      bind_fn2()},
    //{"hincrbyfloat", "wm",  4, 1, 1, 1, RedisCmdType::HINCRBYFLOAT, bind_fn2()},
    //{"hkeys",        "rS",  2, 1, 1, 1, RedisCmdType::HKEYS,        bind_fn2()},
    //{"hlen",         "r",   2, 1, 1, 1, RedisCmdType::HLEN,         bind_fn2()},
    {"hmget",        "r",  -3, 1, 1, 1, RedisCmdType::HMGET,        bind_fn3(hmget_encode, hmget_split, hmget_build_reply)},
    {"hmset",        "wm", -4, 1, 1, 1, RedisCmdType::HMSET,        bind_fn3(hmset_encode, hmset_split, hmset_build_reply)},
    {"hset",         "wm",  4, 1, 1, 1, RedisCmdType::HSET,         bind_fn2(hset_encode, hset_build_reply)},
    //{"hsetnx",       "wm",  4, 1, 1, 1, RedisCmdType::HSETNX,       bind_fn2()},
    //{"hvals",        "rS",  2, 1, 1, 1, RedisCmdType::HVALS,        bind_fn2()},
    //{"hscan",        "rR", -3, 1, 1, 1, RedisCmdType::HSCAN,        bind_fn2()},

    //list
    /*
    {"blpop",      "ws",  -3, 1, -2, 1, RedisCmdType::BLPOP,      bind_fn2()},
    {"brpop",      "ws",  -3, 1,  1, 1, RedisCmdType::BRPOP,      bind_fn2()},
    {"brpoplpush", "wms",  4, 1,  2, 1, RedisCmdType::BRPOPLPUSH, bind_fn2()},
    {"lindex",     "r",    3, 1,  1, 1, RedisCmdType::LINDEX,     bind_fn2()},
    {"linsert",    "wm",   5, 1,  1, 1, RedisCmdType::LINSERT,    bind_fn2()},
    {"llen",       "r",    2, 1,  1, 1, RedisCmdType::LLEN,       bind_fn2()},
    {"lpop",       "w",    2, 1,  1, 1, RedisCmdType::LPOP,       bind_fn2()},
    {"lpush",      "wm",  -3, 1,  1, 1, RedisCmdType::LPUSH,      bind_fn2()},
    {"lpushx",     "wm",   3, 1,  1, 1, RedisCmdType::LPUSHX,     bind_fn2()},
    {"lrange",     "r",    4, 1,  1, 1, RedisCmdType::LRANGE,     bind_fn2()},
    {"lrem",       "w",    4, 1,  1, 1, RedisCmdType::LREM,       bind_fn2()},
    {"lset",       "wm",   4, 1,  1, 1, RedisCmdType::LSET,       bind_fn2()},
    {"ltrim",      "w",    4, 1,  1, 1, RedisCmdType::LTRIM,      bind_fn2()},
    {"rpop",       "w",    2, 1,  1, 1, RedisCmdType::RPOP,       bind_fn2()},
    {"rpoplpush",  "wm",   3, 1,  2, 1, RedisCmdType::RPOPLPUSH,  bind_fn2()},
    {"rpush",      "wm",  -3, 1,  1, 1, RedisCmdType::RPUSH,      bind_fn2()},
    {"rpushx",     "wm",   3, 1,  1, 1, RedisCmdType::RPUSHX,     bind_fn2()},

    //set
    {"sadd",        "wm", -3, 1,  1, 1, RedisCmdType::SADD,        bind_fn2()},
    {"scard",       "r",   2, 1,  1, 1, RedisCmdType::SCARD,       bind_fn2()},
    {"sdiff",       "rS", -2, 1, -1, 1, RedisCmdType::SDIFF,       bind_fn2()},
    {"sdiffstore",  "wm", -3, 1, -1, 1, RedisCmdType::SDIFFSTORE,  bind_fn2()},
    {"sinter",      "rS", -2, 1, -1, 1, RedisCmdType::SINTER,      bind_fn2()},
    {"sinterstore", "wm", -3, 1, -1, 1, RedisCmdType::SINTERSTORE, bind_fn2()},
    {"sismember",   "r",   3, 1,  1, 1, RedisCmdType::SISMEMBER,   bind_fn2()},
    {"smembers",    "rS",  2, 1,  1, 1, RedisCmdType::SMEMBERS,    bind_fn2()},
    {"smove",       "w",   4, 1,  2, 1, RedisCmdType::SMOVE,       bind_fn2()},
    {"spop",        "wRs", 2, 1,  1, 1, RedisCmdType::SPOP,        bind_fn2()},
    {"srandmember", "rR", -2, 1,  1, 1, RedisCmdType::SRANDMEMBER, bind_fn2()},
    {"srem",        "w",  -3, 1,  1, 1, RedisCmdType::SREM,        bind_fn2()},
    {"sunion",      "rS", -2, 1, -1, 1, RedisCmdType::SUNION,      bind_fn2()},
    {"sunionstore", "wm", -3, 1, -1, 1, RedisCmdType::SUNIONSTORE, bind_fn2()},
    {"sscan",       "rR", -3, 1,  1, 1, RedisCmdType::SSCAN,       bind_fn2()},

    //sorted set
    {"zadd",             "wm", -4, 1, 1, 1, RedisCmdType::ZADD,             bind_fn2()},
    {"zcard",            "r",   2, 1, 1, 1, RedisCmdType::ZCARD,            bind_fn2()},
    {"zcount",           "r",   4, 1, 1, 1, RedisCmdType::ZCOUNT,           bind_fn2()},
    {"zincrby",          "wm",  4, 1, 1, 1, RedisCmdType::ZINCRBY,          bind_fn2()},
    {"zrange",           "r",  -4, 1, 1, 1, RedisCmdType::ZRANGE,           bind_fn2()},
    {"zrangebyscore",    "r",  -4, 1, 1, 1, RedisCmdType::ZRANGEBYSCORE,    bind_fn2()},
    {"zrank",            "r",   3, 1, 1, 1, RedisCmdType::ZRANK,            bind_fn2()},
    {"zrem",             "w",  -3, 1, 1, 1, RedisCmdType::ZREM,             bind_fn2()},
    {"zremrangebyrank",  "w",   4, 1, 1, 1, RedisCmdType::ZREMRANGEBYRANK,  bind_fn2()},
    {"zremrangebyscore", "w",   4, 1, 1, 1, RedisCmdType::ZREMRANGEBYSCORE, bind_fn2()},
    {"zrevrange",        "r",  -4, 1, 1, 1, RedisCmdType::ZREVRANGE,        bind_fn2()},
    {"zrevrangebyscore", "r",  -4, 1, 1, 1, RedisCmdType::ZREMRANGEBYSCORE, bind_fn2()},
    {"zrevrank",         "r",   3, 1, 1, 1, RedisCmdType::ZREVRANK,         bind_fn2()},
    {"zscore",           "r",   3, 1, 1, 1, RedisCmdType::ZSCORE,           bind_fn2()},
    {"zunionstore",      "wm", -4, 0, 0, 0, RedisCmdType::ZUNIONSTORE,      bind_fn2()},
    {"zinterstore",      "wm", -4, 0, 0, 0, RedisCmdType::ZINTERSTORE,      bind_fn2()},
    {"zscan",            "rR", -3, 1, 1, 1, RedisCmdType::ZSCAN,            bind_fn2()},
    {"zrangebylex",      "r",  -4, 1, 1, 1, RedisCmdType::ZRANGEBYLEX,      bind_fn2()},
    {"zlexcount",        "r",   4, 1, 1, 1, RedisCmdType::ZLEXCOUNT,        bind_fn2()},
    {"zremrangebylex",   "w",   4, 1, 1, 1, RedisCmdType::ZREMRANGEBYLEX,   bind_fn2()},
    {"zrevrangebylex",   "r",  -4, 1, 1, 1, RedisCmdType::ZREVRANGEBYLEX,   bind_fn2()},

    //hyperloglog
    {"pfadd",      "wm", -2, 1,  1, 1, RedisCmdType::PFADD,      bind_fn2()},
    {"pfcount",    "w",  -2, 1,  1, 1, RedisCmdType::PFCOUNT,    bind_fn2()},
    {"pfmerge",    "wm", -2, 1, -1, 1, RedisCmdType::PFMERGE,    bind_fn2()},
    {"pfselftest", "r",   1, 0,  0, 0, RedisCmdType::PFSELFTEST, bind_fn2()},
    {"pfdebug",    "w",  -3, 0,  0, 0, RedisCmdType::PFDEBUG,    bind_fn2()},

    //pub/sub
    {"psubscribe",   "rpslt", -2, 0, 0, 0, RedisCmdType::PSUBSCRIBE,   bind_fn2()},
    {"publish",      "pltr",   3, 0, 0, 0, RedisCmdType::PUBLISH,      bind_fn2()},
    {"pubsub",       "pltrR", -2, 0, 0, 0, RedisCmdType::PUBSUB,       bind_fn2()},
    {"punsubscribe", "rpslt", -1, 0, 0, 0, RedisCmdType::PUNSUBSCRIBE, bind_fn2()},
    {"subscribe",    "rpslt", -2, 0, 0, 0, RedisCmdType::SUBSCRIBE,    bind_fn2()},
    {"unsubscribe",  "rpslt", -1, 0, 0, 0, RedisCmdType::UNSUBSCRIBE,  bind_fn2()},

    //transaction
    {"discard", "rs",  1, 0,  0, 0, RedisCmdType::DISCARD, bind_fn2()},
    {"exec",    "sM",  1, 0,  0, 0, RedisCmdType::EXEC,    bind_fn2()},
    {"multi",   "rs",  1, 0,  0, 0, RedisCmdType::MULTI,   bind_fn2()},
    {"unwatch", "rs",  1, 0,  0, 0, RedisCmdType::UNWATCH, bind_fn2()},
    {"watch",   "rs", -2, 1, -1, 1, RedisCmdType::WATCH,   bind_fn2()},

    //script
    {"eval",    "s",   -3, 0, 0, 0, RedisCmdType::EVAL,    bind_fn2()},
    {"eval",    "s",   -4, 3, 3, 1, RedisCmdType::EVAL,    bind_fn2()}, // treat as single key, must be 4 fields: eval "return redis.call('set', KEYS[1], 'val')" 1 key_test
    {"evalsha", "s",   -3, 0, 0, 0, RedisCmdType::EVALSHA, bind_fn2()},
    {"script",  "ras", -2, 0, 0, 0, RedisCmdType::SCRIPT,  bind_fn2()},

    //connection
    {"auth",   "rslt", 2, 0, 0, 0, RedisCmdType::AUTH,   bind_fn2()},
    {"echo",   "r",    2, 0, 0, 0, RedisCmdType::ECHO,   bind_fn2()},
    {"ping",   "rt",   1, 0, 0, 0, RedisCmdType::PING,   bind_fn2()},
    {"select", "rl",   2, 0, 0, 0, RedisCmdType::SELECT, bind_fn2()},

    //server
    {"bgrewriteaof", "ar",     1, 0, 0, 0, RedisCmdType::BGREWRITEAOF, bind_fn2()},
    {"bgsave",       "ar",     1, 0, 0, 0, RedisCmdType::BGSAVE,       bind_fn2()},
    {"client",       "ar",    -2, 0, 0, 0, RedisCmdType::CLIENT,       bind_fn2()},
    {"config",       "art",   -2, 0, 0, 0, RedisCmdType::CONFIG,       bind_fn2()},
    {"dbsize",       "r",      1, 0, 0, 0, RedisCmdType::DBSIZE,       bind_fn2()},
    {"debug",        "as",    -2, 0, 0, 0, RedisCmdType::DEBUG,        bind_fn2()},
    {"flushdb",      "w",      1, 0, 0, 0, RedisCmdType::FLUSHDB,      bind_fn2()},
    {"flushall",     "w",      1, 0, 0, 0, RedisCmdType::FLUSHALL,     bind_fn2()},
    {"info",         "rlt",   -1, 0, 0, 0, RedisCmdType::INFO,         bind_fn2()},
    {"lastsave",     "rR",     1, 0, 0, 0, RedisCmdType::LASTSAVE,     bind_fn2()},
    {"monitor",      "ars",    1, 0, 0, 0, RedisCmdType::MONITOR,      bind_fn2()},
    {"psync",        "ars",    3, 0, 0, 0, RedisCmdType::PSYNC,        bind_fn2()},
    {"replconf",     "arslt", -1, 0, 0, 0, RedisCmdType::REPLCONF,     bind_fn2()},
    {"role",         "last",   1, 0, 0, 0, RedisCmdType::ROLE,         bind_fn2()},
    {"save",         "ars",    1, 0, 0, 0, RedisCmdType::SAVE,         bind_fn2()},
    {"shutdown",     "arlt",  -1, 0, 0, 0, RedisCmdType::SHUTDOWN,     bind_fn2()},
    {"slaveof",      "ast",    3, 0, 0, 0, RedisCmdType::SLAVEOF,      bind_fn2()},
    {"slowlog",      "r",     -2, 0, 0, 0, RedisCmdType::SLOWLOG,      bind_fn2()},
    {"sync",         "ars",    1, 0, 0, 0, RedisCmdType::SYNC,         bind_fn2()},
    {"time",         "rR",     1, 0, 0, 0, RedisCmdType::TIME,         bind_fn2()},
    */
};

JimStatus RedisCmdTable::Init() {
    auto count = sizeof(command_table)/sizeof(command_table[0]);
    for (size_t i = 0; i < count; i++) {
        g_cmds_[command_table[i].name] =  &command_table[i];
    }
    return JimStatus::OK;
}


} //namespace kv
} //namespace sdk
} //namespace jim
