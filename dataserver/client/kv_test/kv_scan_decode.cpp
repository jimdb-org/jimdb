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

#include "jim_kv_context.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "kv_string.h"
#include "kv_hash.h"
#include "kv_reply.h"
#include "jim_kv_serialize.h"
#include "jim_log.h"

using namespace jim;
using namespace jim::sdk;
using namespace jim::sdk::kv;

static const uint8_t DATA_TYPE_PREFIX = 0x01;
static const uint8_t REDIS_TYPE_STRING = 0x10;
static const uint8_t REDIS_TYPE_HASH = 0x14;

int main(int argc, char **argv) {
    jimkv_log_level_set(JIMKV_LOG_DEBUG);
    std::string orig_key = "jim_kv_scan_key_decode_kkkkkk";
    std::string field0 = "field0";
    std::string encode_key;
    uint64_t table_id = 1;
    uint16_t key_hash = jimkv_hash(orig_key);
    CBLOG_DEBUG("hash %" PRIu16, key_hash);
    //set encode_key

    encode_key.push_back(static_cast<char>(DATA_TYPE_PREFIX));
    encode_uint64(table_id, encode_key);
    encode_bytes_ascending(orig_key.c_str(), orig_key.length(), encode_key);
    //std::string encode_key = key_encode(table_id, orig_key);

    encode_key.push_back(REDIS_TYPE_HASH);
    encode_uint16(key_hash, encode_key);
    encode_bytes_ascending(field0.c_str(), field0.length(), encode_key);
    //encode_key = hkey_encode(table_id, orig_key, key_hash, field0);
    CBLOG_DEBUG("before key %s", encode_key.c_str());

    std::string after_key;
    //scan_key_decode(encode_key, after_key);
    if (key_decode(encode_key, after_key) != JimStatus::OK) {
        //reply->set_type(JimKVReplyType::REPLY_ERROR);
        //reply->set_buf("data error");
        CBLOG_ERROR("error result: %s", after_key.c_str());
        return -1;
    }
    CBLOG_INFO("result: %s", after_key.c_str());
    return 0;
}
