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

#include "jimkv_kv_serialize.h"

#include <arpa/inet.h>

#include "base/util.h"
#include "dspb/api.pb.h"

#include "jimkv_common.h"
#include "jimkv_hash.h"
#include "jimkv_log.h"

#include "redis_cmd_table.h"

namespace jimkv {
namespace sdk {
namespace kv {

typedef dspb::RangeRequest::ReqCase KvType;


static const uint8_t kBytesMarker = 0x12;
static const uint8_t kEscape = 0x00;
static const uint8_t kEscapedTerm = 0x01;
static const uint8_t kEscaped00 = 0xff;
static const uint8_t kEscapedFF = 0x00;

static const uint8_t DATA_TYPE_PREFIX = 0x01;

static const uint8_t REDIS_TYPE_STRING = 0x10;
static const uint8_t REDIS_TYPE_LIST = 0x11;
static const uint8_t REDIS_TYPE_SET = 0x12;
static const uint8_t REDIS_TYPE_ZSET = 0x13;
static const uint8_t REDIS_TYPE_HASH = 0x14;
static const uint8_t REDIS_TYPE_META = 0x15;

//static const uint8_t REDIS_ENCODE_INT = 0xfa;
//static const uint8_t REDIS_ENCODE_HASH = 0xfb;

static const uint8_t kHashSeparator = 0x05;

void encode_uint16(const uint16_t num, std::string &buf) {
    buf.push_back(static_cast<char>((num >> 8) & 0xFF));
    buf.push_back(static_cast<char>(num & 0xFF));
}

bool decode_uint16(const std::string &data, size_t &offset, uint16_t& value) {
    if (offset + 2 > data.size()) {
        return false;
    }
    value = 0;
    value += static_cast<uint8_t>(data[offset]);
    value <<= 8;
    value += static_cast<uint8_t>(data[offset + 1]);
    offset += 2;
    return true;
}

void encode_uint32(const uint32_t num, std::string &buf) {
    buf.push_back(static_cast<char>((num >> 24) & 0xFF));
    buf.push_back(static_cast<char>((num >> 16) & 0xFF));
    buf.push_back(static_cast<char>((num >> 8) & 0xFF));
    buf.push_back(static_cast<char>(num & 0xFF));
}

void encode_uint64(const uint64_t num, std::string &buf) {
    buf.push_back(static_cast<char>((num >> 56) & 0xFF));
    buf.push_back(static_cast<char>((num >> 48) & 0xFF));
    buf.push_back(static_cast<char>((num >> 40) & 0xFF));
    buf.push_back(static_cast<char>((num >> 32) & 0xFF));
    buf.push_back(static_cast<char>((num >> 24) & 0xFF));
    buf.push_back(static_cast<char>((num >> 16) & 0xFF));
    buf.push_back(static_cast<char>((num >> 8) & 0xFF));
    buf.push_back(static_cast<char>(num & 0xFF));
}

bool decode_uint64(const std::string &data, size_t &offset, uint64_t& value) {
    if (offset + 8 > data.size()) {
        return false;
    }
    value = 0;
    for (int i = 0; i < 8; i++) {
        value <<= 8;
        value += static_cast<uint8_t>(data[offset + i]);
    }
    offset += 8;
    return true;
}

void encode_bytes_ascending(const char *value, size_t value_size, std::string &buf) {
    buf.push_back(static_cast<char>(kBytesMarker));
    for (size_t i = 0; i < value_size; i++) {
        buf.push_back(value[i]);
        if (static_cast<uint8_t>(value[i]) == kEscape) {
            buf.push_back(kEscaped00);
        }
    }
    buf.push_back(kEscape);
    buf.push_back(kEscapedTerm);
}

bool decode_bytes_ascending(const std::string& buf, size_t& pos, std::string &out) {
    if (pos >= buf.size() || buf[pos] != kBytesMarker) return false;
    out.clear();
    for (++pos; pos < buf.size();) {
        auto escapePos = buf.find((char) kEscape, pos);
        if (escapePos == std::string::npos || escapePos + 1 >= buf.size()) return false;
        auto escapeChar = (unsigned char) buf[escapePos + 1];
        if (escapeChar == kEscapedTerm) {
            out.append(buf.substr(pos, escapePos - pos));
            pos = escapePos + 2;
            return true;
        }
        if (escapeChar != kEscaped00) return false;
        out.append(buf.substr(pos, escapePos - pos + 1));
        pos = escapePos + 2;
    }
    return false;
}

static std::string key_encode(uint64_t table_id, const std::string &key) {
    std::string encode_key;

    //set encode_key
    encode_key.push_back(static_cast<char>(DATA_TYPE_PREFIX));
    encode_uint64(table_id, encode_key);
    encode_bytes_ascending(key.c_str(), key.length(), encode_key);

    return encode_key;
}

std::string hkey_encode(uint64_t table_id,
    const std::string &key, uint16_t key_hash, const std::string &field)
{
    std::string encode_key = key_encode(table_id, key);

    encode_key.push_back(REDIS_TYPE_HASH);
    encode_uint16(key_hash, encode_key);

    //encode_key.push_back(kHashSeparator);
    encode_bytes_ascending(field.c_str(), field.length(), encode_key);

    return encode_key;
}

static void set_key(CmdPtr cmd, const std::string &key, std::string &encode_key,
    uint16_t hash_code, KvType type)
{
    cmd->set_hash_code(hash_code);
    cmd->set_req_type(type);
    cmd->set_encode_key(encode_key);

    auto kv_cmd = std::dynamic_pointer_cast<JimKVCommand>(cmd);
    kv_cmd->set_origin_key(key);
}

JimStatus del_encode(CmdPtr cmd, const ArrayString &args) {
    uint16_t key_hash = jimkv_hash(args[1]);
    //set encode_key
    std::string encode_key = key_encode(cmd->table_id(), args[1]);

    //del command, don't encode hash code & type
    //encode_uint16(key_hash, encode_key);
    set_key(cmd, args[1], encode_key, key_hash, KvType::kKvDelRange);

    return JimStatus::OK;
}

JimStatus get_encode(CmdPtr cmd, const ArrayString &args) {
    uint16_t key_hash = jimkv_hash(args[1]);
    //set encode_key
    std::string encode_key = key_encode(cmd->table_id(), args[1]);

    encode_key.push_back(REDIS_TYPE_STRING);
    encode_uint16(key_hash, encode_key);

    set_key(cmd, args[1], encode_key, key_hash, KvType::kKvGet);
    cmd->set_rw_flag(CmdRWFlag::READ);

    return JimStatus::OK;
}

JimStatus set_encode(CmdPtr cmd, const ArrayString &args) {
    char *endptr;
    uint64_t expire = 0;
    if (args.size() == 5) {
        expire = strtoull(args[4].c_str(), &endptr, 10);

        if (strcasecmp(args[3].c_str(), "ex") == 0) {
            expire *= 1000;
            expire += NowMilliSeconds();
        } else if (strcasecmp(args[3].c_str(), "px") == 0) {
            expire += NowMilliSeconds();
        } else {
            return JimStatus::ERROR;
        }
    }

    return set_encode_ex(cmd, args, expire);
}

JimStatus mget_encode(CmdPtr cmd, const ArrayString &args) {
    return get_encode(cmd, args);
}

JimStatus mset_encode(CmdPtr cmd, const ArrayString &args) {
    return set_encode(cmd, args);
}

JimStatus setex_encode(CmdPtr cmd, const ArrayString &args) {
    char *endptr;
    uint64_t expire = strtoull(args[2].c_str(), &endptr, 10) * 1000;
    expire += NowMilliSeconds();

    auto &tmp = const_cast<ArrayString &> (args);
    tmp.erase(tmp.begin() + 2);

    return set_encode_ex(cmd, tmp, expire);
}

JimStatus psetex_encode(CmdPtr cmd, const ArrayString &args) {
    char *endptr;
    uint64_t expire = strtoull(args[2].c_str(), &endptr, 10);
    expire += NowMilliSeconds();

    auto &tmp = const_cast<ArrayString &> (args);
    tmp.erase(tmp.begin() + 2);

    return set_encode_ex(cmd, tmp, expire);
}

JimStatus set_encode_ex(CmdPtr cmd, const ArrayString &args, uint64_t expire) {
    uint16_t key_hash = jimkv_hash(args[1]);
    //set encode_key
    std::string encode_key = key_encode(cmd->table_id(), args[1]);

    encode_key.push_back(REDIS_TYPE_STRING);
    encode_uint16(key_hash, encode_key);

    std::string encode_value;
    //set encode value
    encode_uint64(expire, encode_value);
    encode_bytes_ascending(args[2].c_str(), args[2].size(), encode_value);

    set_key(cmd, args[1], encode_key, key_hash, KvType::kKvPut);

    cmd->set_encode_value(encode_value);

    return JimStatus::OK;
}

JimStatus hget_encode(CmdPtr cmd, const ArrayString &args) {
    uint16_t key_hash = jimkv_hash(args[1]);

    //set encode_key
    std::string origin_key = "hkey:" + args[1] + " field:" + args[2];
    std::string encode_key = hkey_encode(cmd->table_id(), args[1], key_hash, args[2]);

    set_key(cmd, origin_key, encode_key, key_hash, KvType::kKvGet);
    cmd->set_rw_flag(CmdRWFlag::READ);

    return JimStatus::OK;
}

JimStatus hexists_encode(CmdPtr cmd, const ArrayString &args) {
    return hget_encode(cmd, args);
}

JimStatus hset_encode(CmdPtr cmd, const ArrayString &args) {
    uint16_t key_hash = jimkv_hash(args[1]);

    //set encode_key
    std::string origin_key = "hkey:" + args[1] + " field:" + args[2];
    std::string encode_key = hkey_encode(cmd->table_id(), args[1], key_hash, args[2]);

    //set encode_value
    std::string encode_value;

    encode_uint64(0, encode_value);
    encode_bytes_ascending(args[3].c_str(), args[3].size(), encode_value);

    set_key(cmd, origin_key, encode_key, key_hash, KvType::kKvPut);

    cmd->set_encode_value(encode_value);

    return JimStatus::OK;
}

JimStatus hdel_encode(CmdPtr cmd, const ArrayString &args) {
    uint16_t key_hash = jimkv_hash(args[1]);

    //set encode_key
    std::string origin_key = "hkey:" + args[1] + " field:" + args[2];
    std::string encode_key = hkey_encode(cmd->table_id(), args[1], key_hash, args[2]);

    set_key(cmd, origin_key, encode_key, key_hash, KvType::kKvDelete);

    return JimStatus::OK;
}


JimStatus hmget_encode(CmdPtr cmd, const ArrayString &args) {
    return hget_encode(cmd, args);
}

JimStatus hmset_encode(CmdPtr cmd, const ArrayString &args) {
    return hset_encode(cmd, args);
}

JimStatus hgetall_encode(CmdPtr cmd, const ArrayString &args) {
    uint16_t key_hash = jimkv_hash(args[1]);

    //set encode_key
    std::string encode_key = key_encode(cmd->table_id(), args[1]);
    encode_key.push_back(REDIS_TYPE_HASH);

    set_key(cmd, args[1], encode_key, key_hash, KvType::kKvScan);
    cmd->set_rw_flag(CmdRWFlag::READ);

    return JimStatus::OK;
}

JimStatus exists_encode(CmdPtr cmd, const ArrayString &args) {
    uint16_t key_hash = jimkv_hash(args[1]);
    //set encode_key
    std::string encode_key = key_encode(cmd->table_id(), args[1]);

    set_key(cmd, args[1], encode_key, key_hash, KvType::kKvScan);
    cmd->set_rw_flag(CmdRWFlag::READ);

    return JimStatus::OK;
}

JimStatus ttl_encode(CmdPtr cmd, const ArrayString &args) {
    return get_encode(cmd, args);
}

JimStatus pttl_encode(CmdPtr cmd, const ArrayString &args) {
    return get_encode(cmd, args);
}

JimStatus key_decode(const std::string &src_buf, std::string &dest_buf) {
    //prefix+table_id+key+type+hashcode+field
    size_t offset = 0;
    uint64_t table_id;
    uint16_t key_hash;

    if ( src_buf.size() == 0 || src_buf[offset] != DATA_TYPE_PREFIX) {
        return JimStatus::ERROR;
    }

    //prefix pos
    offset = offset + 1;

    //table_id pos
    bool ret = decode_uint64(src_buf, offset, table_id);
    if (!ret) {
        CBLOG_ERROR("received error: invalid expire");
        return JimStatus::ERROR;
    }
    CBLOG_DEBUG("table_id is %" PRIu64, table_id);

    //key pos
    ret = decode_bytes_ascending(src_buf, offset, dest_buf);
    if (!ret) {
        CBLOG_ERROR("received error: invalid key");
        return JimStatus::ERROR;
    }

    //type pos
    auto type = static_cast<uint8_t>(src_buf[offset]);
    if (type == REDIS_TYPE_STRING) {
        return JimStatus::OK;
    }
    offset = offset + 1;

    //hash_code pos
    ret = decode_uint16(src_buf, offset, key_hash);
    if (!ret) {
        CBLOG_ERROR("received error: invalid key_hash");
        return JimStatus::ERROR;
    }
    CBLOG_DEBUG("received hash_code is %" PRIu16, key_hash);

    //field pos
    ret = decode_bytes_ascending(src_buf, offset, dest_buf);
    if (!ret) {
        CBLOG_ERROR("received error: invalid field");
        return JimStatus::ERROR;
    }
    return JimStatus::OK;
}

JimStatus value_decode(const std::string &src_buf, std::string &dest_buf, uint64_t &expire) {
    size_t offset = 0;
    bool ret = decode_uint64(src_buf, offset, expire);
    if (!ret) {
        CBLOG_ERROR("received error: invalid expire");
        return JimStatus::ERROR;
    }
    uint64_t now = NowMilliSeconds();

    if (expire != 0 && expire < now) {
        return JimStatus::EXPIRED;
    }

    CBLOG_DEBUG("expire is %" PRIu64, expire);
    ret = decode_bytes_ascending(src_buf, offset, dest_buf);
    if (!ret) {
        CBLOG_ERROR("received error: invalid value");
        return JimStatus::ERROR;
    }

    return JimStatus::OK;
}

} //namespace kv
} //namespace sdk
} //namespace jimkv
