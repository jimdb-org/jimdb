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

#include <arpa/inet.h>
#include <vector>
#include <string>

#include "dspb/api.pb.h"

#include "jimkv_hash.h"
#include "jimkv_common.h"
#include "jimkv_kv_command.h"

namespace jimkv {
namespace sdk {
namespace kv {

void encode_uint16(const uint16_t num, std::string &buf);

void encode_uint32(const uint32_t num, std::string &buf);

void encode_uint64(const uint64_t num, std::string &buf);

bool decode_uint64(const std::string &data, size_t &offset, uint64_t& value);

void encode_bytes_ascending(const char *value, size_t value_size, std::string &buf);

bool decode_bytes_ascending(const std::string& buf, size_t& pos, std::string &out);

JimStatus set_encode_ex(CmdPtr cmd, const ArrayString &args, uint64_t expire);

JimStatus del_encode(CmdPtr cmd, const ArrayString &args);

JimStatus get_encode(CmdPtr cmd, const ArrayString &args);

JimStatus set_encode(CmdPtr cmd, const ArrayString &args);

JimStatus setex_encode(CmdPtr cmd, const ArrayString &args);

JimStatus psetex_encode(CmdPtr cmd, const ArrayString &args);

JimStatus mget_encode(CmdPtr cmd, const ArrayString &args);

JimStatus mset_encode(CmdPtr cmd, const ArrayString &args);

JimStatus hget_encode(CmdPtr cmd, const ArrayString &args);

JimStatus hexists_encode(CmdPtr cmd, const ArrayString &args);

JimStatus hset_encode(CmdPtr cmd, const ArrayString &args);

JimStatus hdel_encode(CmdPtr cmd, const ArrayString &args);

JimStatus hmget_encode(CmdPtr cmd, const ArrayString &args);

JimStatus hmset_encode(CmdPtr cmd, const ArrayString &args);

JimStatus hgetall_encode(CmdPtr cmd, const ArrayString &args);

JimStatus exists_encode(CmdPtr cmd, const ArrayString &args);

JimStatus ttl_encode(CmdPtr cmd, const ArrayString &args);

JimStatus pttl_encode(CmdPtr cmd, const ArrayString &args);

std::string hkey_encode(uint64_t table_id,
    const std::string &key, uint16_t key_hash, const std::string &field);

JimStatus key_decode(const std::string &src_buf, std::string &dest_buf);

JimStatus value_decode(const std::string &src_buf, std::string &dest_buf,uint64_t &expire);

} //namespace kv
} //namespace sdk
} //namespace jimkv
