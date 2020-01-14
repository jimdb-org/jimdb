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
#include "string.h"
#include <vector>
#include <map>
#include <functional>

#include "jim_kv_reply.h"
#include "jim_kv_common.h"
#include "jim_kv_reply.h"

#include "dspb/kv.pb.h"
#include "dspb/api.pb.h"
#include "dspb/function.pb.h"
#include "dspb/error.pb.h"

namespace jim {
namespace sdk {
namespace kv {

class JimKVCommand;

using ArrayRespPtr = std::vector<std::shared_ptr<dspb::RangeResponse>>;

using EncodeFunc = std::function<JimStatus(std::shared_ptr<JimKVCommand>, const ArrayString&)>;
using SplitFunc = std::function<JimStatus(const ArrayString&, std::vector<ArrayString>&)>;
using BuildReplyFunc = std::function<void(ArrayRespPtr&, KVReplyPtr&)>;

typedef struct _redis_cmd_entry {
    std::string name;
    std::string flags;
    int arity;
    int first_key;
    int last_key;
    int key_step;
    RedisCmdType cmd_type;

    EncodeFunc encode;
    SplitFunc split;
    BuildReplyFunc build_reply;

} RedisCmdEntry;

struct stringcmp {
    bool operator()(const std::string& lhs, const std::string& rhs) const {
        if (lhs.size() < rhs.size()) {
            return true;
        } else if (lhs.size() > rhs.size()) {
            return false;
        } else {
            if (strncasecmp(lhs.c_str(), rhs.c_str(), lhs.size()) < 0) {
                return true;
            }
            return false;
        }
    }
};

class RedisCmdTable {
public:
    static RedisCmdTable& Instance() {
        static RedisCmdTable table;
        return table;
    }

    ~RedisCmdTable() = default;
    RedisCmdTable(const RedisCmdTable&) = delete;
    RedisCmdTable& operator=(const RedisCmdTable&) = delete;

    JimStatus Init();

    RedisCmdEntry* kv_cmd_get(const std::string& name, int argc);

    RedisCmdEntry* multi_convert_single(const RedisCmdEntry* cmd);

private:
    RedisCmdTable() = default;

private:
    std::map<std::string, RedisCmdEntry*, stringcmp> g_cmds_;
};

CmdRWFlag kv_rw_flag(const RedisCmdEntry* cmd);

bool is_multi_key_cmd(const RedisCmdEntry* cmd);

} //namespace kv
} //namespace sdk
} //namespace jim
