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

#include <vector>
#include <memory>
#include "jim_kv_common.h"

namespace jim {
namespace sdk {
namespace kv {

class JimKVReply {
public:
    JimKVReply() = default;
    ~JimKVReply() = default;
    JimKVReply(const JimKVReply&) = delete;
    JimKVReply& operator=(const JimKVReply&) = delete;

    JimKVReply(JimKVReply&&);

    using KVReplyPtr = std::shared_ptr<JimKVReply>;
    using ArrayKVReplyPtr = std::vector<KVReplyPtr>;

public:
    void set_type(const JimKVReplyType& type);

    JimKVReplyType& type();

    void set_buf(const std::string& data);
    std::string& buf();

    void set_integer(const int64_t interger);
    int64_t integer();

    uint32_t element_size();
    void add_element(KVReplyPtr reply);
    const ArrayKVReplyPtr& elements();
private:
    JimKVReplyType type_;
    std::string buf_; //result of string or error string when error
    int64_t interger_; //when result is integer, set this field
    ArrayKVReplyPtr elements_; //elements vector when type is JIMKV_ARRAY
};

using KVReplyPtr = std::shared_ptr<JimKVReply>;
using ArrayKVReplyPtr = std::vector<KVReplyPtr>;

inline KVReplyPtr NewJimKVReply() { return std::make_shared<JimKVReply>();}

} //namespace kv
} //namespace sdk
} //namespace jim
