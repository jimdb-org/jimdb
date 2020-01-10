
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

#include "jimkv_kv_reply.h"
namespace jimkv {
namespace sdk {
namespace kv {

JimKVReply::JimKVReply(JimKVReply&& other) {
    type_ = other.type_;
    interger_ = other.interger_;
    buf_ = std::move(other.buf_);
    elements_ = std::move(other.elements_);
}


void JimKVReply::set_type(const JimKVReplyType& type) {
    type_ = type;
}
JimKVReplyType& JimKVReply::type() {
    return type_;
}

void JimKVReply::set_buf(const std::string& data) {
    //todo optimize
    buf_ = data;
}
std::string& JimKVReply::buf() {
    return buf_;
}

void JimKVReply::set_integer(const int64_t integer) {
    interger_ = integer;
}

int64_t JimKVReply::integer() {
    return interger_;
}

uint32_t JimKVReply::element_size() {
    return elements_.size();
}

void JimKVReply::add_element(KVReplyPtr reply) {
    elements_.push_back(reply);
}

const ArrayKVReplyPtr& JimKVReply::elements() {
    return elements_;
}

} //namespace kv
} //namespace sdk
} //namespace jimkv
