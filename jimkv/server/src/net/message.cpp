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

#include "message.h"

#include "session.h"

namespace jimkv {
namespace net {

bool Context::Write(const MessagePtr& resp_msg) const {
    auto conn = session.lock();
    if (conn) {
        conn->Write(resp_msg);
        return true;
    } else {
        return false;
    }
}

bool Context::Write(const Head& req_head, std::vector<uint8_t>&& resp_body) const {
    auto resp_msg = NewMessage();
    resp_msg->body = std::move(resp_body);
    resp_msg->head.SetResp(req_head, static_cast<uint32_t>(resp_msg->body.size()));
    return this->Write(resp_msg);
}

}  // namespace net
}  // namespace jimkv
