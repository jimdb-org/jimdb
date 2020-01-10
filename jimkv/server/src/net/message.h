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

_Pragma("once");

#include <functional>
#include <vector>
#include <memory>
#include "protocol.h"

namespace jimkv {
namespace net {

class Session;

struct Message {
    Head head;
    std::vector<uint8_t> body;
};

using MessagePtr = std::shared_ptr<Message>;

inline MessagePtr NewMessage() { return std::make_shared<Message>(); }


struct Context {
    std::weak_ptr<Session> session;
    std::string remote_addr;
    std::string local_addr;

    bool Write(const MessagePtr& resp_msg) const;

    bool Write(const Head& req_head, std::vector<uint8_t>&& resp_body) const;
};


using Handler = std::function<void(const Context&, const MessagePtr& msg)>;

}  // namespace net
}  // namespace jimkv
