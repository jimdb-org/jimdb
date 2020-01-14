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


#include "jim_node.h"

#include "jim_conn.h"
#include "jim_log.h"
#include "jim_client.h"

namespace jim {
namespace sdk {

ConnPtr JimNode::GetConn(uint16_t hash_code) {
    size_t try_get = conn_count_;
    while (try_get--) {
        auto conn = conns_[hash_code % conn_count_];
        if (conn != nullptr && conn->IsEstablished()) {
            return conn;
        }
        ++hash_code;
    }

    return nullptr;
}

void JimNode::CreateConn() {
    auto io_pool = JimClient::Instance().GetIOContextPool();
    for(uint32_t i = 0; i < conn_count_; i++) {
        auto conn = std::make_shared<JimConn>(id_, io_pool->GetIOContext(i));
        conn->Connect(ip_, port_);
        conns_[i] = conn;
    }
    CBLOG_DEBUG("create conn %u %s:%s", conn_count_, ip_.c_str(), port_.c_str());
}

bool JimNode::SendMsg(MsgPtr msg) {
    auto hash_code = msg->cmd()->hash_code();
    if (msg->cmd()->rw_flag() == CmdRWFlag::READ) {
        hash_code = round_num_++;
    }
    ConnPtr conn = GetConn(hash_code);
    if (conn == nullptr) {
        CBLOG_WARN("not found conncetion %s:%s", ip_.c_str(), port_.c_str());
        return false;
    }

    return conn->Send(msg);
}

} // namespace sdk
} // namespace jim

