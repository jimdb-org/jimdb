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

_Pragma ("once");

#include <memory>
#include <string>
#include <vector>

#include "jimkv_conn.h"
#include "jimkv_message.h"

namespace jimkv {
namespace sdk {

class JimNode {
public:
    JimNode(uint64_t id, const std::string& ip, uint32_t port, uint32_t conn_count)
        : id_(id), ip_(ip), port_(std::to_string(port)), conn_count_(conn_count)
    {
        conns_.resize(conn_count_);
    }

    ~JimNode() = default;

    JimNode(const JimNode&) = delete;
    JimNode& operator=(const JimNode&) = delete;

    void CreateConn();
    bool SendMsg(MsgPtr msg);

private:
    ConnPtr GetConn(uint16_t hash_code);

private:
    uint64_t id_;
    std::string ip_;
    std::string port_;

    uint32_t conn_count_;
    uint16_t round_num_ = 0;
    std::vector<ConnPtr> conns_;
};

using NodePtr = std::shared_ptr<JimNode>;

} // namespace sdk
} // namespace jimkv

