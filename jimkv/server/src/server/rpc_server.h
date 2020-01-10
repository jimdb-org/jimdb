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

#include <google/protobuf/message.h>
#include "base/status.h"
#include "net/server.h"

namespace jimkv {
namespace ds {
namespace server {

class Worker;

class RPCServer final {
public:
    explicit RPCServer(const net::ServerOptions& ops);
    ~RPCServer();

    RPCServer(const RPCServer&) = delete;
    RPCServer& operator=(const RPCServer&) = delete;

    Status Start(const std::string& ip, uint16_t port, Worker* worker);
    Status Stop();

private:
    void onMessage(const net::Context& ctx, const net::MessagePtr& msg);

private:
    const net::ServerOptions ops_;
    std::unique_ptr<net::Server> net_server_;
    Worker* worker_ = nullptr;
};

} /* namespace server */
} /* namespace ds */
} /* namespace jimkv */

