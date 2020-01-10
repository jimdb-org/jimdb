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
#include "base/status.h"
#include "../raft_types.h"

namespace jimkv {
namespace raft {
namespace impl {
namespace transport {

typedef std::function<void(MessagePtr&)> MessageHandler;

class Connection {
public:
    Connection() = default;
    virtual ~Connection() = default;

    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;

    virtual Status Send(MessagePtr& msg) = 0;

    virtual Status Close() = 0;
};

class Transport {
public:
    Transport() = default;
    virtual ~Transport() = default;

    Transport(const Transport&) = delete;
    Transport& operator=(const Transport&) = delete;

    virtual Status Start(const std::string& listen_ip, uint16_t listen_port,
                         const MessageHandler& handler) = 0;
    virtual void Shutdown() = 0;

    virtual void SendMessage(MessagePtr& msg) = 0;

    // get a connection to send snapshot
    virtual Status GetConnection(uint64_t to, std::shared_ptr<Connection>* conn) = 0;
};

} /* namespace transport */
} /* namespace impl */
} /* namespace raft */
} /* namespace jimkv */
