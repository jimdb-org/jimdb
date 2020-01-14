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

_Pragma("once");

#include "base/system_info.h"
#include "context_server.h"

namespace jim {
namespace ds {

namespace admin { class AdminServer; }

namespace server {

class DataServer {
public:
    ~DataServer();

    DataServer(const DataServer &) = delete;
    DataServer &operator=(const DataServer &) = delete;

    static DataServer &Instance() {
        static DataServer instance_;
        return instance_;
    };

    bool Start();
    void Stop();

    ContextServer *context_server() { return context_; }

    void ServerCron();

private:
    DataServer();

    void registerToMaster();
    bool startRaftServer();
    void heartbeat();
    void triggerRCU();

private:
    ContextServer *context_ = nullptr;
    std::unique_ptr<admin::AdminServer> admin_server_;
};

} /* namespace server */
} /* namespace ds  */
} /* namespace jim */
