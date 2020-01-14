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

#include <map>
#include "raft/raft.h"
#include "raft/server.h"
#include "telnet_command.h"

namespace jim {
namespace raft {
namespace playground {

class AddressManager;
class TelnetService;
class PGStateMachine;

class Server {
public:
    Server();
    ~Server();

    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;

    void run();
    void stop();

    std::string handleCommand(std::string& cmd);

private:
    std::string doCommand(std::vector<std::string>& args);

    void addHandler(const std::string& command, int argc,
                    CommandHandler handler, const std::string& usage);
    void setupHandleMap();
    std::string handleSubmit(const std::vector<std::string>& args);
    std::string handleMemeber(const std::vector<std::string>& args);
    std::string handleHelp(const std::vector<std::string>& args);
    std::string handleInfo(const std::vector<std::string>& args);
    std::string handleTruncate(const std::vector<std::string>& args);
    std::string handleElect(const std::vector<std::string>& args);
    std::string handleTest(const std::vector<std::string>& args);

private:
    std::shared_ptr<AddressManager> addrs_mgr_;
    std::shared_ptr<PGStateMachine> sm_;
    std::unique_ptr<RaftServer> rs_;
    std::shared_ptr<Raft> raft_;

    TelnetService* telnet_service_{nullptr};
    std::map<std::string, TelnetCommand> telnet_commands_;
};

} /* namespace playground  */
} /* namespace raft */
} /* namespace jim */
