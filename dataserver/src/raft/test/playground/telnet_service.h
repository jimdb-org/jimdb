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

#include "asio/io_service.hpp"
#include "asio/ip/tcp.hpp"

namespace jim {
namespace raft {
namespace playground {

class Server;

class TelnetService {
public:
    TelnetService(Server* server, uint16_t port);
    ~TelnetService();

    TelnetService(const TelnetService&) = delete;
    TelnetService& operator=(const TelnetService&) = delete;

private:
    void do_accept();

private:
    Server* server_;

    asio::io_service io_service_;
    asio::ip::tcp::acceptor acceptor_;
    asio::ip::tcp::socket socket_;
};

} /* namespace playground */
} /* namespace raft */
} /* namespace jim */
