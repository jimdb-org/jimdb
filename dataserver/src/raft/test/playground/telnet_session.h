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

#include <memory>

#include "asio/ip/tcp.hpp"
#include "asio/streambuf.hpp"

namespace jim {
namespace raft {
namespace playground {

class Server;

class TelnetSession : public std::enable_shared_from_this<TelnetSession> {
public:
    TelnetSession(Server* s, asio::ip::tcp::socket socket);
    ~TelnetSession();

    TelnetSession(const TelnetSession&) = delete;
    TelnetSession& operator=(const TelnetSession&) = delete;

    void start();

private:
    void do_read();
    void do_write(const std::string& s);

private:
    Server* server_;
    asio::ip::tcp::socket socket_;
    asio::streambuf buffer_;
};

} /* namespace playground */
} /* namespace raft */
} /* namespace jim */
