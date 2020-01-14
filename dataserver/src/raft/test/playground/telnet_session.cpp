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

#include "telnet_session.h"

#include <iostream>

#include "asio/read_until.hpp"
#include "asio/write.hpp"
#include "server.h"

namespace jim {
namespace raft {
namespace playground {

TelnetSession::TelnetSession(Server* s, asio::ip::tcp::socket socket)
    : server_(s), socket_(std::move(socket)) {}

TelnetSession::~TelnetSession() {}

void TelnetSession::start() { do_read(); }

void TelnetSession::do_read() {
    auto self(shared_from_this());
    asio::async_read_until(
        socket_, buffer_, "\r\n",
        [this, self](std::error_code ec, std::size_t length) {
            if (!ec) {
                std::string cmd(asio::buffer_cast<const char*>(buffer_.data()),
                                length);
                buffer_.consume(length);
                if (cmd == "q\r\n" || cmd == "quit\r\n") {
                    return;
                } else {
                    std::string resp = server_->handleCommand(cmd);
                    if (!resp.empty()) {
                        do_write(resp);
                    } else {
                        do_read();
                    }
                }
            } else {
                std::cerr << "read telnet session error: " << ec.message()
                          << std::endl;
            }
        });
}

void TelnetSession::do_write(const std::string& s) {
    auto self(shared_from_this());
    asio::async_write(socket_, asio::buffer(s.data(), s.size()),
                      [this, self, s](std::error_code ec, std::size_t length) {
                          if (!ec) {
                              do_read();
                          } else {
                              std::cerr << "write telnet session error: "
                                        << ec.message() << std::endl;
                          }
                      });
}

}  // namespace playground
}  // namespace raft
} /* namespace jim */
