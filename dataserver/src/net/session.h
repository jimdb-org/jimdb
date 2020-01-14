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

#include <queue>
#include <memory>
#include "asio/ip/tcp.hpp"
#include "asio/streambuf.hpp"

#include "message.h"
#include "options.h"
#include "protocol.h"

namespace jim {
namespace net {

class Session : public std::enable_shared_from_this<Session> {
public:
    enum class Direction {
        kClient,
        kServer,
    };

public:
    Session(const SessionOptions& opt, const Handler& handler, asio::ip::tcp::socket socket);
    Session(const SessionOptions& opt, const Handler& handler, asio::io_context& io_context);

    ~Session();

    Session(const Session&) = delete;
    Session& operator=(const Session&) = delete;

    // for server session
    void Start();

    // for client session
    void Connect(const std::string& address, const std::string& port);

    void Write(const MessagePtr& msg);
    void Close();

private:
    void do_close();
    void do_write();
    bool init_establish();

    void read_head();
    void read_body();

private:
    const SessionOptions opt_;
    Handler handler_;

    asio::ip::tcp::socket socket_;
    Direction direction_;
    std::string local_addr_;
    std::string remote_addr_;
    std::string id_;

    bool established_ = false;
    std::atomic<bool> closed_ = {false};

    Head head_;
    std::vector<uint8_t> body_;

    std::deque<MessagePtr> write_msgs_;
};

}  // namespace net
}  // namespace jim
