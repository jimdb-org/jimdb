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

_Pragma ("once");

#include <string>
#include <deque>
#include <vector>
#include <map>
#include <memory>

#include "asio/io_context.hpp"
#include "asio/ip/tcp.hpp"
#include "asio/steady_timer.hpp"

#include "net/protocol.h"
#include "jim_message.h"

using tcp = asio::ip::tcp;

namespace jim {
namespace sdk {

using CompleteFunc = std::function<void()>;

class JimConn final : public std::enable_shared_from_this<JimConn> {
public:
    enum class ConnState { Closed, Connecting, Established, Closing };
    const int kConnectInterval = 2;  //s
    const int kConnectMaxInterval = 1800; //60 * 30 s
    const int kIdleTime  = 60; //s

public:
    JimConn(uint64_t node_id, asio::io_context& io_ctx) :
        node_id_(node_id),
        socket_(io_ctx),
        deadline_(io_ctx)
    {};

    ~JimConn() = default;

    JimConn(const JimConn&) = delete;
    JimConn& operator=(const JimConn&) = delete;

    bool Connect(std::string &ip, std::string &port);
    bool IsEstablished() {return state_ == ConnState::Established;}

    bool Send(MsgPtr& msg);
    void Close();

private:
    void read_head();
    void read_body();
    void do_close();

    void do_connect();
    void do_reconnect();

    void do_send();
    void do_write(MsgPtr msg);

    void do_idle();
    void Heartbeat();

    void ClearMsg();
    bool CheckError(RespPtr resp);
    void ParseBody(MsgPtr msg);
private:
    uint64_t node_id_;
    std::string remote_addr_;

    std::atomic<ConnState> state_ = {ConnState::Closed};

    int reconnect_times_ = 0;

    tcp::socket socket_;
    tcp::resolver::results_type endpoint_;
    int fd_;

    asio::steady_timer deadline_;

    net::Head head_;
    std::vector<uint8_t> body_;

    std::deque<MsgPtr> write_msgs_;
    std::map<uint64_t, MsgPtr> sent_msgs_;
};

using ConnPtr = std::shared_ptr<JimConn>;

} // namespace sdk
} // namespace jim

