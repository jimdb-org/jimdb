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

#include "session.h"

#include "asio/read.hpp"
#include "asio/write.hpp"
#include "asio/read_until.hpp"
#include "asio/connect.hpp"

#include "common/logger.h"

namespace jimkv {
namespace net {

Session::Session(const SessionOptions& opt, const Handler& handler, asio::ip::tcp::socket socket) :
    opt_(opt),
    handler_(handler),
    socket_(std::move(socket)) {
    if (opt_.statistics) {
        opt_.statistics->AddSessionCount(1);
    }
}

Session::Session(const SessionOptions& opt, const Handler& handler, asio::io_context& io_context) :
    Session(opt, handler, asio::ip::tcp::socket(io_context)) {
}

Session::~Session() {
    do_close();

    if (opt_.statistics) {
        opt_.statistics->AddSessionCount(-1);
    }

    FLOG_INFO("{} destroyed.", id_);
}

void Session::Start() {
    direction_ = Direction::kServer;

    if (init_establish()) {
        read_head();
    } else {
        do_close();
    }
}

void Session::Connect(const std::string& address, const std::string& port) {
    direction_ = Direction::kClient;
    id_ = std::string("C[unknown<->") + address + ":" + port + "]";
    remote_addr_ = address + ":" + port;

    asio::ip::tcp::resolver resolver(socket_.get_executor());
    asio::error_code ec;
    auto endpoints = resolver.resolve(address, port, ec);
    if (ec) {
        FLOG_ERROR("[Net] resolve {}:{} error: {}", address, port, ec.message()) ;
        Close();
        return;
    }

    auto self(shared_from_this());
    asio::async_connect(socket_, endpoints,
            [self, address, port](asio::error_code ec, asio::ip::tcp::endpoint) {
                if (ec) {
                    FLOG_ERROR("[Net] connect to {}:{} error: {}",
                            address, port, ec.message());
                    self->do_close();
                    return;
                }
                if (!self->init_establish()) {
                    self->do_close();
                    return;
                }
                // start read
                self->read_head();
                // start send
                if (!self->write_msgs_.empty()) {
                    self->do_write();
                }
            });
}

bool Session::init_establish() {
    try {
        auto local_ep = socket_.local_endpoint();
        local_addr_  = local_ep.address().to_string() + ":" + std::to_string(local_ep.port());

        auto remote_ep = socket_.remote_endpoint();
        remote_addr_ = remote_ep.address().to_string() + ":" + std::to_string(remote_ep.port());
    } catch (std::exception& e) {
        FLOG_ERROR("[Net] get socket addr error: {}", e.what());
        return false;
    }

    id_.clear();
    id_.push_back(direction_ == Direction::kServer ? 'S' : 'C');
    id_ += "[" + local_addr_ + "<->" + remote_addr_ + "]";

    socket_.set_option(asio::ip::tcp::no_delay(true));

    established_ = true;
    FLOG_INFO("{} establised.", id_);

    return true;
}

void Session::Close() {
    if (closed_) {
        return;
    }
    auto self(shared_from_this());
    asio::post(socket_.get_executor(), [self] { self->do_close(); });
}

void Session::do_close() {
    if (closed_) {
        return;
    }

    closed_ = true;
    asio::error_code ec;
    socket_.close(ec);
    (void)ec;

    FLOG_INFO("{} closed. ", id_);
}

void Session::read_head() {
    auto self(shared_from_this());
    asio::async_read(socket_, asio::buffer(&head_, sizeof(head_)),
            [this, self](std::error_code ec, std::size_t) {
                if (!ec) {
                    head_.Decode();
                    auto ret = head_.Valid();
                    if (ret.ok()) {
                        read_body();
                    } else {
                        FLOG_ERROR("{} invalid rpc head: {}", id_, ret.ToString());
                        do_close();
                    }
                } else {
                    if (ec == asio::error::eof) {
                        FLOG_INFO("{} read rpc head error: {}", id_, ec.message());
                    } else {
                        FLOG_ERROR("{} read rpc head error: {}", id_, ec.message());
                    }
                    do_close();
                }
            });
}

void Session::read_body() {
    if (head_.body_length == 0) {
        if (head_.func_id == kHeartbeatFuncID) { // response heartbeat
            if (opt_.statistics) {
                opt_.statistics->AddMessageRecv(sizeof(head_));
            }
            auto msg = NewMessage();
            msg->head.SetResp(head_, 0);
            Write(msg);
        }
        read_head();
        return;
    }

    body_.resize(head_.body_length);
    auto self(shared_from_this());
    asio::async_read(socket_, asio::buffer(body_.data(), body_.size()),
            [this, self](std::error_code ec, std::size_t length) {
                if (!ec) {
                    if (opt_.statistics) {
                        opt_.statistics->AddMessageRecv(sizeof(head_) + length);
                    }

                    Context ctx;
                    ctx.session = self;
                    ctx.local_addr = local_addr_;
                    ctx.remote_addr = remote_addr_;
                    auto msg = NewMessage();
                    msg->head = head_;
                    msg->body = std::move(body_);
                    handler_(ctx, msg);

                    read_head();
                } else {
                    if (ec == asio::error::eof) {
                        FLOG_INFO("{} read rpc body error: {}", id_, ec.message());
                    } else {
                        FLOG_ERROR("{} read rpc body error: {}", id_, ec.message());
                    }
                    do_close();
                }
            });
}

void Session::do_write() {
    auto self(shared_from_this());

    // prepare write buffer
    auto msg = write_msgs_.front();
    msg->head.body_length = static_cast<uint32_t>(msg->body.size());
    msg->head.Encode();
    std::vector<asio::const_buffer> buffers{
        asio::buffer(&msg->head, sizeof(msg->head)),
        asio::buffer(msg->body.data(), msg->body.size())
    };

    asio::async_write(socket_, buffers,
            [this, self](std::error_code ec, std::size_t length) {
                if (!ec) {
                    if (opt_.statistics) {
                        opt_.statistics->AddMessageSent(length);
                    }
                    write_msgs_.pop_front();
                    if (!write_msgs_.empty()) {
                        do_write();
                    }
                } else {
                    FLOG_ERROR("{} write message error: {}", id_, ec.message());
                    do_close();
                }
            });
}

void Session::Write(const MessagePtr& msg) {
    if (closed_) {
        return;
    }

    auto self(shared_from_this());
    asio::post(socket_.get_executor(), [self, msg] {
        bool write_in_progress = !self->write_msgs_.empty();
        self->write_msgs_.push_back(msg);
        if (!write_in_progress && self->established_) {
            self->do_write();
        }
    });
}

} /* net */
} /* jimkv */
