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

#include "server.h"

#include "base/util.h"
#include "common/logger.h"

#include "context_pool.h"
#include "session.h"

namespace jimkv {
namespace net {

Server::Server(const ServerOptions& opt, const std::string& name) :
    name_(name),
    opt_(opt),
    acceptor_(context_),
    context_pool_(new IOContextPool(opt.io_threads_num, name)) {
    if (opt_.session_opt.statistics == nullptr) {
        opt_.session_opt.statistics = std::make_shared<Statistics>();
    }
}

Server::~Server() {
    Stop();
}

Status Server::ListenAndServe(const std::string& listen_ip, uint16_t listen_port,
                              const Handler& handler) {
    std::string bind_ip = listen_ip;
    if (bind_ip.empty()) {
        bind_ip = "0.0.0.0";
    }
    try {
        asio::ip::tcp::endpoint endpoint(asio::ip::make_address(bind_ip), listen_port);
        acceptor_.open(endpoint.protocol());
        acceptor_.set_option(asio::ip::tcp::acceptor::reuse_address(true));
        acceptor_.bind(endpoint);
        acceptor_.listen(asio::socket_base::max_listen_connections);
    } catch (std::exception& e) {
        return Status(Status::kIOError, "listen", e.what());
    }

    handler_ = handler;
    context_pool_->Start();

    doAccept();

    thr_.reset(new std::thread([this]() {
        try {
            context_.run();
        } catch (...) {
        }
    }));

    char thr_name[16] = {'\0'};
    snprintf(thr_name, 16, "%s-acpt", name_.c_str());
    AnnotateThread(thr_->native_handle(), thr_name);

    thr_->detach();

    return Status::OK();
}

void Server::Stop() {
    if (stopped_) return;

    stopped_ = true;

    acceptor_.close();
    context_.stop();
    context_pool_->Stop();
}

void Server::doAccept() {
    acceptor_.async_accept(getContext(), [this](const std::error_code& ec,
                                                asio::ip::tcp::socket socket) {
        if (ec) {
            FLOG_ERROR("[Net] accept error: {}", ec.message());
        } else if (opt_.session_opt.statistics->session_count > opt_.max_connections) {
            FLOG_WARN("[Net] accept max connection limit reached: {}", opt_.max_connections);
        } else {
            FLOG_DEBUG("accept connection remote: {}", socket.remote_endpoint().address().to_string());
            std::make_shared<Session>(opt_.session_opt, handler_, std::move(socket))->Start();
        }

        doAccept();
    });
}

asio::io_context& Server::getContext() {
    if (context_pool_->Size() > 0) {
        return context_pool_->GetIOContext();
    } else {
        return context_;
    }
}

}  // namespace net
}  // namespace jimkv
