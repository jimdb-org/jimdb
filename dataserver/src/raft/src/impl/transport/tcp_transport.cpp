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

#include "tcp_transport.h"

#include <cinttypes>
#include <mutex>
#include "common/logger.h"

namespace jim {
namespace raft {
namespace impl {
namespace transport {

TcpConnection::TcpConnection(const std::shared_ptr<net::Session>& session) :
    session_(session) {
}

Status TcpConnection::Send(MessagePtr& raft_msg) {
    static std::atomic<uint64_t> msg_id_increaser = {1};

    // package message
    auto net_msg = net::NewMessage();
    net_msg->head.msg_id = msg_id_increaser.fetch_add(1);
    net_msg->head.msg_type = net::kDataRequestType;
    auto& body = net_msg->body;
    body.resize(raft_msg->ByteSizeLong());
    if (!raft_msg->SerializeToArray(body.data(), static_cast<int>(body.size()))) {
        return Status(Status::kCorruption, "serialize raft msg", raft_msg->ShortDebugString());
    }

    auto conn = session_.lock();
    if (!conn) {
        return Status(Status::kIOError, "connection closed.", std::to_string(raft_msg->to()));
    } else {
        conn->Write(net_msg);
        return Status::OK();
    }
}

Status TcpConnection::Close() {
    auto conn = session_.lock();
    if (conn) {
        conn->Close();
    }
    return Status::OK();
}


TcpTransport::ConnectionGroup::ConnectionGroup(const CreateConnFunc& create_func) :
    create_func_(create_func) {
}

TcpConnPtr TcpTransport::ConnectionGroup::Get(uint64_t to) {
    {
        jim::shared_lock<jim::shared_mutex> locker(mu_);
        auto it = connections_.find(to);
        if (it != connections_.end()) {
            return it->second;
        }
    }

    std::unique_lock<jim::shared_mutex> locker(mu_);
    auto it = connections_.find(to);
    if (it != connections_.end()) {
        return it->second;
    }

    TcpConnPtr conn;
    auto ret = create_func_(to, conn);
    if (!ret.ok()) {
        return nullptr;
    }
    connections_.emplace(to, conn);
    return conn;
}

void TcpTransport::ConnectionGroup::Remove(uint64_t to, const TcpConnPtr& conn) {
    std::unique_lock<jim::shared_mutex> locker(mu_);

    auto it = connections_.find(to);
    if (it != connections_.end() && it->second.get() == conn.get()) {
        connections_.erase(it);
    }
}

// TcpTransport Methods
//
TcpTransport::TcpTransport(const TransportOptions& ops) :
    resolver_(ops.resolver) {
    // new server
    net::ServerOptions sopt;
    sopt.io_threads_num = ops.recv_io_threads;
    server_.reset(new net::Server(sopt, "raft-srv"));

    // init connection pool
    CreateConnFunc create_func =
            [this](uint64_t to, TcpConnPtr& conn) { return newConnection(to, conn); };
    for (size_t i = 0; i < ops.connection_pool_size; ++i) {
        std::unique_ptr<ConnectionGroup> group(new ConnectionGroup(create_func));
        conn_pool_.push_back(std::move(group));
    }

    // init io thread
    client_.reset(new net::IOContextPool(ops.send_io_threads, "raft-cli"));
}

TcpTransport::~TcpTransport() {
    Shutdown();
}

void TcpTransport::onMessage(const net::Context& ctx, const net::MessagePtr& msg) {
    MessagePtr raft_msg(new pb::Message);
    auto data = msg->body.data();
    auto len = static_cast<int>(msg->body.size());
    if (raft_msg->ParseFromArray(data, len)) {
        FLOG_DEBUG("recv {} message from {}, from node_id: {} to node_id {}",
                pb::MessageType_Name(raft_msg->type()), ctx.remote_addr,
                   raft_msg->from(), raft_msg->to());

        handler_(raft_msg);
    } else {
        FLOG_ERROR("parse raft message failed from {}", ctx.remote_addr);
    }
}

Status TcpTransport::Start(const std::string& listen_ip, uint16_t listen_port, const MessageHandler& handler) {
    handler_ = handler;
    // start server
    auto ret = server_->ListenAndServe(listen_ip, listen_port,
                                   [this](const net::Context& ctx, const net::MessagePtr& msg) {
                                       onMessage(ctx, msg);
                                   });
    if (!ret.ok()) {
        return ret;
    }

    // start client threads
    client_->Start();

    return Status::OK();
}

void TcpTransport::Shutdown() {
    if (server_) {
        server_->Stop();
        server_.reset();
    }
    if (client_) {
        client_->Stop();
        client_.reset();
    }
}

void TcpTransport::SendMessage(MessagePtr& msg) {
    auto& group = conn_pool_[hash_func_(msg->id()) % conn_pool_.size()];
    auto conn = group->Get(msg->to());
    if (!conn) {
        FLOG_ERROR("could not get a connection to {}", msg->to());
        return;
    }
    auto ret = conn->Send(msg);
    if (!ret.ok()) {
        FLOG_ERROR("send to {} error: {}", msg->to(), ret.ToString());
        group->Remove(msg->to(), conn);
    }
}

Status TcpTransport::newConnection(uint64_t to, TcpConnPtr& conn) {
    // resolve
    auto addr = resolver_->GetNodeAddress(to);
    if (addr.empty()) {
        FLOG_ERROR("could not resolve address of {}", to);
        return Status(Status::kInvalidArgument, "resolve node address", std::to_string(to));
    } else {
        FLOG_INFO("resolve address of {} is {}", to, addr);
    }

    // split ip & port
    std::string ip, port;
    auto pos = addr.find(':');
    if (pos != std::string::npos) {
        ip = addr.substr(0, pos);
        port = addr.substr(pos + 1);
    } else {
        FLOG_ERROR("invalid address of {}: {}", to, addr);
        return Status(Status::kInvalidArgument, "invalid node address", std::to_string(to) + ", addr=" + addr);
    }

    // create new session
    auto null_handler = [](const net::Context&, const net::MessagePtr&) {};
    auto session = std::make_shared<net::Session>(client_opt_, null_handler, client_->GetIOContext());
    session->Connect(ip, port);
    conn = std::make_shared<TcpConnection>(session);
    FLOG_INFO("new connection to {}-{}", to, addr);
    return Status::OK();
}

Status TcpTransport::GetConnection(uint64_t to, std::shared_ptr<Connection>* conn) {
    TcpConnPtr tcp_conn;
    auto s = newConnection(to, tcp_conn);
    if (!s.ok()) {
        return s;
    }
    *conn = tcp_conn;
    return Status::OK();
}

} /* namespace transport */
} /* namespace impl */
} /* namespace raft */
} /* namespace jim */
