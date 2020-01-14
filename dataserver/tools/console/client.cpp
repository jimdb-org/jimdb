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

#include "client.h"
#include "dspb/function.pb.h"

namespace jim {
namespace ds {
namespace tool {

static const size_t kAdminRequestTimeoutSecs = 10;

std::pair<uint64_t, AdminClient::RespFuture> AdminClient::RequestQueue::add() {
    std::unique_lock<std::mutex> lock(mu_);
    auto seq = ++seq_;
    return { seq, que_[seq].get_future() };
}

void AdminClient::RequestQueue::set(uint64_t seq, dspb::AdminResponse&& resp) {
    std::unique_lock<std::mutex> lock(mu_);
    auto it = que_.find(seq);
    if (it != que_.end()) {
        it->second.set_value(std::move(resp));
        que_.erase(it);
    }
}

void AdminClient::RequestQueue::remove(uint64_t seq) {
    std::unique_lock<std::mutex> lock(mu_);
    que_.erase(seq);
}

AdminClient::AdminClient(const std::string& ip, const std::string& port) :
    ip_(ip),
    port_(port),
    pool_(1, "net") {
    pool_.Start();
}

AdminClient::~AdminClient() {
    auto sp_session = session_.lock();
    if (sp_session) {
        sp_session->Close();
    }
    pool_.Stop();
}

Status AdminClient::Admin(const dspb::AdminRequest& req, dspb::AdminResponse& resp) {
    uint64_t seq = 0;
    RespFuture future;
    std::tie(seq, future) = request_queue_.add();

    auto msg = net::NewMessage();
    msg->head.msg_type = net::kAdminRequestType;
    msg->head.msg_id = seq;
    msg->head.func_id = dspb::FunctionID::kFuncAdmin;
    msg->body.resize(req.ByteSizeLong());
    req.SerializeToArray(msg->body.data(), msg->body.size());

    getConn()->Write(msg);

    auto ws = future.wait_for(std::chrono::seconds(kAdminRequestTimeoutSecs));
    if (ws != std::future_status::ready) {
        request_queue_.remove(seq);
        return Status(Status::kTimedOut);
    }
    resp = future.get();
    // check response error
    if (resp.code() != 0) {
        return Status(Status::kUnknown, "server return code=" + std::to_string(resp.code()),
                resp.error_msg());
    }
    return Status::OK();
}

void AdminClient::onMessage(const net::Context& ctx, const net::MessagePtr& msg) {
    dspb::AdminResponse resp;
    if (!resp.ParseFromArray(msg->body.data(), msg->body.size())) {
        std::cerr<< "parse server response failed: " << "parse protobuf" << std::endl;
        return;
    }
    request_queue_.set(msg->head.msg_id, std::move(resp));
}

std::shared_ptr<net::Session> AdminClient::getConn() {
    std::lock_guard<std::mutex> lock(session_lock_);
    auto sp_session = session_.lock();
    if (sp_session) {
        return sp_session;
    }
    auto handle = [this](const net::Context& ctx, const net::MessagePtr& msg) {
        this->onMessage(ctx, msg);
    };
    sp_session = std::make_shared<net::Session>(net::SessionOptions{}, handle, pool_.GetIOContext());
    sp_session ->Connect(ip_, port_);
    session_ = sp_session;
    return sp_session;
}

} // namespace jim
} // namespace tool
} // namespace ds
