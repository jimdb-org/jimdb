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

#include "inprocess_transport.h"

namespace jim {
namespace raft {
namespace impl {
namespace transport {

InProcessTransport::MailBox::MailBox() : running_(true) {}

InProcessTransport::MailBox::~MailBox() { close(); }

void InProcessTransport::MailBox::send(const MessagePtr& msg) {
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (!running_) return;
        msgs_.push(msg);
    }
    cv_.notify_one();
}

bool InProcessTransport::MailBox::recv(MessagePtr* msg) {
    std::unique_lock<std::mutex> lock(mu_);
    while (msgs_.empty() && running_) {
        cv_.wait(lock);
    }
    if (!running_) return false;
    *msg = msgs_.front();
    msgs_.pop();
    return true;
}

void InProcessTransport::MailBox::close() {
    std::lock_guard<std::mutex> lock(mu_);
    running_ = false;
    cv_.notify_all();
}

InProcessTransport::MsgHub::MsgHub() {}

InProcessTransport::MsgHub::~MsgHub() {}

std::shared_ptr<InProcessTransport::MailBox> InProcessTransport::MsgHub::regist(
    uint64_t node_id) {
    std::unique_lock<jim::shared_mutex> lock(mu_);

    auto it = mail_boxes_.find(node_id);
    if (it != mail_boxes_.end()) {
        return it->second;
    } else {
        auto box = std::make_shared<MailBox>();
        mail_boxes_.emplace(node_id, box);
        return box;
    }
}

void InProcessTransport::MsgHub::unregister(uint64_t node_id) {
    std::unique_lock<jim::shared_mutex> lock(mu_);
    mail_boxes_.erase(node_id);
}

void InProcessTransport::MsgHub::send(const MessagePtr& msg) {
    std::shared_ptr<MailBox> box;
    jim::shared_lock<jim::shared_mutex> lock(mu_);
    auto it = mail_boxes_.find(msg->to());
    if (it != mail_boxes_.end()) {
        it->second->send(msg);
    }
}

InProcessTransport::MsgHub InProcessTransport::msg_hub_;

InProcessTransport::InProcessTransport(uint64_t node_id)
    : node_id_(node_id), running_(false) {
    assert(node_id_ != 0);
}

InProcessTransport::~InProcessTransport() { Shutdown(); }

Status InProcessTransport::Start(const std::string& listen_ip,
                                 uint16_t listen_port,
                                 const MessageHandler& handler) {
    handler_ = handler;
    running_ = true;
    mail_box_ = msg_hub_.regist(node_id_);
    assert(mail_box_ != nullptr);
    pull_thr_.reset(
        new std::thread(std::bind(&InProcessTransport::recvRoutine, this)));
    return Status::OK();
}

void InProcessTransport::Shutdown() {
    running_ = false;
    msg_hub_.unregister(node_id_);
    mail_box_->close();
    if (pull_thr_ && pull_thr_->joinable()) {
        pull_thr_->join();
        pull_thr_.reset();
    }
}

void InProcessTransport::SendMessage(MessagePtr& msg) { msg_hub_.send(msg); }

Status InProcessTransport::GetConnection(uint64_t to,
                                         std::shared_ptr<Connection>* conn) {
    auto c = std::make_shared<InProcessConn>(this);
    *conn = std::static_pointer_cast<Connection>(c);
    return Status::OK();
}

void InProcessTransport::recvRoutine() {
    MessagePtr msg;
    while (mail_box_->recv(&msg)) {
        handler_(msg);
    }
}

} /* namespace transport */
} /* namespace impl */
} /* namespace raft */
} /* namespace jim */
