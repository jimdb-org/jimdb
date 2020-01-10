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

#include "base/util.h"

#include "jimkv_assist.h"
#include "jimkv_client.h"
#include "jimkv_cluster.h"
#include "jimkv_log.h"

namespace jimkv {
namespace sdk {

void JimAssist::Start() {
    thread_ = std::thread([this] { this->runLoop(); });
    AnnotateThread(thread_.native_handle(), "assist");
}

void JimAssist::Stop() {
    running_ = false;
    cond_.notify_all();

    if (thread_.joinable()) {
        thread_.join();
    }
}

void JimAssist::AddReSendMsg(MsgPtr msg) {
    std::lock_guard<std::mutex> lock(mutex_);
    resend_msg_.push(msg);
    cond_.notify_one();
}

MsgPtr JimAssist::GetMsg() {
    std::unique_lock<std::mutex> lock(mutex_);
    while (running_) {
        if (!resend_msg_.empty()) {
            auto msg = resend_msg_.front();
            resend_msg_.pop();
            return msg;
        }

        std::chrono::milliseconds timeout(30);
        cond_.wait_for(lock, timeout);
    }
    return nullptr;
}

void JimAssist::ReSendMsg() {
    auto msg = GetMsg();
    auto cluster_id = msg->cmd()->cluster_id();

    auto cluster = JimClient::Instance().GetCluster(cluster_id);
    if (cluster == nullptr) {
        CBLOG_WARN("cluster %" PRIu64 " not found.", cluster_id);

        return msg->Complete();
    }

    cluster->ReSendMsg(msg);
}

void JimAssist::runLoop() {
    while (running_) {
        ReSendMsg();
    }
}

} // namespace sdk
} // namespace jimkv
