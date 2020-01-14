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


#include "jim_message.h"
#include "jim_log.h"
#include "jim_uuid.h"

namespace jim {
namespace sdk {

void JimMessage::AddSubMsg(MsgPtr sub_msg) {
    sub_msg->parent_set(shared_from_this());

    std::lock_guard<std::mutex> lock(sub_msg_mutex_);
    sub_msg_[sub_msg->msg_id()] = sub_msg;
}

bool JimMessage::Serialize(dspb::RangeRequest &req) {
    msg_id_ = JimUuid::Instance().id_generate();

    msg_.head.msg_type = net::kDataRequestType;
    msg_.head.func_id = dspb::kFuncRangeRequest;

    auto len = req.ByteSizeLong();
    msg_.body.resize(len);
    if (!req.SerializeToArray(msg_.body.data(), len)) {
        CBLOG_ERROR("range request serialize fail.");
        return false;
    }

    msg_.head.msg_id = msg_id_;
    msg_.head.body_length = len;
    msg_.head.Encode();

    return true;
}

void JimMessage::SendEnd() {
    state_ = State::WAIT;

    if (sub_msg_completed_ == sub_msg_.size()) {
        sub_msg_.clear();
        cmd_->net_callback();
    }
}

void JimMessage::Complete() {
    if (has_parent()) {
        parent_->do_complete();
    } else {
        do_complete();
    }
}

void JimMessage::Timeout() {
    CBLOG_WARN("Timeout msg id %" PRIu64, msg_id_);
    TimerCancel();
    Complete();
}

void JimMessage::TimeoutHandle(std::function<int()> pred, const asio::error_code& e) {
    if (pred() != 0 && e != asio::error::operation_aborted) {
        CBLOG_WARN("TimeoutHandle msg id %" PRIu64, msg_id_);
        TimerCancel();
        Complete();
    }
}

void JimMessage::do_complete() {
    if (sub_msg_.empty()) {
        return cmd_->net_callback();
    }

    auto old = State::WAIT;

    auto count = sub_msg_completed_.fetch_add(1, std::memory_order_relaxed);
    bool is_wait = state_.compare_exchange_strong(old, State::END);

    if ((count + 1 == sub_msg_.size()) && is_wait) {
        sub_msg_.clear();
        cmd_->net_callback();
    }
}

} // namespace sdk
} // namespace jim

