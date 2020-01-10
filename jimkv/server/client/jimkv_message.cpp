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


#include "jimkv_message.h"
#include "jimkv_log.h"
#include "jimkv_uuid.h"

namespace jimkv {
namespace sdk {

void JimMessage::AddSubMsg(MsgPtr sub_msg) {
    sub_msg->parent_set(shared_from_this());

    std::lock_guard<std::mutex> lock(sub_msg_mutex_);
    sub_msg_[sub_msg->msg_id()] = sub_msg;
}

bool JimMessage::Serialize(RangePtr rng) {
    msg_id_ = JimUuid::Instance().id_generate();

    msg_.head.msg_type = net::kDataRequestType;
    msg_.head.func_id = dspb::kFuncRangeRequest;

    dspb::RangeRequest req;
    auto head = req.mutable_header();
    head->set_cluster_id(cmd_->cluster_id());
    head->set_range_id(rng->id());
    head->mutable_range_epoch()->CopyFrom(rng->range_epoch());

    switch (cmd_->req_type()) {
    case dspb::RangeRequest::ReqCase::kKvGet:
        return KvGet(req);
    case dspb::RangeRequest::ReqCase::kKvPut:
        return KvPut(req);
    case dspb::RangeRequest::ReqCase::kKvDelete:
        return KvDelete(req);
    case dspb::RangeRequest::ReqCase::kKvScan:
        return KvScan(req);
    case dspb::RangeRequest::ReqCase::kKvDelRange:
        return KvDelRange(req);
    default:
        CBLOG_WARN("not support request type: %d", cmd_->req_type());
    }
    return false;
}

bool JimMessage::KvGet(dspb::RangeRequest& req) {
    req.mutable_kv_get()->set_key(cmd_->encode_key());
    auto len = req.ByteSizeLong();
    msg_.body.resize(len);
    if (!req.SerializeToArray(msg_.body.data(), len)) {
        return false;
    }

    msg_.head.msg_id = msg_id_;
    msg_.head.body_length = len;
    msg_.head.Encode();
    return true;
}

bool JimMessage::KvPut(dspb::RangeRequest& req) {
    req.mutable_kv_put()->set_key(cmd_->encode_key());
    req.mutable_kv_put()->set_value(cmd_->encode_value());
    auto len = req.ByteSizeLong();
    msg_.body.resize(len);
    if (!req.SerializeToArray(msg_.body.data(), len)) {
        return false;
    }

    msg_.head.msg_id = msg_id_;
    msg_.head.body_length = len;
    msg_.head.Encode();
    return true;
}

bool JimMessage::KvDelete(dspb::RangeRequest& req) {
    req.mutable_kv_delete()->set_key(cmd_->encode_key());
    auto len = req.ByteSizeLong();
    msg_.body.resize(len);
    if (!req.SerializeToArray(msg_.body.data(), len)) {
        return false;
    }

    msg_.head.msg_id = msg_id_;
    msg_.head.body_length = len;
    msg_.head.Encode();
    return true;
}

bool JimMessage::KvScan(dspb::RangeRequest& req) {
    req.mutable_kv_scan()->set_start_key(start_key_);
    req.mutable_kv_scan()->set_end_key(end_key_);
    auto len = req.ByteSizeLong();
    msg_.body.resize(len);
    if (!req.SerializeToArray(msg_.body.data(), len)) {
        return false;
    }

    msg_.head.msg_id = msg_id_;
    msg_.head.body_length = len;
    msg_.head.Encode();
    return true;
}

bool JimMessage::KvDelRange(dspb::RangeRequest& req) {
    req.mutable_kv_del_range()->set_start_key(start_key_);
    req.mutable_kv_del_range()->set_end_key(end_key_);
    auto len = req.ByteSizeLong();
    msg_.body.resize(len);
    if (!req.SerializeToArray(msg_.body.data(), len)) {
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
} // namespace jimkv

