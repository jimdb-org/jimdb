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

#include "submit.h"

#include "base/util.h"
#include "common/logger.h"

namespace jim {
namespace ds {
namespace range {

SubmitContext::SubmitContext(RPCRequestPtr rpc_request,
        dspb::CmdType type, dspb::RangeRequest_Header&& req_header) :
    submit_time_(NowMicros()),
    rpc_request_(std::move(rpc_request)),
    type_(type),
    req_header_(std::move(req_header)) {
}


void SubmitContext::SendResponse(dspb::RangeResponse& resp, ErrorPtr err) {
    SetResponseHeader(resp.mutable_header(), req_header_, std::move(err));
    rpc_request_->Reply(resp);
}

void SubmitContext::SendError(ErrorPtr err) {
    dspb::RangeResponse resp;
    SendResponse(resp, std::move(err));
}

void SubmitContext::CheckExecuteTime(uint64_t rangeID, int64_t thresold_usecs) {
    auto take = NowMicros() - rpc_request_->begin_time;
    if (take > thresold_usecs) {
        const auto& method = dspb::CmdType_Name(type_);
        auto msg_id = rpc_request_->msg->head.msg_id;
        FLOG_WARN("range[{}] {} takes too long({} ms), from={}, msgid={}",
                rangeID, method, take / 1000, rpc_request_->ctx.remote_addr, msg_id);
    }
}

uint64_t SubmitContext::MsgID() const {
    return rpc_request_->MsgID();
}


uint64_t SubmitQueue::GetSeq() {
    std::lock_guard<std::mutex> lock(mu_);
    return ++seq_;
}

uint64_t SubmitQueue::Add(RPCRequestPtr rpc_request, dspb::CmdType type,
                          dspb::RangeRequest_Header&& req_header) {
    auto expire_time = rpc_request->expire_time;
    SubmitContextPtr ctx(new SubmitContext(std::move(rpc_request), type, std::move(req_header)));
    std::lock_guard<std::mutex> lock(mu_);
    ctx_map_.emplace(++seq_, std::move(ctx));
    expire_que_.emplace(expire_time, seq_);
    return seq_;
}

std::unique_ptr<SubmitContext> SubmitQueue::Remove(uint64_t seq_id) {
    std::unique_ptr<SubmitContext> ret;
    std::lock_guard<std::mutex> lock(mu_);
    auto it = ctx_map_.find(seq_id);
    if (it != ctx_map_.end()) {
        ret = std::move(it->second);
        ctx_map_.erase(it);
    }
    return ret;
}

std::vector<std::unique_ptr<SubmitContext>> SubmitQueue::ClearAll() {
    std::vector<std::unique_ptr<SubmitContext>> result;
    std::lock_guard<std::mutex> lock(mu_);
    for (auto& kv: ctx_map_) {
        result.push_back(std::move(kv.second));
    }
    ctx_map_.clear();
    expire_que_.Clear();
    return result;
}

std::vector<uint64_t> SubmitQueue::GetExpired(size_t max_count) {
    std::vector<uint64_t> result;
    auto now = NowMilliSeconds();
    const size_t kMaxCheckCount = 100000;
    size_t check_count = 0;
    {
        std::lock_guard<std::mutex> lock(mu_);
        while (!expire_que_.empty() && result.size() < max_count && check_count < kMaxCheckCount) {
            ++check_count;
            auto& p = expire_que_.top();
            if (p.first < now) {
                auto it = ctx_map_.find(p.second);
                if (it != ctx_map_.end()) { // return not replied context only
                    result.push_back(p.second);
                }
                expire_que_.pop();
            } else {
                break;
            }
        }
    }
    return result;
}

size_t SubmitQueue::Size() const {
    std::lock_guard<std::mutex> lock(mu_);
    return ctx_map_.size();
}

void SubmitQueue::Shrink() {
    std::lock_guard<std::mutex> lock(mu_);
    auto &vec = expire_que_.GetContainer();
    if (vec.capacity() > vec.size() * 10) {
        vec.shrink_to_fit();
    }
}

}  // namespace range
}  // namespace ds
}  // namespace jim
