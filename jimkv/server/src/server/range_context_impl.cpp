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

#include "common/server_config.h"
#include "range_context_impl.h"
#include "range_server.h"

namespace jimkv {
namespace ds {
namespace server {

RangeContextImpl::RangeContextImpl(ContextServer *s, TimerQueue* timer_queue) :
    server_(s),
    timer_queue_(timer_queue) {
}

uint64_t RangeContextImpl::GetDBUsagePercent() const {
    return server_->run_status->GetDBUsedPercent();
}

void RangeContextImpl::ScheduleCheckSize(uint64_t range_id) {
    server_->range_server->StatisPush(range_id);
}

std::shared_ptr<range::Range> RangeContextImpl::FindRange(uint64_t range_id) {
    return server_->range_server->Find(range_id);
}

// split
Status RangeContextImpl::SplitRange(uint64_t range_id, const dspb::SplitCommand &req,
                  uint64_t raft_index) {
    return server_->range_server->SplitRange(range_id, req, raft_index);
}

}  // namespace server
}  // namespace ds
}  // namespace jimkv
