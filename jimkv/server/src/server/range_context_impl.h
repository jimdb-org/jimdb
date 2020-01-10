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

_Pragma("once");

#include "range/context.h"
#include "context_server.h"
#include "run_status.h"
#include "range_server.h"

namespace jimkv {
namespace ds {
namespace server {

class RangeContextImpl : public range::RangeContext {
public:
    RangeContextImpl(ContextServer *s, TimerQueue* timer_queue);

    uint64_t GetNodeID() const override { return server_->node_id; }

    db::DBManager* DBManager() override { return server_->db_manager; }
    master::MasterClient* MasterClient() override  { return server_->master_client; }
    raft::RaftServer* RaftServer() override { return server_->raft_server; }
    storage::MetaStore* MetaStore() override { return server_->meta_store; }
    range::RangeStats* Statistics() override { return server_->run_status; }
    TimerQueue* GetTimerQueue() override { return timer_queue_; }

    uint64_t GetDBUsagePercent() const override;

    void ScheduleCheckSize(uint64_t range_id) override;

    // range manage
    std::shared_ptr<range::Range> FindRange(uint64_t range_id) override;

    // split
    Status SplitRange(uint64_t range_id, const dspb::SplitCommand& req, uint64_t raft_index) override;

private:
    ContextServer* server_ = nullptr;
    TimerQueue* timer_queue_ = nullptr;
};

}  // namespace server
}  // namespace ds
}  // namespace jimkv
