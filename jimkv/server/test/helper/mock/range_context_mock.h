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

#include <atomic>
#include <mutex>
#include <rocksdb/db.h>

#include "range/context.h"
#include "raft/server.h"
#include "master/client.h"
#include "db/db.h"

using namespace jimkv::ds;
using namespace jimkv::ds::range;

namespace jimkv {
namespace test {
namespace mock {

class RangeContextMock: public RangeContext {
public:
    Status Init();
    void Destroy();

    uint64_t GetNodeID() const override { return 1; }

    db::DBManager* DBManager() override { return db_manager_.get(); }
    master::MasterClient* MasterClient() override { return master_client_.get(); }
    raft::RaftServer* RaftServer() override { return raft_server_.get(); }
    storage::MetaStore* MetaStore() override { return meta_store_.get(); }
    RangeStats* Statistics() override { return range_stats_.get(); }
    TimerQueue* GetTimerQueue() override { return &timer_queue_; };

    void SetDBUsagePercent(uint64_t value) { db_usage_percent_ = value; }
    uint64_t GetDBUsagePercent() const override { return db_usage_percent_.load(); }

    void ScheduleCheckSize(uint64_t range_id) override;

    Status CreateRange(const basepb::Range& meta, uint64_t leader = 0,
            uint64_t index = 0, std::shared_ptr<Range> *result = nullptr);
    std::shared_ptr<Range> FindRange(uint64_t range_id) override;
    Status SplitRange(uint64_t range_id, const dspb::SplitCommand &req, uint64_t raft_index) override;

private:
    std::string path_;
    std::unique_ptr<ds::db::DBManager> db_manager_ = nullptr;
    std::vector<rocksdb::ColumnFamilyHandle*> cf_handles_;
    std::unique_ptr<storage::MetaStore> meta_store_;
    std::unique_ptr<master::MasterClient> master_client_;
    std::unique_ptr<raft::RaftServer> raft_server_;
    std::unique_ptr<RangeStats> range_stats_;

    std::atomic<uint64_t> db_usage_percent_ = {0};

    std::map<uint64_t, std::shared_ptr<Range>> ranges_;
    std::mutex mu_;

    TimerQueue timer_queue_;
};

}
}
}
