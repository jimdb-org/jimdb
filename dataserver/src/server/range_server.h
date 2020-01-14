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

_Pragma("once");

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include "base/shared_mutex.h"
#include "base/status.h"
#include "base/timer.h"

#include "common/rpc_request.h"
#include "mspb/mspb.pb.h"
#include "dspb/raft_internal.pb.h"
#include "dspb/schedule.pb.h"

#include "storage/meta_store.h"

#include "server/context_server.h"
#include "db/db_manager.h"

#include "range_tree.h"

namespace jim {
namespace ds {
namespace server {

using ErrorPtr = std::unique_ptr<dspb::Error>;

class RangeServer final {
public:
    RangeServer() = default;
    ~RangeServer() = default;

    RangeServer(const RangeServer &) = delete;
    RangeServer &operator=(const RangeServer &) = delete;

    int Init(ContextServer *context);
    int Start();
    void Stop();
    void Clear();

    void DealTask(RPCRequestPtr rpc);
    void StatisPush(uint64_t range_id);

    size_t GetRangesCount() const;
    std::shared_ptr<range::Range> Find(uint64_t range_id);

    Status SplitRange(uint64_t old_range_id, const dspb::SplitCommand& req,
            uint64_t raft_index);

private:  // admin
    int openDB();
    void closeDB();

    Status recover(const basepb::Range& meta);
    int recover(const std::vector<basepb::Range> &metas);
    Status createTestRange(ContextServer *context);

    void forwardToRange(RPCRequestPtr& rpc);
    void dispatchSchedule(RPCRequestPtr& rpc);

    Status createRange(const basepb::Range &range, uint64_t leader, std::shared_ptr<range::Range>& existed);
    ErrorPtr createRange(const dspb::CreateRangeRequest& req, dspb::CreateRangeResponse* resp);

    Status deleteRange(uint64_t range_id, uint64_t peer_id = 0);
    ErrorPtr deleteRange(const dspb::DeleteRangeRequest& req, dspb::DeleteRangeResponse* resp);

    ErrorPtr transferLeader(const dspb::TransferRangeLeaderRequest& req, dspb::TransferRangeLeaderResponse* resp);
    ErrorPtr getPeerInfo(const dspb::GetPeerInfoRequest& req, dspb::GetPeerInfoResponse* resp);
    ErrorPtr isAlive(const dspb::IsAliveRequest& req, dspb::IsAliveResponse* resp);
    ErrorPtr nodeInfo(const dspb::NodeInfoRequest& req, dspb::NodeInfoResponse* resp);
    ErrorPtr changeRaftMember(const dspb::ChangeRaftMemberRequest& req, dspb::ChangeRaftMemberResponse* resp);

private:
    ContextServer *context_ = nullptr;

    bool running_ = true;
    db::DBManager *db_manager_ = nullptr;
    storage::MetaStore *meta_store_ = nullptr;

    RangeTree ranges_;
    std::unique_ptr<range::RangeContext> range_context_;

    std::mutex statis_mutex_;
    std::condition_variable statis_cond_;
    std::queue<uint64_t> statis_queue_;
    std::vector<std::thread> worker_;

    std::unique_ptr<TimerQueue> timer_queue_;
};

}  // namespace server
}  // namespace ds
}  // namespace jim
