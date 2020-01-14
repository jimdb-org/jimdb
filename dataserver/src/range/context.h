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

#include <memory>

#include "base/status.h"
#include "base/timer.h"
#include "stats.h"
#include "server/context_server.h"
#include "db/db_manager.h"
#include "dspb/raft_internal.pb.h"

namespace jim {

namespace raft { class RaftServer; }

namespace ds {

namespace master { class MasterClient; }
namespace storage { class MetaStore; }

namespace range {

class Range;

// RangeContext is all other modules range depends on
class RangeContext {
public:
    RangeContext() = default;
    virtual ~RangeContext() = default;

    virtual uint64_t GetNodeID() const = 0;

    virtual db::DBManager* DBManager() = 0;
    virtual master::MasterClient* MasterClient() = 0;
    virtual raft::RaftServer* RaftServer() = 0;
    virtual storage::MetaStore* MetaStore() = 0;
    virtual RangeStats* Statistics() = 0;
    virtual TimerQueue* GetTimerQueue() = 0;

    // db usage percent for check writable
    virtual uint64_t GetDBUsagePercent() const = 0;

    virtual void ScheduleCheckSize(uint64_t range_id) = 0;

    // range manage
    virtual std::shared_ptr<Range> FindRange(uint64_t range_id) = 0;

    // split
    virtual Status SplitRange(uint64_t range_id, const dspb::SplitCommand &req, uint64_t raft_index) = 0;
};

} // namespace range
} // namespace ds
} // namespace jim
