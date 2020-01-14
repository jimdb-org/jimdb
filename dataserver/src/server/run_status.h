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
#include <mutex>
#include <string>
#include <thread>
#include <set>

#include "common/statistics.h"
#include "range/stats.h"

#include "context_server.h"

namespace jim {
namespace ds {
namespace server {

// RunStatus server run status, such as Statistics, db usage
class RunStatus : public range::RangeStats {
public:
    RunStatus() = default;
    virtual ~RunStatus() = default;

    RunStatus(const RunStatus &) = delete;
    RunStatus &operator=(const RunStatus &) = delete;

    void PushTime(HistogramType type, int64_t time) override {
        if (time > 0) statistics_.PushTime(type, static_cast<uint64_t>(time));
    }

    Status GetDBUsage(db::DBManager* db, db::DBUsage& db_usage);
    void UpdateDBUsage(db::DBManager* db);
    uint64_t GetDBUsedPercent() const { return db_usage_percent_.load();}

    void ReportLeader(uint64_t range_id, bool is_leader) override;
    uint64_t GetLeaderCount() const;

    void IncrSplitCount() override { ++split_count_; }
    void DecrSplitCount() override { --split_count_; }
    uint64_t GetSplitCount() const { return split_count_; }

    void PrintStatistics();

private:
    // server method performance
    Statistics statistics_;

    // db(disk or memory usage), upadte by server periodically
    std::atomic<uint64_t> db_usage_percent_ = {0};

    std::atomic<uint64_t> split_count_ = {0};

    // leader's range ids
    std::set<uint64_t> leaders_;
    mutable std::mutex leaders_mu_;
};

}  // namespace server
}  // namespace ds
}  // namespace jim
