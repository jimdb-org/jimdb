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

#include "run_status.h"

#include <chrono>
#include <thread>

#include "base/util.h"
#include "common/server_config.h"
#include "common/logger.h"
#include "db/db_manager.h"

#include "server.h"
#include "worker.h"

namespace jim {
namespace ds {
namespace server {

Status RunStatus::GetDBUsage(db::DBManager* db, db::DBUsage& db_usage) {
    return db->GetUsage(db_usage);
}

void RunStatus::UpdateDBUsage(db::DBManager* db) {
    if (db) {
        db::DBUsage usage;
        auto s = db->GetUsage(usage);
        if (s.ok()) {
            db_usage_percent_ = usage.used_size * 100/ usage.total_size;
        } else {
            FLOG_ERROR("collect db usage failed: {}", s.ToString());
        }
    }
}

void RunStatus::ReportLeader(uint64_t range_id, bool is_leader) {
    std::lock_guard<std::mutex> lock(leaders_mu_);
    if (is_leader) {
        leaders_.insert(range_id);
    } else {
        leaders_.erase(range_id);
    }
}

uint64_t RunStatus::GetLeaderCount() const {
    std::lock_guard<std::mutex> lock(leaders_mu_);
    return leaders_.size();
}

void RunStatus::PrintStatistics() {
    FLOG_INFO("\n{}", statistics_.ToString());
    statistics_.Reset();
}

}  // namespace server
}  // namespace ds
}  // namespace jim
