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
#include <vector>

#include "base/status.h"
#include "base/system_info.h"
#include "common/server_config.h"
#include "db/db.h"

namespace jim {
namespace ds {
namespace db {

struct DBUsage {
    uint64_t total_size = 0;
    uint64_t used_size = 0;
    uint64_t free_size = 0;

    std::string ToString() const;
};

class DBManager {
public:
    DBManager() = default;
    virtual ~DBManager() = default;

    DBManager(const DBManager&) = default;
    DBManager& operator=(const DBManager&) = default;

    virtual Status Init() = 0;

    virtual Status CreateDB(uint64_t range_id, const std::string& start_key,
            const std::string& end_key, std::unique_ptr<DB>& db) = 0;

    virtual Status GetUsage(DBUsage& usage) = 0;

    virtual std::string MetricInfo(bool verbose) = 0;
};

Status NewDBManager(std::unique_ptr<DBManager>& db_manager);

} // namespace db
} // namespace ds
} // namespace jim
