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

#include <thread>
#include <mutex>
#include <condition_variable>

#include "common/masstree_options.h"
#include "db/db_manager.h"

namespace jim {
namespace ds {
namespace db {

class MasstreeWrapper;
class CheckpointSchedular;

class MasstreeDBManager: public DBManager {
public:
    explicit MasstreeDBManager(const MasstreeOptions& opt);
    ~MasstreeDBManager();

    MasstreeDBManager(const MasstreeDBManager&) = delete;
    MasstreeDBManager& operator=(const MasstreeDBManager&) = delete;

    Status Init() override;

    Status CreateDB(uint64_t range_id, const std::string& start_key,
            const std::string& end_key, std::unique_ptr<DB>& db) override;

    Status GetUsage(DBUsage& usage) override;

    std::string MetricInfo(bool verbose) override;

    CheckpointSchedular* GetSchedular() const { return checkpoint_schedular_; }

    std::string GetDBPath(uint64_t range_id) const;

    Status CreatSplit(uint64_t range_id, const std::string& start_key,
            const std::string& end_key, std::unique_ptr<DB>& db);

    void RCUFree(bool wait);

    Status DumpTree(const std::string& dest_dir = "/tmp");

private:
    Status createDB(uint64_t range_id, const std::string& start_key,
                    const std::string& end_key, bool from_split,
                    std::unique_ptr<DB>& db);

private:
    const MasstreeOptions opt_;

    MasstreeWrapper *default_tree_ = nullptr;
    MasstreeWrapper *txn_tree_ = nullptr;

    CheckpointSchedular* checkpoint_schedular_ = nullptr;
};

} // namespace db
} // namespace ds
} // namespace jim
