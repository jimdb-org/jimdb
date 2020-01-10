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

#include "manager_impl.h"

#include <sstream>

#include "base/util.h"
#include "base/fs_util.h"
#include "common/logger.h"
#include "common/masstree_env.h"

#include "checkpoint.h"
#include "mass_tree_wrapper.h"

#include "db_impl.h"

namespace jimkv {
namespace ds {
namespace db {

MasstreeDBManager::MasstreeDBManager(const MasstreeOptions& opt) :
    opt_(opt),
    default_tree_(new MasstreeWrapper),
    txn_tree_(new MasstreeWrapper) {
    CheckpointSchedular::Options schedular_opt;
    schedular_opt.num_threads = opt.checkpoint_opt.work_threads;
    checkpoint_schedular_ = new CheckpointSchedular(schedular_opt);
}

MasstreeDBManager::~MasstreeDBManager() {
    delete checkpoint_schedular_;
    delete default_tree_;
    delete txn_tree_;
}

Status MasstreeDBManager::Init() {
    FLOG_INFO("masstree configs: {}", opt_.ToString());

    if (!MakeDirAll(opt_.data_path, 0755)) {
        return Status(Status::kIOError, "create masstree db path", strErrno(errno));
    }

    return Status::OK();
}

Status MasstreeDBManager::createDB(uint64_t range_id, const std::string& start_key,
                const std::string& end_key, bool from_split,
                std::unique_ptr<DB>& db) {
    MasstreeDBOptions db_opt;
    db_opt.id = range_id;
    db_opt.start_key = start_key;
    db_opt.end_key = end_key;
    db_opt.db_path = GetDBPath(range_id);
    db_opt.wal_disabled = opt_.wal_disabled;
    db_opt.replay = !from_split;
    db_opt.checkpoint_opt = opt_.checkpoint_opt;

    db.reset(new MasstreeDBImpl(db_opt, this, default_tree_, txn_tree_));

    return db->Open();
}

Status MasstreeDBManager::CreateDB(uint64_t range_id, const std::string& start_key,
              const std::string& end_key, std::unique_ptr<DB>& db) {
    return createDB(range_id, start_key, end_key, false, db);
}

Status MasstreeDBManager::CreatSplit(uint64_t range_id, const std::string& start_key,
                  const std::string& end_key, std::unique_ptr<DB>& db) {
    return createDB(range_id, start_key, end_key, true, db);
}

Status MasstreeDBManager::GetUsage(DBUsage& usage) {
    // TODO: collect disk usage
    uint64_t total = 0, available = 0;
    auto ret = GetMemoryUsage(&total, &available);
    if (!ret) {
        return Status(Status::kIOError, "collect memory usage", strErrno(errno));
    }

    usage.total_size = total;
    usage.free_size = available;
    usage.used_size = total - available;
    if (total > 0 && available <= total) {
        return Status::OK();
    } else {
        return Status(Status::kInvalid, "db usage", usage.ToString());
    }
}

std::string MasstreeDBManager::MetricInfo(bool verbose) {
    auto alloc_size = ThreadCounter(tc_alloc) + ThreadCounter(tc_alloc_value)
            + ThreadCounter(tc_alloc_other);

    std::ostringstream ss;
    ss << "Masstree thread counters: alloc=" << alloc_size;
    ss << ", gc=" << ThreadCounter(tc_gc);
    ss << ", leaf_split=" << ThreadCounter(tc_stable_leaf_split);
    ss << std::endl;

    ss << "Masstree default counters: " << default_tree_->CollectTreeCouter() << std::endl;
    ss << "Masstree txn counters: " << txn_tree_->CollectTreeCouter() << std::endl;

    if (verbose) {
        ss << "Masstree default stat: " << default_tree_->TreeStat() << std::endl;
        ss << "Masstree txn stat: " << txn_tree_->TreeStat() << std::endl;
    }

    return ss.str();
}

std::string MasstreeDBManager::GetDBPath(uint64_t range_id) const {
    return JoinFilePath({opt_.data_path, std::to_string(range_id)});
}

void MasstreeDBManager::RCUFree(bool wait) {
    RunRCUFree(wait);
}

Status MasstreeDBManager::DumpTree(const std::string& dest_dir) {
    auto subdir = JoinFilePath({dest_dir, "masstree-dump." + std::to_string(time(NULL))});
    if (!MakeDirAll(subdir, 0755)) {
        return Status(Status::kIOError, "create dump dir", strErrno(errno));
    }

    auto default_file = JoinFilePath({subdir, "default"});
    auto s = default_tree_->Dump(default_file);
    if (!s.ok()) {
        return s;
    }

    auto txn_file = JoinFilePath({subdir, "txn"});
    return txn_tree_->Dump(txn_file);
}

} // namespace db
} // namespace ds
} // namespace jimkv
