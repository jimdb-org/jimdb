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

#include <memory>
#include <atomic>

#include "base/status.h"
#include "common/masstree_options.h"
#include "db/db.h"

namespace jimkv {
namespace ds {
namespace db {

class MasstreeWrapper;
class MasstreeDBManager;
class CheckpointManager;
class WALManager;
class MassTreeBatch;

struct MasstreeDBOptions {
    uint64_t id = 0;
    std::string db_path;
    std::string start_key;
    std::string end_key;
    bool wal_disabled = false;
    bool replay = true;

    CheckpointOptions checkpoint_opt;
};

class MasstreeDBImpl : public DB {
public:
    MasstreeDBImpl(const MasstreeDBOptions& ops, MasstreeDBManager* manager,
            MasstreeWrapper* data_tree, MasstreeWrapper* txn_tree);
    ~MasstreeDBImpl();

    Status Open() override;
    Status Close() override;
    Status Destroy() override;

    const std::string& GetStartKey() const { return start_key_; }
    const std::string& GetEndKey() const { return end_key_; } // for test only, not thread safety

    uint64_t PersistApplied() override;

    Status Get(CFType cf, const std::string& key, std::string& value) override;
    Status Put(CFType cf, const std::string& key, const std::string& value, uint64_t raft_index) override;
    Status Delete(CFType cf, const std::string& key, uint64_t raft_index) override;
    Status DeleteRange(CFType cf, const std::string& start, const std::string& limit, uint64_t raft_index) override;

    WriteBatchPtr NewWriteBatch() override;
    Status Write(WriteBatch* batch, uint64_t raft_index) override;

    Status ApplySnapshotStart(uint64_t raft_index) override;
    Status ApplySnapshotData(WriteBatch *batch) override;
    Status ApplySnapshotFinish(uint64_t raft_index) override;

    IteratorPtr NewIterator(const std::string& start, const std::string& limit) override;

    Status NewIterators(const std::string& start, const std::string& limit,
            IteratorPtr &data_iter, IteratorPtr &txn_iter) override;

    Status SplitDB(uint64_t split_range_id, const std::string& split_key,
            uint64_t raft_index, std::unique_ptr<DB>& split_db) override;

    Status ApplySplit(const std::string& split_key, uint64_t raft_index) override;

    void TEST_Enable_Checkpoint() { ckp_opt_.disabled = false; }
    void TEST_Disable_Checkpoint() { ckp_opt_.disabled = true; }
    Status TEST_Run_Checkpoint();

private:
    MasstreeWrapper* getTree(CFType cf);
    bool keyInRange(const std::string& key) const;
    Status writeTree(const MassTreeBatch& batch, bool check_in_range = false);

    Status loadCheckpoint(bool replay = true);
    Status loadWAL(uint64_t index, bool replay = true);

    Status writeBatch(MassTreeBatch *batch);

    void truncateTree(const std::string& tree_name, MasstreeWrapper *tree);
    Status doCheckpoint(uint64_t applied, bool sync = false);
    void checkDirty();

private:
    const uint64_t id_;
    const bool wal_disabled_ = false;
    const bool replay_ = true;
    const std::string start_key_;
    std::string end_key_;
    CheckpointOptions ckp_opt_;

    MasstreeDBManager* manager_ = nullptr;
    MasstreeWrapper *data_tree_ = nullptr;
    MasstreeWrapper *txn_tree_ = nullptr;

    std::atomic<uint64_t> applied_index_ = {0};
    CheckpointManager* checkpoint_ = nullptr;
    WALManager *wal_ = nullptr;
    uint64_t dirty_bytes_ = 0;
};

} // namespace db
} // namespace ds
} // namespace jimkv
