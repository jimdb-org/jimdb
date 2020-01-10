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

#include "db_impl.h"

#include "base/fs_util.h"
#include "common/logger.h"
#include "common/masstree_env.h"

#include "mass_tree_wrapper.h"
#include "checkpoint.h"
#include "wal.h"

#include "iterator_impl.h"
#include "manager_impl.h"
#include "write_batch_impl.h"

namespace jimkv {
namespace ds {
namespace db {

static const char* kCheckpointSubDir = "checkpoint";
static const char* kWALSubDir = "wal";

MasstreeDBImpl::MasstreeDBImpl(const MasstreeDBOptions& ops, MasstreeDBManager* manager,
        MasstreeWrapper* data_tree, MasstreeWrapper* txn_tree) :
    id_(ops.id),
    wal_disabled_(ops.wal_disabled),
    replay_(ops.replay),
    start_key_(ops.start_key),
    end_key_(ops.end_key),
    ckp_opt_(ops.checkpoint_opt),
    manager_(manager),
    data_tree_(data_tree),
    txn_tree_(txn_tree) {
    // create checkpoint manager
    std::string ckp_path = JoinFilePath({ops.db_path, kCheckpointSubDir});
    checkpoint_ = new CheckpointManager(ops.checkpoint_opt, ops.id,
                                        std::move(ckp_path), manager->GetSchedular());

    // create wal manager
    std::string wal_path = JoinFilePath({ops.db_path, kWALSubDir});
    WALOptions wal_opt;
    wal_ = new WALManager(wal_opt, wal_path);
}

MasstreeDBImpl::~MasstreeDBImpl() {
    this->Close();
    delete checkpoint_;
    delete wal_;
}

bool MasstreeDBImpl::keyInRange(const std::string& key) const {
    return key >= start_key_ && key < end_key_;
}

MasstreeWrapper* MasstreeDBImpl::getTree(CFType cf) {
    if (cf == CFType::kData) {
        return data_tree_;
    } else if (cf == CFType::kTxn) {
        return txn_tree_;
    }
    return nullptr;
}

Status MasstreeDBImpl::writeTree(const MassTreeBatch& batch, bool check_in_range) {
    Status s;
    for (const auto& ent: batch.Entries()) {
        if (check_in_range && !keyInRange(ent.key)) {
            continue;
        }
        switch (ent.type) {
        case KvEntryType::kPut:
            s = getTree(ent.cf)->Put(ent.key, ent.value);
            break;
        case KvEntryType::kDelete:
            s = getTree(ent.cf)->Delete(ent.key);
            break;
        }
        if (!s.ok()) {
            break;
        }
    }
    return s;
}


Status MasstreeDBImpl::loadCheckpoint(bool replay) {
    std::unique_ptr<CheckpointReader> recover_point;
    auto s = checkpoint_->Open(recover_point);
    if (!s.ok()) {
        return Status(Status::kIOError, "open checkpoint", s.ToString());
    }

    if (recover_point == nullptr) {
        return Status::OK();
    }

    applied_index_ = recover_point->AppliedIndex();
    bool over = false;
    while (!over && replay) {
        CFType cf;
        std::string key, value;
        s = recover_point->Next(cf, key, value, over);
        if (!s.ok()) {
            return Status(Status::kIOError, "recover read kv", s.ToString());
        }
        if (over)  {
            break;
        }
        if (!keyInRange(key)) {
            continue;
        }
        s = getTree(cf)->Put(key, value);
        if (!s.ok()) {
            return Status(Status::kIOError, "recover put kv", s.ToString());
        }
    }

    return Status::OK();
}

Status MasstreeDBImpl::loadWAL(uint64_t index, bool replay) {
    std::vector<std::string> wals;
    auto s = wal_->Open(index, wals);
    if (!s.ok()) {
        return s;
    }

    if (!replay) {
        return Status::OK();
    }

    // replay wal records
    for (const auto& filename : wals) {
        WALReader reader(filename);
        s = reader.Open();
        if (!s.ok()) {
            return s;
        }
        while (true) {
            MassTreeBatch batch;
            bool over = false;
            s = reader.Next(batch, over);
            if (!s.ok()) {
                return s;
            }
            if (over) {
                break;
            }
            if (batch.GetIndex() <= index) {
                continue;
            }
            s = writeTree(batch, true);
            if (!s.ok()) {
                return s;
            }
            applied_index_ = batch.GetIndex();
        }
    }
    return Status::OK();
}

Status MasstreeDBImpl::Open() {
    auto s = loadCheckpoint(replay_);
    if (!s.ok()) {
        return s;
    }
    s = loadWAL(checkpoint_->Applied(), replay_);
    if (!s.ok()) {
        return s;
    }
    return Status::OK();
}

Status MasstreeDBImpl::Close() {
    return Status::OK();
}

Status MasstreeDBImpl::Destroy() {
    truncateTree("data-tree", data_tree_);
    truncateTree("txn-tree", txn_tree_);

    auto s = checkpoint_->Destroy();
    if (!s.ok()) {
        return s;
    }
    return wal_->Destory();
}

uint64_t MasstreeDBImpl::PersistApplied() {
    return applied_index_;
}

Status MasstreeDBImpl::Get(CFType cf, const std::string& key, std::string& value) {
    return getTree(cf)->Get(key, value);
}

Status MasstreeDBImpl::writeBatch(MassTreeBatch *batch) {
    assert(batch->GetIndex() != 0);

    if (!wal_disabled_) {
        auto s = wal_->Append(*batch);
        if (!s.ok()) {
            return s;
        }
    }

    auto s = writeTree(*batch);
    if (!s.ok()) {
        return s;
    }

    applied_index_ = batch->GetIndex();
    dirty_bytes_ += batch->BytesSize();
    checkDirty();
    if (!wal_disabled_) {
        wal_->Compact(checkpoint_->Applied());
    }

    return s;
}

Status MasstreeDBImpl::Put(CFType cf, const std::string& key, const std::string& value, uint64_t raft_index) {
    MassTreeBatch batch(raft_index);
    batch.Put(cf, key, value);
    return writeBatch(&batch);
}

Status MasstreeDBImpl::Delete(CFType cf, const std::string& key, uint64_t raft_index) {
    MassTreeBatch batch(raft_index);
    batch.Delete(cf, key);
    return writeBatch(&batch);
}

Status MasstreeDBImpl::DeleteRange(CFType cf, const std::string& start, const std::string& limit, uint64_t raft_index) {
    uint64_t deleted_count = 0;
    auto tree = getTree(cf);

    auto iter = tree->NewIterator(start, limit);
    while (iter->Valid()) {
        tree->Delete(iter->Key());
        iter->Next();
    }

    return Status::OK();
}

std::unique_ptr<WriteBatch> MasstreeDBImpl::NewWriteBatch() {
    std::unique_ptr<WriteBatch> batch(new MassTreeBatch);
    return batch;
}

Status MasstreeDBImpl::Write(WriteBatch* batch, uint64_t raft_index) {
    auto kv_batch = dynamic_cast<MassTreeBatch*>(batch);
    kv_batch->SetIndex(raft_index);
    return writeBatch(kv_batch);
}

Status MasstreeDBImpl::ApplySnapshotStart(uint64_t raft_index) {
    truncateTree("data-tree", data_tree_);
    truncateTree("txn-tree", txn_tree_);

    dirty_bytes_ = 0;
    applied_index_ = 0;

    auto s = wal_->Truncate();
    if (!s.ok()) {
        return s;
    }

    if (!ckp_opt_.disabled) {
        return checkpoint_->ApplySnapshotStart(raft_index);
    } else {
        return Status::OK();
    }
}

Status MasstreeDBImpl::ApplySnapshotData(WriteBatch* batch) {
    auto kv_batch = dynamic_cast<MassTreeBatch*>(batch);
    if (!ckp_opt_.disabled) {
        auto s = checkpoint_->ApplySnapshotData(kv_batch);
        if (!s.ok()) {
            return s;
        }
    }
    return writeTree(*kv_batch, false);
}

Status MasstreeDBImpl::ApplySnapshotFinish(uint64_t raft_index) {
    if (!ckp_opt_.disabled) {
        auto s = checkpoint_->ApplySnapshotFinish(raft_index);
        if (!s.ok()) {
            return s;
        }
    }
    dirty_bytes_ = 0;
    applied_index_ = raft_index;
    return Status::OK();
}

IteratorPtr MasstreeDBImpl::NewIterator(const std::string& start, const std::string& limit) {
    IteratorPtr iter(new MassIterator(data_tree_, start, limit));
    return iter;
}

Status MasstreeDBImpl::NewIterators(const std::string& start, const std::string& limit,
        IteratorPtr &data_iter, IteratorPtr &txn_iter) {
    data_iter.reset(new MassIterator(data_tree_, start, limit));
    txn_iter.reset(new MassIterator(txn_tree_, start, limit));
    return Status::OK();
}

Status MasstreeDBImpl::SplitDB(uint64_t split_range_id, const std::string& split_key,
             uint64_t raft_index, std::unique_ptr<DB>& split_db) {
    auto split_path = manager_->GetDBPath(split_range_id);
    if (!wal_disabled_) {
        // empty batch means split op applied
        MassTreeBatch batch;
        batch.SetIndex(raft_index);
        auto s = wal_->Append(batch);
        if (!s.ok()) {
            return s;
        }
        // link wal
        s = wal_->CopyTo(JoinFilePath({split_path, kWALSubDir}));
        if (!s.ok()) {
            return s;
        }
    }

    if (!ckp_opt_.disabled) {
        // link checkpoint file
        auto s = checkpoint_->CopyTo(JoinFilePath({split_path, kCheckpointSubDir}));
        if (!s.ok()) {
            return s;
        }
    }

    auto s = manager_->CreatSplit(split_range_id, split_key, end_key_, split_db);
    if (!s.ok()) {
        return s;
    }
    // set applied index
    dynamic_cast<MasstreeDBImpl*>(split_db.get())->applied_index_ = raft_index;
    return s;
}

Status MasstreeDBImpl::ApplySplit(const std::string& split_key, uint64_t raft_index) {
    end_key_ = split_key;
    applied_index_ = raft_index;
    dirty_bytes_ = std::numeric_limits<uint64_t>::max();
    // TODO: cancal running checkpoint job?
    checkDirty();
    return Status::OK();
}

void MasstreeDBImpl::truncateTree(const std::string& tree_name, MasstreeWrapper *tree) {
    auto begin = std::chrono::system_clock::now();
    uint64_t deleted_count = 0;

    // iterate and delete keys
    auto iter = tree->NewIterator(start_key_, end_key_);
    while (iter->Valid()) {
        tree->Delete(iter->Key());
        iter->Next();
        ++deleted_count;
    }

    auto delete_end = std::chrono::system_clock::now();

    // wait rcu free
    RunRCUFree(true);

    auto end = std::chrono::system_clock::now();
    auto total_elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();
    auto delete_elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(delete_end - begin).count();
    auto rcu_wait_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - delete_end).count();

    // print truncate stat
    FLOG_INFO("DB[{}] truncate {} total took {} ms, deleted took {} ms, rcu took {} ms, deleted {} keys.",
            id_, tree_name, total_elapsed_ms, delete_elapsed_ms, rcu_wait_ms, deleted_count);
}

Status MasstreeDBImpl::doCheckpoint(uint64_t applied, bool sync) {
    IteratorPtr data_iter, txn_iter;
    auto s = NewIterators(start_key_, end_key_, data_iter, txn_iter);
    if (!s.ok()) {
        FLOG_ERROR("DB[{}] create iterators failed: {}", id_, s.ToString());
    }
    s = checkpoint_->NewJob(applied, std::move(data_iter), std::move(txn_iter), sync);
    return s;
}

void MasstreeDBImpl::checkDirty() {
    if (ckp_opt_.disabled || ckp_opt_.threshold_bytes == 0 || dirty_bytes_ < ckp_opt_.threshold_bytes ) {
        return;
    }

    // schedular checkpoint
    dirty_bytes_ = 0;
    auto s = doCheckpoint(applied_index_, false);
    if (!s.ok()) {
        if (s.code() == Status::kBusy) {
            FLOG_INFO("DB[{}] run checkpoint job failed: {}", id_, s.ToString());
        } else {
            FLOG_ERROR("DB[{}] run checkpoint job failed: {}", id_, s.ToString());
        }
    }
}

Status MasstreeDBImpl::TEST_Run_Checkpoint() {
    dirty_bytes_ = 0;
    return doCheckpoint(applied_index_, true);
}

} // namespace db
} // namespace ds
} // namespace jimkv
