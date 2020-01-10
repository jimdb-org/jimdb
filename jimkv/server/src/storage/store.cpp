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

#include "store.h"

#include "base/util.h"
#include "common/server_config.h"
#include "common/ds_encoding.h"

#include "field_value.h"
#include "snapshot.h"

namespace jimkv {
namespace ds {
namespace storage {

Store::Store(const basepb::Range& meta, std::unique_ptr<db::DB> db)
    : table_id_(meta.table_id()),
      range_id_(meta.id()),
      start_key_(meta.start_key()),
      end_key_(meta.end_key()),
      db_(std::move(db)),
      key_schema_(meta.key_schema()) {

    assert(!start_key_.empty());
    assert(!end_key_.empty());
    // check key (primary col or index col)
    assert(key_schema_.key_cols_size() > 0);

}

Store::~Store() = default;

Status Store::Get(const std::string& key, std::string* value, bool metric) {
    assert(value != nullptr);
    auto s = db_->Get(key, *value);
    switch (s.code()) {
        case Status::kOk:
            if (metric) {
                addMetricRead(1, key.size() + value->size());
            }
            return Status::OK();
        case Status::kNotFound:
            return Status(Status::kNotFound);
        default:
            return Status(Status::kIOError, "get", s.ToString());
    }
}

Status Store::Put(const std::string& key, const std::string& value, uint64_t raft_index) {
    auto s = db_->Put(key, value, raft_index);
    if (s.ok()) {
        addMetricWrite(1, key.size() + value.size());
        return Status::OK();
    }
    return Status(Status::kIOError, "put", s.ToString());
}

Status Store::Delete(const std::string& key, uint64_t raft_index) {
    auto s = db_->Delete(key, raft_index);
    switch (s.code()) {
        case Status::kOk:
            addMetricWrite(1, key.size());
            return Status::OK();
        case Status::kNotFound:
            return Status(Status::kNotFound);
        default:
            return Status(Status::kIOError, "delete", s.ToString());
    }
}

Status Store::DeleteRange(const std::string& start, const std::string& limit, uint64_t raft_index) {
    auto s = db_->DeleteRange(start, limit, raft_index);
    switch (s.code()) {
        case Status::kOk:
            addMetricWrite(1, start.size() + limit.size());
            return Status::OK();
        case Status::kNotFound:
            return Status(Status::kNotFound);
        default:
            return Status(Status::kIOError, "delete range", s.ToString());
    }
}

Status Store::Destroy() {
    return db_->Destroy();
}

std::string Store::GetStartKey() const {
    return start_key_;
}

void Store::SetEndKey(std::string end_key) {
    std::unique_lock<std::mutex> lock(key_lock_);
    assert(start_key_ < end_key);
    end_key_ = std::move(end_key);
}

std::string Store::GetEndKey() const {
    std::unique_lock<std::mutex> lock(key_lock_);
    return end_key_;
}

Store::KeyScope Store::fixKeyScope(const std::string& start_key, const std::string& end_key) const {
    KeyScope result;
    if (start_key.empty() || start_key < start_key_) {
        result.first = start_key_;
    } else {
        result.first = start_key;
    }

    auto store_end_key = GetEndKey();
    if (end_key.empty() || end_key > store_end_key) {
        result.second = std::move(store_end_key);
    } else {
        result.second = end_key;
    }

    assert(result.first <= result.second);
    assert(result.first >= start_key_);

    return result;
}

bool Store::keyInScop(const std::string & key) const
{
    bool f = false;
    if ( start_key_ <= key && key < GetEndKey()) {
        f = true;
    }

    return f;
}

IterPtr Store::NewIterator(const std::string& start, const std::string& limit) {
    auto scope = fixKeyScope(start, limit);
    return IterPtr(db_->NewIterator(scope.first, scope.second));
}

Status Store::NewIterators(IterPtr &data_iter, IterPtr &txn_iter,
        const std::string& start, const std::string& limit) {
    auto scope = fixKeyScope(start, limit);
    return db_->NewIterators(scope.first, scope.second, data_iter, txn_iter);
}

Status Store::GetSnapshot(uint64_t apply_index, std::string&& context,
                          std::shared_ptr<raft::Snapshot>* snapshot) {
    assert(snapshot != nullptr);

    IterPtr data_iter, txn_iter;
    auto s = this->NewIterators(data_iter, txn_iter);
    if (!s.ok()) {
        return s;
    }
    snapshot->reset(new Snapshot(apply_index, std::move(context), std::move(data_iter), std::move(txn_iter)));
    return Status::OK();
}

Status Store::ApplySnapshotStart(uint64_t raft_index) {
    return db_->ApplySnapshotStart(raft_index);
}

Status Store::ApplySnapshotData(const std::vector<std::string>& datas) {
    auto batch = db_->NewWriteBatch();
    for (const auto& data : datas) {
        dspb::SnapshotKVPair p;
        if (!p.ParseFromString(data)) {
            return Status(Status::kCorruption, "apply snapshot data", "deserilize return false");
        }
        switch (p.cf_type()) {
        case dspb::CF_DEFAULT:
            batch->Put(db::CFType::kData, p.key(), p.value());
            break;
        case dspb::CF_TXN:
            batch->Put(db::CFType::kTxn, p.key(), p.value());
            break;
        default:
            return Status(Status::kInvalidArgument,
                          "apply snapshot data: invalid cf type: ", std::to_string(p.cf_type()));
        }
    }
    auto ret = db_->ApplySnapshotData(batch.get());
    if (!ret.ok()) {
        return Status(Status::kIOError, "snap batch write", ret.ToString());
    } else {
        return Status::OK();
    }
}

Status Store::ApplySnapshotFinish(uint64_t raft_index) {
    return db_->ApplySnapshotFinish(raft_index);
}

void Store::addMetricRead(uint64_t keys, uint64_t bytes) {
    metric_.AddRead(keys, bytes);
    g_metric.AddRead(keys, bytes);
}

void Store::addMetricWrite(uint64_t keys, uint64_t bytes) {
    metric_.AddWrite(keys, bytes);
    g_metric.AddWrite(keys, bytes);
}

Status Store::StatSize(uint64_t split_size, uint64_t* real_size, std::string* split_key, uint64_t* kv_count_1, uint64_t *kv_count_2) {
    uint64_t total_size = 0;
    uint64_t count_1 = 0;
    uint64_t count_2 = 0;

    // The number of the same characters is greater than
    // the length of start_key_ and more than 5,
    // then the length of the split_key is
    // start_key_.length() + 5
    auto max_len = start_key_.length() + 5;

    auto it = NewIterator();
    std::string middle_key;
    std::string first_key;

    while (it->Valid()) {
        total_size += it->KeySize();
        total_size += it->ValueSize();
        count_1++;

        if (total_size >= split_size) {
            middle_key = it->Key();
            it->Next();
            break;
        }

        it->Next();
    }

    if (!it->Valid()) {
        if (!it->status().ok()) {
            return it->status();
        } else {
            *kv_count_1 = count_1;
            *kv_count_2 = count_2;
            *real_size = total_size;
            kv_count_  = count_1 + count_2;
            return Status(Status::kUnexpected, "no more data", "total_size:{"+std::to_string(total_size)+"}");
        }
    }

    *split_key = SliceSeparate(it->Key(), middle_key, max_len);

    // iterate remain datas
    while (it->Valid()) {
        total_size += it->KeySize();
        total_size += it->ValueSize();
        it->Next();
        count_2++;
    }
    if (!it->status().ok()) {
        return it->status();
    }

    *real_size = total_size;
    *kv_count_1 = count_1;
    *kv_count_2 = count_2;
    kv_count_ = count_1 + count_2;
    return Status::OK();
}

Status Store::StatSize(const std::string& split_key, uint64_t* left_count, uint64_t* left_size, uint64_t* right_count, uint64_t* right_size) {

    left_count = 0;
    right_count = 0;
    left_size = 0;
    right_size = 0;

    auto it = NewIterator();

    while (it->Valid()) {
        left_size += it->KeySize();
        left_size += it->ValueSize();
        left_count++;

        if ( it->Key() >= split_key) {
            it->Next();
            break;
        }
        it->Next();
    }

    if (!it->Valid()) {
        if (!it->status().ok()) {
            return it->status();
        } else {
            return Status(Status::kUnexpected, "no more data", "left_size:{"+std::to_string(*left_size)+"}");
        }
    }

    while (it->Valid()) {
        right_size += it->KeySize();
        right_size += it->ValueSize();
        it->Next();
        right_count++;
    }
    if (!it->status().ok()) {
        return it->status();
    }

    return Status::OK();
}

Status Store::Split(uint64_t new_range_id, const std::string& split_key, uint64_t raft_index,
             std::unique_ptr<db::DB>& new_db) {
    auto s = db_->SplitDB(new_range_id, split_key, raft_index, new_db);
    if (!s.ok()) {
        return s;
    }
    return Status::OK();
}

Status Store::ApplySplit(const std::string& split_key, uint64_t raft_index) {
    SetEndKey(split_key);
    return db_->ApplySplit(split_key, raft_index);
}

} /* namespace storage */
} /* namespace ds */
} /* namespace jimkv */
