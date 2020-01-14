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

#include <mutex>

#include "metric.h"
#include "dspb/api.pb.h"
#include "field_value.h"
#include "db/db.h"
#include "raft/snapshot.h"

// test fixture forward declare for friend class
namespace jim { namespace test { namespace helper { class StoreTestFixture; }}}

namespace jim {
namespace ds {
namespace storage {

using IterPtr = db::IteratorPtr;

using TxnErrorPtr = std::unique_ptr<dspb::TxnError>;


static const size_t kRowPrefixLength = 4;
static const size_t kDefaultMaxSelectLimit = 1000;

class Store {
public:
    Store(const basepb::Range& meta, std::unique_ptr<db::DB> db);
    ~Store();

    Store(const Store&) = delete;
    Store& operator=(const Store&) = delete;

    uint64_t GetTableID() const {  return table_id_; }
    uint64_t GetRangeID() const {  return range_id_; }

    std::string GetStartKey() const;

    void SetEndKey(std::string end_key);
    std::string GetEndKey() const;

    const basepb::KeySchema& GetKeySchema() const { return key_schema_; }

    void ResetMetric() { metric_.Reset(); }
    void CollectMetric(MetricStat* stat) { metric_.Collect(stat); }

    Status StatSize(uint64_t split_size, uint64_t *real_size, std::string *split_key, uint64_t *kv_count_1, uint64_t *kv_count_2);
    Status StatSize(const std::string & split_key, uint64_t* left_count, uint64_t* left_size, uint64_t* right_count, uint64_t* right_size);

    uint64_t PersistApplied() { return db_->PersistApplied(); }

    Status Split(uint64_t new_range_id, const std::string& split_key, uint64_t raft_index,
            std::unique_ptr<db::DB>& new_db);

    Status ApplySplit(const std::string& split_key, uint64_t raft_index);

    Status Destroy();

    Status Get(const std::string& key, std::string* value, bool metric = true);
    Status Put(const std::string& key, const std::string& value, uint64_t raft_index);
    Status Delete(const std::string& key, uint64_t raft_index);
    Status DeleteRange(const std::string& start, const std::string& limit, uint64_t raft_index);

    Status GetTxnValue(const std::string& key, std::string& db_value);
    Status GetTxnValue(const std::string& key, dspb::TxnValue* value);

    uint64_t TxnPrepare(const dspb::PrepareRequest& req, uint64_t raft_index, dspb::PrepareResponse* resp);
    uint64_t TxnDecide(const dspb::DecideRequest& req, uint64_t raft_index, dspb::DecideResponse* resp);
    void TxnClearup(const dspb::ClearupRequest& req, uint64_t raft_index, dspb::ClearupResponse* resp);
    void TxnGetLockInfo(const dspb::GetLockInfoRequest& req, dspb::GetLockInfoResponse* resp);
    Status TxnSelect(const dspb::SelectRequest& req, dspb::SelectResponse* resp);
    Status TxnScan(const dspb::ScanRequest& req, dspb::ScanResponse* resp);
    Status TxnSelectFlow(const dspb::SelectFlowRequest& req, dspb::SelectFlowResponse* resp);

    Status IdxStatsGet(const dspb::IndexStatsRequest& req, dspb::IndexStatsResponse& resp);
    Status ColsStatsGet(const dspb::ColumnsStatsRequest& req, dspb::ColumnsStatsResponse& resp);
public:

    uint64_t KvCount() { return kv_count_; }
    void SetKvCount(uint64_t kv_count) {
        kv_count_ = kv_count;
    }

    bool keyInScop(const std::string & key) const;
    IterPtr NewIterator(const std::string& start = "", const std::string& limit = "");

    Status NewIterators(IterPtr &data_iter, IterPtr &txn_iter,
            const std::string& start = "", const std::string& limit = "");

    Status GetSnapshot(uint64_t apply_index, std::string&& context,
            std::shared_ptr<raft::Snapshot>* snapshot);

    Status ApplySnapshotStart(uint64_t raft_index);
    Status ApplySnapshotData(const std::vector<std::string>& datas);
    Status ApplySnapshotFinish(uint64_t raft_index);

    void addMetricRead(uint64_t keys, uint64_t bytes);
    void addMetricWrite(uint64_t keys, uint64_t bytes);

private:
    friend class ::jim::test::helper::StoreTestFixture;

    using KeyScope = std::pair<std::string, std::string>;

    KeyScope fixKeyScope(const std::string& start_key, const std::string& end_key) const;

    Status writeTxnValue(const dspb::TxnValue& value, db::WriteBatch* batch);
    TxnErrorPtr checkLockable(const std::string& key, const std::string& txn_id, bool *exist_flag);
    Status getCommittedInfo(const std::string& key, uint64_t& version, std::string& txn_id);
    TxnErrorPtr checkUniqueAndVersion(const dspb::TxnIntent& intent, const std::string& txn_id, bool local = false);
    uint64_t prepareLocal(const dspb::PrepareRequest& req, uint64_t version, dspb::PrepareResponse* resp);
    TxnErrorPtr prepareIntent(const dspb::PrepareRequest& req, const dspb::TxnIntent& intent,
            uint64_t version, db::WriteBatch* batch);

    Status commitIntent(const dspb::TxnIntent& intent, uint64_t version,
            const std::string& txn_id, uint64_t &bytes_written, db::WriteBatch* batch);

    TxnErrorPtr decidePrimary(const dspb::DecideRequest& req, uint64_t& bytes_written,
            db::WriteBatch* batch, dspb::DecideResponse* resp);
    TxnErrorPtr decideSecondary(const dspb::DecideRequest& req, const std::string& key,
            uint64_t& bytes_written, db::WriteBatch* batch);

private:
    const uint64_t table_id_ = 0;
    const uint64_t range_id_ = 0;
    const std::string start_key_;
    uint64_t kv_count_;

    std::string end_key_;
    mutable std::mutex key_lock_;

    std::unique_ptr<db::DB> db_;

    basepb::KeySchema key_schema_;

    Metric metric_;
};

} /* namespace storage */
} /* namespace ds */
} /* namespace jim */
