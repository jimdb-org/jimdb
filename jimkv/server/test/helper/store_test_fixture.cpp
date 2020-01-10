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

#include "store_test_fixture.h"

#include "base/fs_util.h"
#include "query_parser.h"
#include "helper_util.h"

#include "common/server_config.h"

namespace jimkv {
namespace test {
namespace helper {

using namespace ::jimkv::ds::storage;

StoreTestFixture::StoreTestFixture(std::unique_ptr<Table> t) :
    table_(std::move(t)) {
}

void StoreTestFixture::SetUp() {
    InitLog();

    if (!table_) {
        throw std::runtime_error("invalid table");
    }

    // open rocksdb
    char path[] = "/tmp/jimkv_ds_store_test_XXXXXX";
    char* tmp = mkdtemp(path);
    ASSERT_TRUE(tmp != NULL);
    tmp_dir_ = tmp;

    auto s = NewDBManager(tmp_dir_, db_manager_);
    ASSERT_TRUE(s.ok()) << s.ToString();

    // make meta
    meta_ = MakeRangeMeta(table_.get());

    std::unique_ptr<DB> db;
    s = db_manager_->CreateDB(meta_.id(), meta_.start_key(), meta_.end_key(), db);
    ASSERT_TRUE(s.ok()) << s.ToString();

    store_ = new jimkv::ds::storage::Store(meta_, std::move(db));
}

void StoreTestFixture::TearDown() {
    delete store_;
    db_manager_.reset();
    if (!tmp_dir_.empty()) {
        RemoveDirAll(tmp_dir_.c_str());
    }
}

Status StoreTestFixture::testSelect(
        const std::function<void(SelectRequestBuilder&)>& build_func,
        const std::vector<std::vector<std::string>>& expected_rows) {
    SelectRequestBuilder builder(table_.get());
    build_func(builder);
    auto req = builder.Build();

    dspb::SelectResponse resp;
    auto s = store_->TxnSelect(req, &resp);
    if (!s.ok()) {
        return Status(Status::kUnexpected, "select", s.ToString());
    }
    if (resp.code() != 0) {
        return Status(Status::kUnexpected, "select code", std::to_string(resp.code()));
    }

    SelectResultParser parser(req, resp);
    s = parser.Match(expected_rows);
    if (!s.ok()) {
        return Status(Status::kUnexpected, "select rows", s.ToString());
    }
    return Status::OK();
}

Status StoreTestFixture::testSelectFlow(
        const std::function<void(SelectFlowRequestBuilder&)>& build_func,
        const std::vector<std::vector<std::string>>& expected_rows) {
    SelectFlowRequestBuilder builder(table_.get());
    build_func(builder);
    auto req = builder.Build();

    dspb::SelectFlowResponse resp;
    auto s = store_->TxnSelectFlow(req, &resp);
    if (!s.ok()) {
        return Status(Status::kUnexpected, "select", s.ToString());
    }
    if (resp.code() != 0) {
        return Status(Status::kUnexpected, "select code", std::to_string(resp.code()));
    }

    // std::cout << "testSelectFlow resp:" << resp.DebugString() << std::endl;
    SelectFlowResultParser parser(req, resp);
    s = parser.Match(expected_rows);
    if (!s.ok()) {
        return Status(Status::kUnexpected, "select rows", s.ToString());
    }
    return Status::OK();
}

Status StoreTestFixture::testInsert(const std::vector<std::vector<std::string>> &rows, uint64_t *insert_bytes) {
    InsertRequestBuilder builder(table_.get());
    builder.AddRows(rows);
    auto req = builder.Build();

    dspb::PrepareResponse resp;
    store_->TxnPrepare(req, ++raft_index_, &resp);
    if (resp.errors_size() != 0) {
        return Status(Status::kUnexpected, "insert", resp.ShortDebugString());
    }
    if (insert_bytes != nullptr) {
        *insert_bytes = 0;
        for (const auto& row: req.intents()) {
            *insert_bytes += row.key().size();
            *insert_bytes += row.value().size();
        }
    }
    return Status::OK();
}

Status StoreTestFixture::testDelete(const std::function<void(DeleteRequestBuilder&)>& build_func,
                  uint64_t expected_affected) {
    DeleteRequestBuilder builder(table_.get());
    build_func(builder);
    auto req = builder.Build();

    dspb::PrepareResponse resp;
    store_->TxnPrepare(req, ++raft_index_, &resp);
    if (resp.errors_size() != 0) {
        return Status(Status::kUnexpected, "delete", resp.ShortDebugString());
    }
    return Status::OK();
}

uint64_t StoreTestFixture::statSizeUntil(const std::string& end) {
    uint64_t size = 0;
    auto it = store_->NewIterator("", end);
    while (it->Valid()) {
        size += it->KeySize();
        size += it->ValueSize();
        it->Next();
    }
    return size;
}

Status StoreTestFixture::putTxn(const std::string& key, const dspb::TxnValue& value) {
    auto batch = store_->db_->NewWriteBatch();
    auto s = store_->writeTxnValue(value, batch.get());
    if (!s.ok()) {
        return s;
    }
    return store_->db_->Write(batch.get(), ++raft_index_);
}

void StoreTestFixture::testTxn(std::vector<dspb::TxnIntent>& intents) {
    // use first intent as primary
    intents[0].set_is_primary(true);
    const auto& primary = intents[0];
    for (size_t i = 1; i < intents.size(); ++i) {
        intents[i].set_is_primary(false);
    }

    auto txn_id = randomString(20, 50);
    Status s;

    {
        // prepare primary
        dspb::PrepareRequest req;
        req.set_txn_id(txn_id);
        req.set_primary_key(primary.key());
        req.set_lock_ttl(1000000);
        req.add_intents()->CopyFrom(intents[0]);
        for (size_t i = 1; i < intents.size(); ++i) {
            req.add_secondary_keys(intents[i].key());
        }
        dspb::PrepareResponse resp;
        store_->TxnPrepare(req, 1, &resp);
        ASSERT_EQ(resp.errors_size(), 0) << resp.DebugString();
    }

    // prepare secondary
    for (size_t i = 1; i < intents.size(); ++i) {
        dspb::PrepareRequest req;
        req.set_txn_id(txn_id);
        req.set_primary_key(primary.key());
        req.set_lock_ttl(1000000);
        req.add_intents()->CopyFrom(intents[i]);

        dspb::PrepareResponse resp;
        store_->TxnPrepare(req, ++raft_index_, &resp);
        ASSERT_EQ(resp.errors_size(), 0) << resp.DebugString();
    }

    // check prepare result
    for (const auto& intent: intents) {
        dspb::TxnValue txn_value;
        s = store_->GetTxnValue(intent.key(), &txn_value);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_EQ(txn_value.txn_id(), txn_id);
        ASSERT_EQ(txn_value.intent().ShortDebugString(), intent.ShortDebugString());
        ASSERT_EQ(txn_value.primary_key(), primary.key());
        ASSERT_EQ(txn_value.txn_status(), dspb::TXN_INIT);

        if (intent.is_primary()) {
            ASSERT_EQ(static_cast<size_t>(txn_value.secondary_keys_size()), intents.size() - 1);
            for (int i = 0; i < txn_value.secondary_keys_size(); ++i) {
                ASSERT_EQ(txn_value.secondary_keys(i), intents[i+1].key());
            }
        }
    }

    // decide primary
    {
        dspb::DecideRequest req;
        req.set_txn_id(txn_id);
        req.set_status(dspb::COMMITTED);
        req.add_keys(primary.key());
        req.set_is_primary(true);

        dspb::DecideResponse resp;
        store_->TxnDecide(req, ++raft_index_, &resp);
        ASSERT_FALSE(resp.has_err()) << resp.DebugString();
    }

    // decide secondary
    for (size_t i = 1; i < intents.size(); ++i) {
        dspb::DecideRequest req;
        req.set_txn_id(txn_id);
        req.set_status(dspb::COMMITTED);
        req.set_is_primary(false);
        req.add_keys(intents[i].key());

        dspb::DecideResponse resp;
        store_->TxnDecide(req, ++raft_index_, &resp);
        ASSERT_FALSE(resp.has_err()) << resp.DebugString();
    }

    // clearup
    {
        dspb::ClearupRequest req;
        req.set_txn_id(txn_id);
        req.set_primary_key(primary.key());

        dspb::ClearupResponse resp;
        store_->TxnClearup(req, ++raft_index_, &resp);
        ASSERT_FALSE(resp.has_err()) << resp.DebugString();
    }

    // check result
    for (const auto& intent: intents) {
        dspb::TxnValue txn_value;
        s = store_->GetTxnValue(intent.key(), &txn_value);
        ASSERT_EQ(s.code(), Status::kNotFound); // all intent is cleared

        std::string value;
        s = store_->Get(intent.key(), &value);
        if (intent.typ() == dspb::INSERT) {
            ASSERT_TRUE(s.ok()) << s.ToString();
        } else {
            ASSERT_EQ(s.code(), Status::kNotFound);
        }
    }
}

} /* namespace helper */
} /* namespace test */
} /* namespace jimkv */
