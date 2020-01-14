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

#include <gtest/gtest.h>

#include "base/util.h"
#include "storage/util.h"
#include "helper/helper_util.h"
#include "helper/store_test_fixture.h"
#include "helper/request_builder.h"
#include "common/ds_encoding.h"
#include "storage/kv_fetcher.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {

using namespace jim;
using namespace jim::test::helper;
using namespace jim::ds;
using namespace jim::ds::storage;
using namespace dspb;

// test base the account table
class StoreTxnTest : public StoreTestFixture {
public:
    StoreTxnTest() : StoreTestFixture(CreateAccountTable()) {}

protected:
    // inserted rows
    std::vector<std::vector<std::string>> rows_;
};

void insertIntent(TxnIntent* intent,
                  const std::string& key, const std::string& value) {
    intent->set_typ(INSERT);
    intent->set_key(key);
    intent->set_value(value);
}

// void insertIntent(TxnIntent* intent,
//                   const std::string& key) {
//     intent->set_typ(DELETE);
//     intent->set_key(key);
// }

static void randomIntent(TxnIntent& intent) {
    intent.set_typ(randomInt() % 2 == 0 ? INSERT : DELETE);
    intent.set_key(randomString(20));
    if (intent.typ() == INSERT) {
        intent.set_value(randomString(100));
    }
}

static void randomTxnValue(TxnValue& value) {
    value.set_txn_id(randomString(10, 20));
    randomIntent(*value.mutable_intent());
    value.set_primary_key(randomString(15));
    value.set_expired_at(calExpireAt(10));
    value.set_version(randomInt());
}

TEST(StoreUtilTest, Assemble) {
    std::string db_value;
    std::string user_value = randomString(20, 50);
    uint64_t version = randomInt();
    storage::pb::InternalTag tag;
    tag.set_txn_id(randomString(20, 50));
    auto s = assembleValue(db_value, user_value, version, &tag);
    ASSERT_TRUE(s.ok()) << s.ToString();
    std::cout << EncodeToHex(db_value) << std::endl;

    uint64_t actual_version = 0;
    std::string actual_txn_id;
    size_t value_length = 0;
    storage::pb::InternalTag actual_tag;
    s = disassembleValue(db_value, value_length, actual_version, &actual_tag);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(actual_version, version);
    ASSERT_EQ(value_length, user_value.size());
    ASSERT_EQ(db_value.substr(0, value_length), user_value);
    ASSERT_EQ(actual_tag.txn_id(), tag.txn_id());
}

TEST_F(StoreTxnTest, Prepare) {
    dspb::TxnIntent tnt;
    randomIntent(tnt);
    dspb::PrepareRequest prepare_req;
    prepare_req.set_txn_id("1");
    std::string pkey("1");
    prepare_req.set_primary_key(pkey);

    PrepareResponse resp;
    uint64_t  version{1};
    store_->TxnPrepare(prepare_req, version, &resp);
    ASSERT_TRUE(resp.errors_size() == 0);
}

TEST_F(StoreTxnTest, InsertDelete) {
    // insert
    std::vector<dspb::TxnIntent> inserted_intents;
    size_t count = randomInt() % 5 + 5;
    for (size_t i = 0; i < count; ++i) {
        dspb::TxnIntent intent;
        intent.set_typ(dspb::INSERT);
        intent.set_key(randomString(10, 20));
        intent.set_value(randomString(20, 50));
        inserted_intents.push_back(std::move(intent));
    }
    testTxn(inserted_intents);

    // delete
    for (auto& intent: inserted_intents) {
        intent.set_typ(dspb::DELETE);
    }
    testTxn(inserted_intents);

    // check store data
    {
        auto store = getStore();
        std::unique_ptr<Iterator> data_iter, txn_iter;
        auto s = store->NewIterators(data_iter, txn_iter);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(data_iter->Valid());
        ASSERT_TRUE(data_iter->status().ok()) << data_iter->status().ToString();
        ASSERT_FALSE(txn_iter->Valid());
        ASSERT_TRUE(txn_iter->status().ok()) << txn_iter->status().ToString();
    }
}

TEST_F(StoreTxnTest, PrepareLocal) {
    dspb::PrepareRequest req;
    req.set_local(true); // set local

    std::string txn_id("txn_1");
    req.set_txn_id(txn_id);
//    std::string pk("pk_1");
//    req.set_primary_key(pk); // for fill ds intent value

    auto intent = req.add_intents();
    std::string key = randomString(10);
    std::string value;
    EncodeBytesValue(&value, 1, randomString(10).c_str(), 10);
    insertIntent(intent, key, value);
    intent->set_check_unique(true);

    PrepareResponse resp;

    uint64_t txn_ver = 1;
    store_->TxnPrepare(req, txn_ver, &resp);
    ASSERT_TRUE(resp.errors_size() == 0);

    // get key
    std::string actual_value;
    auto s = store_->Get(key, &actual_value);
    ASSERT_TRUE(s.ok()) << s.ToString();

    std::string expected_value;
    pb::InternalTag tag;
    tag.set_txn_id(txn_id);
    assembleValue(expected_value, value, txn_ver, &tag);
    ASSERT_EQ(actual_value, expected_value);

    // replay
    store_->TxnPrepare(req, txn_ver + 1, &resp);
    ASSERT_TRUE(resp.errors_size() == 0) << resp.ShortDebugString();

    // get key
    s = store_->Get(key, &actual_value);
    ASSERT_TRUE(s.ok()) << s.ToString();
    assembleValue(expected_value, value, txn_ver+1, &tag);
    ASSERT_EQ(actual_value, expected_value);

    // replay
    req.set_txn_id("txn_2");
    store_->TxnPrepare(req, txn_ver + 1, &resp);
    ASSERT_TRUE(resp.errors_size() == 1);
    ASSERT_TRUE(resp.errors(0).has_not_unique());
}

TEST_F(StoreTxnTest, PrepareLocal_intentKeyLocked) {
    std::string key = randomString(10);
    std::string value = randomString(10);
    std::string actual_value;

    dspb::PrepareRequest req1, req2;
    auto intent1 = req1.add_intents();
    auto intent2 = req2.add_intents();
    PrepareResponse resp1;
    PrepareResponse resp2;

    // txn1 without flag local
    req1.set_txn_id("txn_1");
    insertIntent(intent1, key, value);

    store_->TxnPrepare(req1, 1, &resp1);

    ASSERT_TRUE(resp1.errors_size() == 0);
    store_->Get(key, &actual_value);
    ASSERT_TRUE(actual_value.empty());

    // txn2 with flag local
    req2.set_local(true);

    req2.set_txn_id("txn_2");
    insertIntent(intent2, key, value);

    store_->TxnPrepare(req2, 2, &resp2);

    ASSERT_TRUE((resp2.errors().size() == 1) &&
                (resp2.mutable_errors(0)->err_type() == TxnError_ErrType_LOCKED));
    store_->Get(key, &actual_value);
    ASSERT_TRUE(actual_value.empty());
}

TEST_F(StoreTxnTest, Iterator) {
    // test empty iter
    {
        TxnRangeKvFetcher fetcher(*store_, "", "");
        KvRecord rec;
        auto s = fetcher.Next(rec);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(rec.Valid());
    }

    struct Elem {
        std::string key;
        std::string db_value;
        std::string txn_value;
    };

    std::vector<Elem> expected;
    for (int i = 0; i < 100; ++i) {
        char suffix[20] = {'\0'};
        snprintf(suffix, 20, "%05d", i);
        std::string key;
        EncodeKeyPrefix(&key, meta_.table_id());
        key += suffix;

        expected.emplace_back(Elem());
        expected.back().key = key;
        int choice = randomInt() % 3;
        bool has_value = choice == 0 || choice == 2;
        bool has_intent = choice == 1 || choice == 2;
        ASSERT_TRUE(has_value || has_intent);
        if (has_value) {
            std::string value = randomString(10, 20);
            auto s = store_->Put(key, value, 1);
            ASSERT_TRUE(s.ok()) << s.ToString();
            expected.back().db_value = value;
        }
        if (has_intent) {
            TxnValue txn_val;
            randomTxnValue(txn_val);
            txn_val.mutable_intent()->set_key(key);
            auto s = putTxn(key, txn_val);
            ASSERT_TRUE(s.ok()) << s.ToString();
            expected.back().txn_value = txn_val.SerializeAsString();
        }
    }

    size_t index = 0;
    TxnRangeKvFetcher iter(*store_, "", "");
    while (true) {
        KvRecord rec;
        auto s = iter.Next(rec);
        ASSERT_TRUE(s.ok()) << s.ToString();
        if (!rec.Valid()) break;
        ASSERT_EQ(rec.key, expected[index].key);
        ASSERT_EQ(rec.value, expected[index].db_value);
        ASSERT_EQ(rec.intent, expected[index].txn_value);
        ++index;
    }
    ASSERT_EQ(index, expected.size());
}


TEST_F(StoreTxnTest, Scan) {
    // scan empty
    {
        dspb::ScanRequest req;
        dspb::ScanResponse resp;
        auto s = store_->TxnScan(req, &resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_EQ(resp.kvs_size(), 0);
        ASSERT_EQ(resp.code(), 0);
    }

    // insert some keys
    struct Elem {
        std::string key;
        std::string db_value;
        std::string txn_value;
    };
    std::vector<Elem> expected;
    for (int i = 0; i < 100; ++i) {
        char suffix[20] = {'\0'};
        snprintf(suffix, 20, "%05d", i);
        std::string key;
        EncodeKeyPrefix(&key, meta_.table_id());
        key += suffix;

        expected.emplace_back(Elem());
        expected.back().key = key;
        int choice = randomInt() % 3;
        bool has_value = choice == 0 || choice == 2;
        bool has_intent = choice == 1 || choice == 2;
        ASSERT_TRUE(has_value || has_intent);
        if (has_value) {
            std::string value;
            std::string user_value = randomString(10, 20);

            storage::pb::InternalTag tag;
            tag.set_txn_id(randomString(20, 50));
            auto st = assembleValue(value, user_value, 1000+i, &tag);
            ASSERT_TRUE(st.ok()) << st.ToString();

            auto s = store_->Put(key, value, 1000+i);
            ASSERT_TRUE(s.ok()) << s.ToString();
            expected.back().db_value = user_value;
        }
        if (has_intent) {
            TxnValue txn_val;
            randomTxnValue(txn_val);
            txn_val.mutable_intent()->set_key(key);
            auto s = putTxn(key, txn_val);
            ASSERT_TRUE(s.ok()) << s.ToString();
            expected.back().txn_value = txn_val.SerializeAsString();
        }
    }

    // scan all
    {
        dspb::ScanRequest req;
        req.set_max_count(1000000);
        dspb::ScanResponse resp;
        auto s = store_->TxnScan(req, &resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_EQ(static_cast<size_t>(resp.kvs_size()), expected.size());
        for (int i = 0; i < resp.kvs_size(); ++i) {
            ASSERT_EQ(resp.kvs(i).key(), expected[i].key);
            ASSERT_EQ(resp.kvs(i).value(), expected[i].db_value);
            ASSERT_EQ(resp.kvs(i).has_intent(), !expected[i].txn_value.empty());
            if (resp.kvs(i).has_intent()) {
                dspb::TxnValue txn_value;
                ASSERT_TRUE(txn_value.ParseFromString(expected[i].txn_value));
                auto& intent = resp.kvs(i).intent();
                ASSERT_EQ(intent.op_type(), txn_value.intent().typ());
                ASSERT_EQ(intent.txn_id(), txn_value.txn_id());
                ASSERT_EQ(intent.primary_key(), txn_value.primary_key());
                auto expected_val = txn_value.intent().value();
//                EncodeIntValue(&expected_val, kVersionColumnID, txn_value.version());
                ASSERT_EQ(intent.value(), expected_val);
            }
        }
    }
}
}
