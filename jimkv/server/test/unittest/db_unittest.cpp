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

#include <gtest/gtest.h>

#include "base/fs_util.h"
#include "../helper/helper_util.h"

#include "db/mass_tree_impl/db_impl.h"
#include "db/db.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

using namespace jimkv::ds::db;

namespace jimkv {
namespace ds {
namespace storage {

static void CheckDB(DB* db, const std::string& start_key, const std::string& end_key,
        const std::map<std::string, std::string>& def_kvs, const std::map<std::string, std::string>& txn_kvs) {
    // test get
    for (const auto& kv : def_kvs) {
        std::string value;
        auto s = db->Get(CFType::kData, kv.first, value);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_EQ(value, kv.second);
    }
    for (const auto& kv : txn_kvs) {
        std::string value;
        auto s = db->Get(CFType::kTxn, kv.first, value);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_EQ(value, kv.second);
    }

    // test iterator
    auto iter = db->NewIterator(start_key, end_key);
    for (const auto& kv : def_kvs) {
        ASSERT_TRUE(iter->Valid());
        ASSERT_TRUE(iter->status().ok()) << iter->status().ToString();
        ASSERT_EQ(iter->Key(), kv.first);
        ASSERT_EQ(iter->KeySize(), kv.first.size());
        ASSERT_EQ(iter->Value(), kv.second);
        ASSERT_EQ(iter->ValueSize(), kv.second.size());
        iter->Next();
    }
    ASSERT_FALSE(iter->Valid());
    ASSERT_TRUE(iter->status().ok()) << iter->status().ToString();

    std::unique_ptr<Iterator> default_iter, txn_iter;
    auto s = db->NewIterators(start_key, end_key, default_iter, txn_iter);
    ASSERT_TRUE(s.ok()) << s.ToString();
    for (const auto& kv : def_kvs) {
        ASSERT_TRUE(default_iter->Valid());
        ASSERT_TRUE(default_iter->status().ok()) << default_iter->status().ToString();
        ASSERT_EQ(default_iter->Key(), kv.first);
        ASSERT_EQ(default_iter->KeySize(), kv.first.size());
        ASSERT_EQ(default_iter->Value(), kv.second);
        ASSERT_EQ(default_iter->ValueSize(), kv.second.size());
        default_iter->Next();
    }
    ASSERT_FALSE(default_iter->Valid());
    ASSERT_TRUE(default_iter->status().ok()) << default_iter->status().ToString();
    for (const auto& kv : txn_kvs) {
        ASSERT_TRUE(txn_iter->Valid());
        ASSERT_TRUE(txn_iter->status().ok()) << txn_iter->status().ToString();
        ASSERT_EQ(txn_iter->Key(), kv.first);
        ASSERT_EQ(txn_iter->KeySize(), kv.first.size());
        ASSERT_EQ(txn_iter->Value(), kv.second);
        ASSERT_EQ(txn_iter->ValueSize(), kv.second.size());
        txn_iter->Next();
    }
    ASSERT_FALSE(txn_iter->Valid());
    ASSERT_TRUE(txn_iter->status().ok()) << txn_iter->status().ToString();
}

class DBTest: public ::testing::Test {
protected:
    DBTest() = default;

    void SetUp() override {
        char path[] = "/tmp/jimkv_ds_db_test_XXXXXX";
        char *tmp = mkdtemp(path);
        ASSERT_TRUE(tmp != NULL);
        data_path_ = tmp;

        auto s = jimkv::test::helper::NewDBManager(data_path_, manager_);
        ASSERT_TRUE(s.ok()) << s.ToString();

        OpenDB();
    }

    void TearDown() override {
        db_.reset();
        manager_.reset();
        if (!data_path_.empty()) {
            jimkv::RemoveDirAll(data_path_.c_str());
        }
    }

    void OpenDB() {
        auto s = manager_->CreateDB(range_id_, start_key_, end_key_, db_);
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    void ReOpenDB() {
        auto s = db_->Close();
        ASSERT_TRUE(s.ok()) << s.ToString();
        db_.reset();
        manager_.reset();

        s = jimkv::test::helper::NewDBManager(data_path_, manager_);
        ASSERT_TRUE(s.ok()) << s.ToString();

        OpenDB();
    }

    void Check(const std::map<std::string, std::string>& def_kvs,
                 const std::map<std::string, std::string>& txn_kvs) {
        CheckDB(db_.get(), start_key_, end_key_, def_kvs, txn_kvs);
    }

    void DisableCheckpoint() {
        auto p = dynamic_cast<MasstreeDBImpl*>(db_.get());
        if (p) {
            p->TEST_Disable_Checkpoint();
        }
    }

    Status RunCheckpoint() {
        auto p = dynamic_cast<MasstreeDBImpl*>(db_.get());
        if (p) {
            return p->TEST_Run_Checkpoint();
        } else {
            return Status::OK();
        }
    }

protected:
    const uint64_t range_id_ = 1;
    const std::string start_key_ = std::string("\x00", 1);
    const std::string end_key_ = "\xff ";

    std::string data_path_;
    std::unique_ptr<DBManager> manager_ = nullptr;
    std::unique_ptr<DB> db_ = nullptr;
    uint64_t index_ = 0;
};

} // namespace storage
} // namespace ds
} // namespace jimkv


namespace {

using namespace jimkv;
using namespace jimkv::ds::storage;

TEST_F(DBTest, InitalState) {
    ASSERT_EQ(db_->PersistApplied(), 0U);
    // no data
    auto iter = db_->NewIterator(start_key_, end_key_);
    ASSERT_FALSE(iter->Valid());
    ASSERT_TRUE(iter->status().ok()) << iter->status().ToString();

    std::unique_ptr<Iterator> default_iter, txn_iter;
    auto s = db_->NewIterators(start_key_, end_key_, default_iter, txn_iter);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(default_iter->Valid());
    ASSERT_TRUE(default_iter->status().ok()) << default_iter->status().ToString();
    ASSERT_FALSE(txn_iter->Valid());
    ASSERT_TRUE(txn_iter->status().ok()) << txn_iter->status().ToString();
}

TEST_F(DBTest, PutGetDelete) {
    DisableCheckpoint();

    // with cf
    for (int i = 0; i < 10; ++i) {
        auto cf = i % 2 == 0 ? CFType::kData : CFType::kTxn;
        for (int j = 0; j < 1000; ++j) {
            std::string key = jimkv::randomString(32, 1024);
            std::string value = jimkv::randomString(64);
            auto s = db_->Put(cf, key, value, ++index_);
            ASSERT_TRUE(s.ok()) << s.ToString();
            ASSERT_EQ(db_->PersistApplied(), index_);

            std::string actual_value;
            s = db_->Get(cf, key, actual_value);
            ASSERT_TRUE(s.ok()) << s.ToString();
            ASSERT_EQ(actual_value, value);

            s = db_->Delete(cf, key, ++index_);
            ASSERT_TRUE(s.ok()) << s.ToString();
            ASSERT_EQ(db_->PersistApplied(), index_);
            s = db_->Get(cf, key, actual_value);
            ASSERT_EQ(s.code(), Status::kNotFound);
        }
    }

    // no cf
    for (int j = 0; j < 1000; ++j) {
        std::string key = jimkv::randomString(32, 1024);
        std::string value = jimkv::randomString(64);
        auto s = db_->DB::Put(key, value, ++index_);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_EQ(db_->PersistApplied(), index_);

        std::string actual_value;
        s = db_->DB::Get(key, actual_value);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_EQ(actual_value, value);

        s = db_->DB::Delete(key, ++index_);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_EQ(db_->PersistApplied(), index_);
        s = db_->DB::Get(key, actual_value);
        ASSERT_EQ(s.code(), Status::kNotFound);
    }

    ReOpenDB();
    ASSERT_EQ(db_->PersistApplied(), index_);
    Check({}, {});
}

TEST_F(DBTest, ReopenDestroy) {
    DisableCheckpoint();

    std::map<std::string, std::string> def_kvs;
    std::map<std::string, std::string> txn_kvs;
    std::map<std::string, std::string> *kvs = nullptr;
    for (int j = 0; j < 5000; ++j) {
        auto cf = randomInt() % 2 == 0 ? CFType::kData : CFType::kTxn;
        kvs = cf == CFType::kData ? &def_kvs : &txn_kvs;
        std::string key = jimkv::randomString(32, 1024);
        std::string value = jimkv::randomString(64);
        auto s = db_->Put(cf, key, value, ++index_);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_EQ(index_, db_->PersistApplied());
        kvs->emplace(key, value);
        if (j % 1000 == 500) {
            auto s = RunCheckpoint();
            ASSERT_TRUE(s.ok()) << s.ToString();
            ASSERT_EQ(db_->PersistApplied(), index_);
        }
    }
    Check(def_kvs, txn_kvs);

    ReOpenDB();
    ASSERT_EQ(db_->PersistApplied(), index_);
    Check(def_kvs, txn_kvs);

    auto s = db_->Destroy();
    ASSERT_TRUE(s.ok()) << s.ToString();

    ReOpenDB();
    ASSERT_EQ(db_->PersistApplied(), 0U);
    Check({}, {});
}

TEST_F(DBTest, WriteBatch) {
    DisableCheckpoint();

    std::map<std::string, std::string> def_kvs;
    std::map<std::string, std::string> txn_kvs;
    std::map<std::string, std::string> *kvs = nullptr;
    auto batch = db_->NewWriteBatch();
    for (int j = 0; j < 1000; ++j) {
        auto cf = randomInt() % 2 == 0 ? CFType::kData : CFType::kTxn;
        kvs = cf == CFType::kData ? &def_kvs : &txn_kvs;
        auto put_op = randomInt() % 3 > 0;
        if (put_op) {
            std::string key = jimkv::randomString(32, 1024);
            std::string value = jimkv::randomString(64);
            batch->Put(cf, key, value);
            kvs->emplace(key, value);
        } else { // delete
            if (kvs->empty()) {
                continue;
            }
            auto it = kvs->begin();
            std::advance(it, randomInt() % kvs->size());
            batch->Delete(cf, it->first);
            kvs->erase(it);
        }
    }
    auto s = db_->Write(batch.get(), ++index_);
    ASSERT_TRUE(s.ok()) << s.ToString();

    std::cout << "default size: " << def_kvs.size() << ", txn size: " << txn_kvs.size() << std::endl;

    Check(def_kvs, txn_kvs);
}

TEST_F(DBTest, Checkpoint) {
    DisableCheckpoint();

    std::map<std::string, std::string> def_kvs;
    std::map<std::string, std::string> txn_kvs;
    std::map<std::string, std::string> *kvs = nullptr;
    for (int j = 0; j < 5000; ++j) {
        auto cf = randomInt() % 2 == 0 ? CFType::kData : CFType::kTxn;
        kvs = cf == CFType::kData ? &def_kvs : &txn_kvs;
        std::string key = jimkv::randomString(32, 1024);
        std::string value = jimkv::randomString(64);
        auto s = db_->Put(cf, key, value, ++index_);
        ASSERT_TRUE(s.ok()) << s.ToString();
        kvs->emplace(key, value);
        if (j % 1000 == 500) {
            auto s = RunCheckpoint();
            ASSERT_TRUE(s.ok()) << s.ToString();
            ASSERT_EQ(db_->PersistApplied(), index_);
        }
    }
    Check(def_kvs, txn_kvs);

    auto s = RunCheckpoint();
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(db_->PersistApplied(), index_);

    Check(def_kvs, txn_kvs);

    ReOpenDB();
    ASSERT_EQ(db_->PersistApplied(), index_);
    Check(def_kvs, txn_kvs);
}

TEST_F(DBTest, Snapshot) {
    // put some data, to verify snapshot can clear db
    db_->Put(CFType::kData, "123", "456", ++index_);
    db_->Put(CFType::kTxn, "123", "456", ++index_);

    std::map<std::string, std::string> def_kvs;
    std::map<std::string, std::string> txn_kvs;
    std::map<std::string, std::string> *kvs = nullptr;
    std::vector<std::unique_ptr<WriteBatch>> batchs;
    batchs.push_back(db_->NewWriteBatch());
    for (int j = 0; j < 1000; ++j) {
        auto cf = randomInt() % 2 == 0 ? CFType::kData : CFType::kTxn;
        kvs = cf == CFType::kData ? &def_kvs : &txn_kvs;
        std::string key = jimkv::randomString(32, 1024);
        std::string value = jimkv::randomString(64);
        kvs->emplace(key, value);
        batchs.back()->Put(cf, key, value);
        if (j % 1000 == 500) {
            batchs.push_back(db_->NewWriteBatch());
        }
    }

    auto snap_index = randomInt();
    auto s = db_->ApplySnapshotStart(snap_index);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(db_->PersistApplied(), 0U);
    // should clear all db data
    Check({}, {});

    for (auto& batch : batchs) {
        s = db_->ApplySnapshotData(batch.get());
        ASSERT_TRUE(s.ok()) << s.ToString();
    }
    s = db_->ApplySnapshotFinish(snap_index);
    ASSERT_TRUE(s.ok()) << s.ToString();

    ASSERT_EQ(db_->PersistApplied(), static_cast<uint64_t>(snap_index));
    Check(def_kvs, txn_kvs);

    ReOpenDB();
    ASSERT_EQ(db_->PersistApplied(), static_cast<uint64_t>(snap_index));
    Check(def_kvs, txn_kvs);
}

TEST_F(DBTest, Split) {
    std::map<std::string, std::string> def_kvs;
    std::map<std::string, std::string> txn_kvs;
    std::map<std::string, std::string> left_def_kvs;
    std::map<std::string, std::string> left_txn_kvs;
    std::map<std::string, std::string> *kvs = nullptr;
    std::string split_key;
    for (int j = 0; j < 2000; ++j) {
        char key[64] = {'\0'};
        snprintf(key, 64, "%010d", j);
        std::string value = jimkv::randomString(64);

        auto s = db_->Put(CFType::kData, key, value, ++index_);
        ASSERT_TRUE(s.ok()) << s.ToString();
        s = db_->Put(CFType::kTxn, key, value, ++index_);
        ASSERT_TRUE(s.ok()) << s.ToString();

        if (j == 1000) {
            split_key = key;
        }
        if (j >= 1000) {
            def_kvs.emplace(key, value);
            txn_kvs.emplace(key, value);
        } else {
            left_def_kvs.emplace(key, value);
            left_txn_kvs.emplace(key, value);
        }
    }

    std::unique_ptr<DB> split_db;
    auto split_index = ++index_;
    auto s = db_->SplitDB(2, split_key, split_index, split_db);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(split_db->PersistApplied(), split_index);
    CheckDB(split_db.get(), split_key, end_key_, def_kvs, txn_kvs);

    CheckDB(db_.get(), start_key_, split_key, left_def_kvs, left_txn_kvs);

    // close and reopen split db
    s = split_db->Close();
    ASSERT_TRUE(s.ok()) << s.ToString();
    split_db.reset();
    s = manager_->CreateDB(2, split_key, end_key_, split_db);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(split_db->PersistApplied(), split_index);
    CheckDB(split_db.get(), split_key, end_key_, def_kvs, txn_kvs);
}

}

