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

#include <gtest/gtest.h>
#include <functional>

#include "base/status.h"
#include "storage/store.h"
#include "db/db_manager.h"
#include "request_builder.h"

namespace jim {
namespace test {
namespace helper {

using namespace ::jim::ds::db;
using namespace ::jim::ds::storage;

class StoreTestFixture : public ::testing::Test {
public:
    explicit StoreTestFixture(std::unique_ptr<Table> t);

protected:
    void SetUp() override;
    void TearDown() override;

protected:
    // sql test methods:
    Status testSelect(const std::function<void(SelectRequestBuilder&)>& build_func,
                      const std::vector<std::vector<std::string>>& expected_rows);

    // sql test flows
    Status testSelectFlow(const std::function<void(SelectFlowRequestBuilder&)>& build_func,
                      const std::vector<std::vector<std::string>>& expected_rows);

    Status testInsert(const std::vector<std::vector<std::string>> &rows, uint64_t *insert_bytes= 0);

    Status testDelete(const std::function<void(DeleteRequestBuilder&)>& build_func,
                      uint64_t expected_affected);

    Status putTxn(const std::string& key, const dspb::TxnValue& value);

    void testTxn(std::vector<dspb::TxnIntent>& intents);

    Store *getStore() const { return store_; }

protected:
    uint64_t statSizeUntil(const std::string& end);

protected:
    std::unique_ptr<Table> table_;
    basepb::Range meta_;
    Store* store_ = nullptr;

private:
    uint64_t raft_index_ = 0;
    std::string tmp_dir_;
    std::unique_ptr<DBManager> db_manager_ = nullptr;
};

} /* namespace helper */
} /* namespace test */
} /* namespace jim */
