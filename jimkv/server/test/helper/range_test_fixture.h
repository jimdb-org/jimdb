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

#include <gtest/gtest.h>
#include "range/range.h"
#include "range/context.h"
#include "storage/meta_store.h"

#include "mock/range_context_mock.h"

#include "table.h"

namespace jimkv {
namespace test {
namespace helper {

// for test only
using namespace jimkv;
using namespace jimkv::ds;
using namespace jimkv::ds::range;

// construc a raft cluster with three members(1, 2, 3)
// local node id is 1
class RangeTestFixture : public ::testing::Test {
protected:
    void SetUp() override;
    void TearDown() override;

protected:
    uint64_t GetNodeID() const { return range_->node_id_; }
    uint64_t GetRangeID() const { return range_->id_; }
    uint64_t GetSplitRangeID() const { return range_->split_range_id_; }

    void SetLeader(uint64_t leader);

    // version=0 means to use current version
    void MakeHeader(dspb::RangeRequest_Header *header, uint64_t version = 0, uint64_t conf_ver = 0);
    void Process(const dspb::RangeRequest& req, dspb::RangeResponse* resp);

    Status Split();

protected:
    std::unique_ptr<mock::RangeContextMock> context_;
    std::unique_ptr<Table> table_;
    std::shared_ptr<ds::range::Range> range_;
    uint64_t term_ = 0;
};

} /* namespace helper */
} /* namespace test */
} /* namespace jimkv */
