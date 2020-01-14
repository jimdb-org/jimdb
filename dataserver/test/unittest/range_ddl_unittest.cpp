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

#include "base/status.h"
#include "base/util.h"
#include "base/fs_util.h"
#include "common/server_config.h"

#include "server/run_status.h"
#include "storage/store.h"
#include "dspb/schedule.pb.h"

#include "helper/table.h"
#include "helper/mock/raft_mock.h"
#include "helper/mock/rpc_request_mock.h"
#include "helper/helper_util.h"
#include "server/range_tree.h"

#include "helper/cpp_permission.h"
#include "server/range_server.h"

int main(int argc, char *argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

using namespace jim::test::helper;
using namespace jim::test::mock;
using namespace jim::ds;
using namespace jim::ds::storage;

class DdlTest : public ::testing::Test {
protected:
    void SetUp() override {
        data_path_ = "/tmp/jim_ds_store_test_";
        data_path_ += std::to_string(NowMilliSeconds());

        ds_config.engine_type = EngineType::kMassTree;
        ds_config.masstree_config.data_path = data_path_ + "/mass-data";
        ds_config.meta_path = data_path_ + "/meta-data";

        ds_config.range_config.recover_concurrency = 1;

        range_server_ = new server::RangeServer;

        context_ = new server::ContextServer;

        context_->node_id = 1;
        context_->range_server = range_server_;
        context_->raft_server = new RaftServerMock;
        context_->run_status = new server::RunStatus;

        range_server_->Init(context_);
    }

    void TearDown() override {
        if (context_->range_server) {
            context_->range_server->Stop();
            delete context_->range_server;
        }
        delete context_->raft_server;
        delete context_->run_status;
        delete context_;
        if (!data_path_.empty()) {
            RemoveDirAll(data_path_.c_str());
        }
    }

    void TestCreate(basepb::Range&& meta, dspb::SchResponse& resp) {
        dspb::SchRequest req;
        req.mutable_create_range()->set_allocated_range(new basepb::Range(std::move(meta)));
        auto rpc = NewMockRPCRequest(req, dspb::kFuncSchedule);
        range_server_->dispatchSchedule(rpc.first);
        auto s = rpc.second->Get(resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    void TestDelete(uint64_t range_id, dspb::SchResponse& resp) {
        dspb::SchRequest req;
        req.mutable_delete_range()->set_range_id(range_id);
        auto rpc = NewMockRPCRequest(req, dspb::kFuncSchedule);
        range_server_->dispatchSchedule(rpc.first);
        dspb::SchResponse sch_resp;
        auto s = rpc.second->Get(resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

protected:
    server::ContextServer *context_;
    std::string data_path_;
    server::RangeServer *range_server_;
};

basepb::Range genRange() {
    basepb::Range meta;

    meta.set_id(1);
    meta.set_start_key("01003");
    meta.set_end_key("01004");
    meta.mutable_range_epoch()->set_conf_ver(1);
    meta.mutable_range_epoch()->set_version(1);
    meta.set_table_id(1);

    auto peer = meta.add_peers();
    peer->set_id(1);
    peer->set_node_id(1);

    auto pks = CreateAccountTable()->GetPKs();
    for (const auto& pk : pks) {
        auto p = meta.mutable_key_schema()->add_key_cols();

        p->set_id(pk.id());
        p->set_type(pk.sql_type().type());
        p->set_unsigned_(pk.sql_type().unsigned_());

    }

    return meta;
}

TEST_F(DdlTest, Ddl) {
    {
        dspb::SchResponse resp;
        TestCreate(genRange(), resp);
        ASSERT_FALSE(resp.has_header() && resp.header().has_error()) << resp.ShortDebugString();

        ASSERT_FALSE(range_server_->ranges_.Empty());
        ASSERT_TRUE(range_server_->Find(1) != nullptr);

        std::vector<basepb::Range> metas;
        auto ret = range_server_->meta_store_->GetAllRange(&metas);
        ASSERT_TRUE(metas.size() == 1) << metas.size();
        // end test create range
    }

    {
        // begin test create range (repeat)
        dspb::SchResponse resp;
        TestCreate(genRange(), resp);
        ASSERT_FALSE(resp.has_header() && resp.header().has_error()) << resp.ShortDebugString();

        ASSERT_FALSE(range_server_->ranges_.Empty());
        ASSERT_TRUE(range_server_->Find(1) != nullptr);
        // test meta_store
        std::vector<basepb::Range> metas;
        auto ret = range_server_->meta_store_->GetAllRange(&metas);
        ASSERT_TRUE(metas.size() == 1) << metas.size();

        // end test create range
    }

    {
        // begin test create range (repeat but epoch stale)
        auto meta = genRange();
        meta.mutable_range_epoch()->set_version(2);
        dspb::SchResponse resp;
        TestCreate(std::move(meta), resp);
        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_stale_epoch());

        ASSERT_FALSE(range_server_->ranges_.Empty());
        ASSERT_TRUE(range_server_->Find(1) != nullptr);

        // test meta_store
        std::vector<basepb::Range> metas;
        auto ret = range_server_->meta_store_->GetAllRange(&metas);

        ASSERT_TRUE(metas.size() == 1) << metas.size();
        // end test create range
    }

    {
        dspb::SchResponse resp;
        TestDelete(1, resp);

        ASSERT_TRUE(range_server_->ranges_.Empty());
        ASSERT_TRUE(range_server_->Find(1) == nullptr);

        ASSERT_FALSE(resp.header().has_error());

        // test meta_store
        std::vector<basepb::Range> metas;
        auto ret = range_server_->meta_store_->GetAllRange(&metas);

        ASSERT_TRUE(metas.size() == 0) << metas.size();
        // end test delete range
    }

    {
        // begin test delete range (not exist)
        dspb::SchResponse resp;
        TestDelete(1, resp);
        ASSERT_TRUE(!resp.header().has_error() || resp.header().error().has_range_not_found());
        // end test delete range
    }
}
