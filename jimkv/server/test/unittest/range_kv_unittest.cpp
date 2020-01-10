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

#include "base/status.h"
#include "base/fs_util.h"
#include "common/server_config.h"
#include "server/run_status.h"
#include "storage/store.h"

#include "helper/table.h"
#include "helper/mock/raft_mock.h"
#include "helper/mock/rpc_request_mock.h"
#include "helper/helper_util.h"

#include "masstree-beta/config.h"
#include "masstree-beta/string.hh"

#include "helper/cpp_permission.h"
#include "range/range.h"
#include "server/range_tree.h"
#include "server/range_server.h"

int main(int argc, char *argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

using namespace jimkv::test::helper;
using namespace jimkv::test::mock;
using namespace jimkv::ds;
using namespace jimkv::ds::storage;

class KvTest : public ::testing::Test {
protected:
    void SetUp() override {
        data_path_ = "/tmp/jimkv_ds_store_test_";
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

    Status testKV(const dspb::RangeRequest& req, dspb::RangeResponse& resp) {
        auto rpc = NewMockRPCRequest(req, dspb::kFuncRangeRequest);
        range_server_->DealTask(std::move(rpc.first));
        return rpc.second->Get(resp);
    }

protected:
    server::ContextServer *context_;
    std::string data_path_;
    server::RangeServer *range_server_;
};

basepb::Range *genRange1() {
    auto meta = new basepb::Range;

    meta->set_id(1);
    meta->set_start_key("01003");
    meta->set_end_key("01004");
    meta->mutable_range_epoch()->set_conf_ver(1);
    meta->mutable_range_epoch()->set_version(1);

    meta->set_table_id(1);

    auto peer = meta->add_peers();
    peer->set_id(1);
    peer->set_node_id(1);

    peer = meta->add_peers();
    peer->set_id(2);
    peer->set_node_id(2);

    auto pks = CreateAccountTable()->GetPKs();
    for (const auto& pk : pks) {
        auto p = meta->mutable_key_schema()->add_key_cols();

        p->set_id(pk.id());
        p->set_type(pk.sql_type().type());
        p->set_unsigned_(pk.sql_type().unsigned_());
    }

    return meta;
}

basepb::Range *genRange2() {
    auto meta = new basepb::Range;

    meta->set_id(2);
    meta->set_start_key("01004");
    meta->set_end_key("01005");
    meta->mutable_range_epoch()->set_conf_ver(1);
    meta->mutable_range_epoch()->set_version(1);

    meta->set_table_id(1);

    auto peer = meta->add_peers();
    peer->set_id(1);
    peer->set_node_id(1);

    auto pks = CreateAccountTable()->GetPKs();
    for (const auto& pk : pks) {
        auto p = meta->mutable_key_schema()->add_key_cols();

        p->set_id(pk.id());
        p->set_type(pk.sql_type().type());
        p->set_unsigned_(pk.sql_type().unsigned_());
    }

    return meta;
}

TEST_F(KvTest, Kv) {
    {
        // begin test create range 1
        dspb::SchRequest req;
        req.mutable_create_range()->set_allocated_range(genRange1());

        auto rpc = NewMockRPCRequest(req);
        range_server_->dispatchSchedule(rpc.first);
        ASSERT_FALSE(range_server_->ranges_.Empty());

        ASSERT_TRUE(range_server_->Find(1) != nullptr);

        std::vector<basepb::Range> metas;
        auto ret = range_server_->meta_store_->GetAllRange(&metas);

        ASSERT_TRUE(metas.size() == 1) << metas.size();
        // end test create range
    }

    {
        // begin test create range 2
        dspb::SchRequest req;
        req.mutable_create_range()->set_allocated_range(genRange2());

        auto rpc = NewMockRPCRequest(req);
        range_server_->dispatchSchedule(rpc.first);

        ASSERT_FALSE(range_server_->ranges_.Empty());
        ASSERT_TRUE(range_server_->Find(2) != nullptr);

        std::vector<basepb::Range> metas;
        auto ret = range_server_->meta_store_->GetAllRange(&metas);
        ASSERT_TRUE(metas.size() == 2) << metas.size();
        // end test create range
    }

    {
        // begin test kv_put (no leader)
        dspb::RangeRequest req;
        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);
        req.mutable_kv_put()->set_key("01003001");
        req.mutable_kv_put()->set_value("01003001:value");

        dspb::RangeResponse resp;
        auto s = testKV(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_not_leader());
        ASSERT_FALSE(resp.header().error().not_leader().has_leader());
        // end test kv_put
    }

    {
        // begin test kv_put (not leader)

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_.Find(1)->raft_.get());
        raft->SetLeaderTerm(2, 1);
        range_server_->ranges_.Find(1)->is_leader_ = false;

        dspb::RangeRequest req;
        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);
        req.mutable_kv_put()->set_key("01003001");
        req.mutable_kv_put()->set_value("01003001:value");

        dspb::RangeResponse resp;
        auto s = testKV(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_not_leader());
        ASSERT_TRUE(resp.header().error().not_leader().has_leader());
        ASSERT_TRUE(resp.header().error().not_leader().leader().node_id() == 2);

        // end test kv_put
    }

    {
        // begin test kv_put (out of bound)

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_.Find(1)->raft_.get());
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_.Find(1)->is_leader_ = true;

        dspb::RangeRequest req;
        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);
        req.mutable_kv_put()->set_key("01004001");
        req.mutable_kv_put()->set_value("01004001:value");

        dspb::RangeResponse resp;
        auto s = testKV(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_out_of_bound());
        ASSERT_EQ(resp.header().error().out_of_bound().range_id(), 1U);
        ASSERT_EQ(resp.header().error().out_of_bound().key(), "01004001");
        ASSERT_EQ(resp.header().error().out_of_bound().range_start(), "01003");
        ASSERT_EQ(resp.header().error().out_of_bound().range_limit(), "01004");

        // end test kv_put
    }

    {
        // begin test kv_put (stale epoch)

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_.Find(1)->raft_.get());
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_.Find(1)->is_leader_ = true;

        dspb::RangeRequest req;
        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(2);
        req.mutable_kv_put()->set_key("01004001");
        req.mutable_kv_put()->set_value("01004001:value");

        dspb::RangeResponse resp;
        auto s = testKV(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_stale_epoch());
        ASSERT_TRUE(resp.header().error().stale_epoch().has_old_range());
        const auto& old_epoch = resp.header().error().stale_epoch().old_range();
        ASSERT_EQ(old_epoch.id(), 1U);
        ASSERT_EQ(old_epoch.range_epoch().version(), 1U);
        ASSERT_EQ(old_epoch.range_epoch().conf_ver(), 1U);

        // end test kv_put
    }

    {
        // begin test kv_put (key empty)

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_.Find(1)->raft_.get());
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_.Find(1)->is_leader_ = true;

        dspb::RangeRequest req;
        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);
        req.mutable_kv_put()->set_key("");
        req.mutable_kv_put()->set_value("01003001:value");

        dspb::RangeResponse resp;
        auto s = testKV(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_out_of_bound());

        // end test kv_put
    }

    {
        // begin test kv_put (ok)

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_.Find(1)->raft_.get());
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_.Find(1)->is_leader_ = true;

        dspb::RangeRequest req;
        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);
        req.mutable_kv_put()->set_key("01003001");
        req.mutable_kv_put()->set_value("01003001:value");

        dspb::RangeResponse resp;
        auto s = testKV(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_FALSE(resp.header().has_error());

        // end test kv_put
    }

    {
        // begin test kv_put (ok)

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_.Find(2)->raft_.get());
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_.Find(2)->is_leader_ = true;

        dspb::RangeRequest req;
        req.mutable_header()->set_range_id(2);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);
        req.mutable_kv_put()->set_key("01004001");
        req.mutable_kv_put()->set_value("01004001:value");

        dspb::RangeResponse resp;
        auto s = testKV(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_FALSE(resp.header().has_error());

        // end test kv_put
    }

    {
        // begin test kv_get(ok)
        dspb::RangeRequest req;
        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);
        req.mutable_kv_get()->set_key("01003001");

        dspb::RangeResponse resp;
        auto s = testKV(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_FALSE(resp.header().has_error());
        ASSERT_TRUE(resp.kv_get().value() == "01003001:value");

        // end test kv_get
    }

    {
        // begin test kv_get (ok)
        dspb::RangeRequest req;
        req.mutable_header()->set_range_id(2);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);
        req.mutable_kv_get()->set_key("01004001");

        dspb::RangeResponse resp;
        auto s = testKV(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_FALSE(resp.header().has_error()) << resp.header().error().DebugString();
        ASSERT_TRUE(resp.kv_get().value() == "01004001:value");

        // end test kv_get
    }

    {
        // begin test kv_get (key empty)
        dspb::RangeRequest req;
        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);
        req.mutable_kv_get()->set_key("");

        dspb::RangeResponse resp;
        auto s = testKV(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_out_of_bound());
        // end test kv_get
    }

    {
        // begin test kv_get (no leader)

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_.Find(1)->raft_.get());
        raft->SetLeaderTerm(0, 2);
        range_server_->ranges_.Find(1)->is_leader_ = false;

        dspb::RangeRequest req;
        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);
        req.mutable_kv_get()->set_key("01003001");

        dspb::RangeResponse resp;
        auto s = testKV(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_not_leader());
        ASSERT_FALSE(resp.header().error().not_leader().has_leader());

        // end test kv_get
    }

    {
        // begin test kv_get (not leader)

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_.Find(1)->raft_.get());
        raft->SetLeaderTerm(2, 2);
        range_server_->ranges_.Find(1)->is_leader_ = true;

        dspb::RangeRequest req;
        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);
        req.mutable_kv_get()->set_key("01003001");

        dspb::RangeResponse resp;
        auto s = testKV(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_not_leader());
        ASSERT_TRUE(resp.header().error().not_leader().has_leader());
        ASSERT_TRUE(resp.header().error().not_leader().leader().node_id() == 2);

        // end test kv_get
    }

    {
        // begin test kv_get (out of bound)

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_.Find(1)->raft_.get());
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_.Find(1)->is_leader_ = true;

        dspb::RangeRequest req;
        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);
        req.mutable_kv_get()->set_key("01004001");

        dspb::RangeResponse resp;
        auto s = testKV(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_out_of_bound());

        // end test kv_get
    }

    {
        // begin test kv_delete (key empty)
        dspb::RangeRequest req;
        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);
        req.mutable_kv_delete()->set_key("");

        dspb::RangeResponse resp;
        auto s = testKV(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_out_of_bound());
        // end test kv_delete
    }

    {
        // begin test kv_delete (no leader)

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_.Find(1)->raft_.get());
        raft->SetLeaderTerm(0, 1);
        range_server_->ranges_.Find(1)->is_leader_ = false;

        dspb::RangeRequest req;
        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);
        req.mutable_kv_delete()->set_key("01003001");

        dspb::RangeResponse resp;
        auto s = testKV(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_not_leader());
        ASSERT_FALSE(resp.header().error().not_leader().has_leader());

        // end test kv_delete
    }

    {
        // begin test kv_delete (not leader)

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_.Find(1)->raft_.get());
        raft->SetLeaderTerm(2, 1);
        range_server_->ranges_.Find(1)->is_leader_ = true;

        dspb::RangeRequest req;
        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);
        req.mutable_kv_delete()->set_key("01003001");

        dspb::RangeResponse resp;
        auto s = testKV(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_not_leader());
        ASSERT_TRUE(resp.header().error().not_leader().has_leader());
        ASSERT_TRUE(resp.header().error().not_leader().leader().node_id() == 2);

        // end test kv_delete
    }

    {
        // begin test kv_delete (out of bound)

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_.Find(1)->raft_.get());
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_.Find(1)->is_leader_ = true;

        dspb::RangeRequest req;
        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);
        req.mutable_kv_delete()->set_key("01004001");

        dspb::RangeResponse resp;
        auto s = testKV(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_out_of_bound());

        // end test kv_delete
    }

    {
        // begin test kv_get( ensure not to be deleted )

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_.Find(1)->raft_.get());
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_.Find(1)->is_leader_ = true;

        dspb::RangeRequest req;
        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);
        req.mutable_kv_get()->set_key("01003001");

        dspb::RangeResponse resp;
        auto s = testKV(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_FALSE(resp.header().has_error());
        ASSERT_TRUE(resp.kv_get().value() == "01003001:value");

        // end test kv_get
    }

    {
        // begin test kv_get( ensure not to be deleted )

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_.Find(2)->raft_.get());
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_.Find(2)->is_leader_ = true;

        dspb::RangeRequest req;
        req.mutable_header()->set_range_id(2);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);
        req.mutable_kv_get()->set_key("01004001");

        dspb::RangeResponse resp;
        auto s = testKV(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_FALSE(resp.header().has_error());
        ASSERT_TRUE(resp.kv_get().value() == "01004001:value");

        // end test kv_get
    }

    {
        // begin test kv_delete( ok )

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_.Find(1)->raft_.get());
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_.Find(1)->is_leader_ = true;

        dspb::RangeRequest req;
        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);
        req.mutable_kv_delete()->set_key("01003001");

        dspb::RangeResponse resp;
        auto s = testKV(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_FALSE(resp.header().has_error());

        // end test kv_delete
    }

    {
        // begin test kv_get(ensure be deleted)
        dspb::RangeRequest req;
        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);
        req.mutable_kv_get()->set_key("01003001");

        dspb::RangeResponse resp;
        auto s = testKV(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_FALSE(resp.header().has_error());
        ASSERT_TRUE(resp.kv_get().value().empty());

        // end test kv_get
    }

    {
        // begin test delete range (range 1)
        dspb::SchRequest req;
        req.mutable_delete_range()->set_range_id(1);

        auto rpc = NewMockRPCRequest(req);
        range_server_->dispatchSchedule(rpc.first);

        ASSERT_TRUE(range_server_->Find(1) == nullptr);

        dspb::SchResponse resp;
        auto s = rpc.second->Get(resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_FALSE(resp.header().has_error());

        // test meta_store
        std::vector<basepb::Range> metas;
        auto ret = range_server_->meta_store_->GetAllRange(&metas);

        ASSERT_TRUE(metas.size() == 1) << metas.size();
        // end test delete range
    }

    {
        // begin test delete range (range 2)
        dspb::SchRequest req;
        req.mutable_delete_range()->set_range_id(2);
        auto rpc = NewMockRPCRequest(req);
        range_server_->dispatchSchedule(rpc.first);

        ASSERT_TRUE(range_server_->Find(2) == nullptr);

        dspb::SchResponse resp;
        auto s = rpc.second->Get(resp);
        ASSERT_TRUE(s.ok()) << s.ToString();

        // test meta_store
        std::vector<basepb::Range> metas;
        auto ret = range_server_->meta_store_->GetAllRange(&metas);
        ASSERT_TRUE(metas.size() == 0) << metas.size();
        // end test delete range
    }
}
