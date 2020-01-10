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

#include "range_test_fixture.h"

#include "storage/meta_store.h"
#include "base/fs_util.h"

#include "helper_util.h"
#include "helper/table.h"
#include "helper/mock/raft_mock.h"
#include "mock/rpc_request_mock.h"

namespace jimkv {
namespace test {
namespace helper {

using namespace dspb;
using namespace jimkv;
using namespace jimkv::ds;
using namespace google::protobuf;

void RangeTestFixture::SetUp() {
    InitLog();

    table_ = CreateAccountTable();

    context_.reset(new mock::RangeContextMock);
    auto s = context_->Init();
    ASSERT_TRUE(s.ok()) << s.ToString();

    auto meta = MakeRangeMeta(table_.get(), 3);
    s = context_->CreateRange(meta, 0, 0, &range_);
    ASSERT_TRUE(s.ok()) << s.ToString();
}

void RangeTestFixture::TearDown() {
    range_.reset();
    if (context_) {
        context_->Destroy();
    }
}

void RangeTestFixture::SetLeader(uint64_t leader) {
    ++term_;
    auto r = std::static_pointer_cast<RaftMock>(range_->raft_);
    r->SetLeaderTerm(leader, term_);
    range_->is_leader_ = (leader == range_->node_id_);
}

void RangeTestFixture::MakeHeader(RangeRequest_Header *header, uint64_t version, uint64_t conf_ver) {
    header->set_range_id(range_->id_);
    header->mutable_range_epoch()->set_version(version == 0 ? range_->meta_.GetVersion() : version);
    header->mutable_range_epoch()->set_conf_ver(conf_ver == 0 ? range_->meta_.GetConfVer() : conf_ver);
}

void RangeTestFixture::Process(const dspb::RangeRequest& req, dspb::RangeResponse* resp) {
    auto req_copy = req;
    auto rpc = mock::NewMockRPCRequest(req_copy);
    range_->Dispatch(std::move(rpc.first), req_copy);
    auto ret = rpc.second->Get(*resp);
    if (!ret.ok()) {
        throw std::runtime_error("get process response failed: " + ret.ToString());
    }
}

Status RangeTestFixture::Split() {
    // for testing, only split once
    if (range_->split_range_id_ != 0) {
        return Status(Status::kDuplicate, "split", "already take place");
    }

    mspb::AskSplitResponse resp;

    std::string split_key;
    EncodeKeyPrefix(&split_key, table_->GetID());
    split_key.push_back('\x70');
    resp.set_split_key(split_key);

    range_->split_range_id_ = 2;
    resp.set_new_range_id(range_->split_range_id_);
    range_->meta_.Get(resp.mutable_range());
    for (const auto &peer : resp.range().peers()) {
        resp.add_new_peer_ids(peer.id() + 1);
    }

    auto old_end_key = range_->meta_.GetEndKey();
    auto old_ver = range_->meta_.GetVersion();

    dspb::SplitCommand_StatsInfo st_info;
    st_info.set_left_count(0);
    st_info.set_right_count(0);
    st_info.set_left_size(0);
    st_info.set_right_size(0);

    range_->startSplit(resp, std::move(st_info));

    // check version
    if (range_->meta_.GetVersion() != old_ver + 1) {
        return Status(Status::kUnexpected, "version", std::to_string(range_->meta_.GetVersion()));
    }
    // end_key
    if (range_->meta_.GetEndKey() != split_key) {
        return Status(Status::kUnexpected, "end key", range_->meta_.GetEndKey());
    }
    // store end key
    if (range_->store_->GetEndKey() != split_key) {
        return Status(Status::kUnexpected, "store end key", range_->store_->GetEndKey());
    }
    // check new range exist
    auto split_range = context_->FindRange(range_->split_range_id_);
    if (split_range == nullptr) {
        return Status(Status::kNotFound, "new split range", "");
    }
    // check new range meta
    auto split_meta = split_range->GetMeta();
    if (split_meta.id() != range_->split_range_id_) {
        return Status(Status::kUnexpected, "split range id", split_meta.DebugString());
    }
    if (split_meta.end_key() != old_end_key) {
        return Status(Status::kUnexpected, "split end key", split_meta.DebugString());
    }
    if (split_meta.start_key() != split_key) {
        return Status(Status::kUnexpected, "split start key", split_meta.DebugString());
    }
    // check range peer
    auto meta = range_->meta_.Get();
    if (split_meta.peers_size() != meta.peers_size()) {
        return Status(Status::kUnexpected, "split peer size", split_meta.DebugString());
    }
    for (int i = 0; i < split_meta.peers_size(); ++i) {
        const auto &old_peer = meta.peers(i);
        const auto &new_peer = split_meta.peers(i);
        if (old_peer.id() + 1 != new_peer.id()) {
            return Status(Status::kUnexpected, "split peer id", split_meta.DebugString());
        }
        if (old_peer.node_id() != new_peer.node_id()) {
            return Status(Status::kUnexpected, "split peer node id", split_meta.DebugString());
        }
        if (new_peer.type() != basepb::PeerType_Normal) {
            return Status(Status::kUnexpected, "split peer type", split_meta.DebugString());
        }
    }
    // check pk
    if (split_meta.key_schema().key_cols_size() != meta.key_schema().key_cols_size()) {
        return Status(Status::kUnexpected, "split pk size", split_meta.key_schema().DebugString());
    }
    for (int i = 0; i < split_meta.key_schema().key_cols_size(); ++i) {
        const auto &old_pk = meta.key_schema().key_cols(i);
        const auto &new_pk = split_meta.key_schema().key_cols(i);
        if (old_pk.ShortDebugString() != new_pk.ShortDebugString()) {
            return Status(Status::kUnexpected, "pk", split_meta.key_schema().DebugString());
        }
    }

//    // check meta store
//    std::vector<basepb::Range> metas;
//    auto s = context_->MetaStore()->GetAllRange(&metas);
//    if (!s.ok()) {
//        return s;
//    }
//    if (metas.size() != 2) {
//        return Status(Status::kUnexpected, "meta storage size", std::to_string(metas.size()));
//    }
//    if (metas[0].ShortDebugString() != meta.ShortDebugString()) {
//        return Status(Status::kUnexpected, "meta storage", metas[0].ShortDebugString());
//    }
//    if (metas[1].ShortDebugString() != split_meta.ShortDebugString()) {
//        return Status(Status::kUnexpected, "meta storage", metas[1].ShortDebugString());
//    }

//    std::cout << split_meta.DebugString() << std::endl;

    return Status::OK();
}

} /* namespace helper */
} /* namespace test */
} /* namespace jimkv */
