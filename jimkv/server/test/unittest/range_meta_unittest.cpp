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

#include "base/util.h"
#include "range/meta_keeper.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {

using namespace jimkv;
using jimkv::ds::range::MetaKeeper;

static basepb::Range randMeta() {
    static uint64_t inc_id  = 0;

    basepb::Range r;
    r.set_id(++inc_id);
    r.set_table_id(randomInt());

    r.set_start_key("b");
    r.set_end_key("e");

    r.mutable_range_epoch()->set_version(5);
    r.mutable_range_epoch()->set_conf_ver(7);

    for (unsigned i = 1; i <= 3; ++i) {
        auto p = r.add_peers();
        p->set_type(i == 3 ? basepb::PeerType_Learner : basepb::PeerType_Normal);
        p->set_id(i);
        p->set_node_id(100 + i);
    }
    return r;
}

TEST(RangeMetaKeeper, Basic) {
    auto meta = randMeta();
    MetaKeeper keeper(meta, nullptr);

    // get
    ASSERT_EQ(keeper.Get().ShortDebugString(), meta.ShortDebugString());
    basepb::Range meta2;
    keeper.Get(&meta2);
    ASSERT_EQ(meta2.ShortDebugString(), meta.ShortDebugString());

    // set new
    meta = randMeta();
    keeper.Set(meta);
    ASSERT_EQ(keeper.Get().ShortDebugString(), meta.ShortDebugString());

    ASSERT_EQ(keeper.GetTableID(), meta.table_id());
    ASSERT_EQ(keeper.GetStartKey(), meta.start_key());
    ASSERT_EQ(keeper.GetEndKey(), meta.end_key());

    // versions
    ASSERT_EQ(keeper.GetVersion(), meta.range_epoch().version());
    ASSERT_EQ(keeper.GetConfVer(), meta.range_epoch().conf_ver());
    basepb::RangeEpoch epoch;
    keeper.GetEpoch(&epoch);
    ASSERT_EQ(epoch.version(), meta.range_epoch().version());
    ASSERT_EQ(epoch.conf_ver(), meta.range_epoch().conf_ver());
}

TEST(RangeMetaKeeper, Peer) {
    auto meta = randMeta();
    MetaKeeper keeper(meta, nullptr);

    // get all peers
    auto peers = keeper.GetAllPeers();
    ASSERT_EQ(peers.size(), static_cast<unsigned>(meta.peers().size()));
    for (std::size_t i = 0; i < peers.size(); ++i) {
        ASSERT_EQ(peers[i].ShortDebugString(), meta.peers(i).ShortDebugString());
    }

    // find peer
    for (std::size_t i = 1; i <= 3; ++i) {
        basepb::Peer peer;
        ASSERT_TRUE(keeper.FindPeer(i, &peer));
        ASSERT_EQ(peer.ShortDebugString(), meta.peers(i-1).ShortDebugString());

        ASSERT_TRUE(keeper.FindPeerByNodeID(i+100));
        ASSERT_EQ(peer.ShortDebugString(), meta.peers(i-1).ShortDebugString());
    }

    // add 4
    basepb::Peer add_peer;
    add_peer.set_type(basepb::PeerType_Normal);
    add_peer.set_id(4);
    add_peer.set_node_id(104);
    // add incorrect conf ver
    auto s = keeper.AddPeer(add_peer, meta.range_epoch().conf_ver() - 1);
    ASSERT_EQ(s.code(), Status::kStaleEpoch);
    s = keeper.AddPeer(add_peer, meta.range_epoch().conf_ver() + 1);
    ASSERT_EQ(s.code(), Status::kInvalidArgument);
    // add exist
    add_peer.set_id(1); // existed peer id
    s = keeper.AddPeer(add_peer, meta.range_epoch().conf_ver());
    ASSERT_EQ(s.code(), Status::kExisted);
    add_peer.set_id(4);
    add_peer.set_node_id(101); // existed node id
    s = keeper.AddPeer(add_peer, meta.range_epoch().conf_ver());
    ASSERT_EQ(s.code(), Status::kExisted);
    // add success
    add_peer.set_node_id(104);
    s = keeper.AddPeer(add_peer, meta.range_epoch().conf_ver());
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(keeper.GetConfVer(), meta.range_epoch().conf_ver() + 1);
    ASSERT_EQ(keeper.GetAllPeers().size(), 4U);

    // delete 4
    auto del_peer = add_peer;
    s = keeper.DelPeer(del_peer, meta.range_epoch().conf_ver());
    ASSERT_EQ(s.code(), Status::kStaleEpoch);
    s = keeper.DelPeer(del_peer, meta.range_epoch().conf_ver() + 2);
    ASSERT_EQ(s.code(), Status::kInvalidArgument);
    // delete not found
    del_peer.set_id(5); // peer id not found
    s = keeper.DelPeer(del_peer, meta.range_epoch().conf_ver() + 1);
    ASSERT_EQ(s.code(), Status::kNotFound);
    del_peer.set_id(4);
    del_peer.set_node_id(105); // node_id not found
    s = keeper.DelPeer(del_peer, meta.range_epoch().conf_ver() + 1);
    ASSERT_EQ(s.code(), Status::kNotFound);
    del_peer.set_node_id(104);
    s = keeper.DelPeer(del_peer, meta.range_epoch().conf_ver() + 1);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(keeper.GetConfVer(), meta.range_epoch().conf_ver() + 2);
    ASSERT_EQ(keeper.GetAllPeers().size(), 3U);

    // promote peer 3
    s = keeper.PromotePeer(104, 3);
    ASSERT_EQ(s.code(), Status::kNotFound);
    s = keeper.PromotePeer(103, 4);
    ASSERT_EQ(s.code(), Status::kNotFound);
    s = keeper.PromotePeer(103, 3);
    ASSERT_TRUE(s.ok()) << s.ToString();
}

TEST(RangeMetaKeeper, Split) {
    auto meta = randMeta();
    MetaKeeper keeper(meta, nullptr);

    auto s = keeper.CheckSplit("c", meta.range_epoch().version() - 1);
    ASSERT_EQ(s.code(), Status::kStaleEpoch);
    s = keeper.CheckSplit("c", meta.range_epoch().version() + 1);
    ASSERT_EQ(s.code(), Status::kInvalidArgument);
    s = keeper.CheckSplit("a", meta.range_epoch().version());
    ASSERT_EQ(s.code(), Status::kOutOfBound);
    s = keeper.CheckSplit("z", meta.range_epoch().version());
    ASSERT_EQ(s.code(), Status::kOutOfBound);
    s = keeper.CheckSplit("c", meta.range_epoch().version());
    ASSERT_TRUE(s.ok()) << s.ToString();

    keeper.Split("c", meta.range_epoch().version() + 1);
    ASSERT_EQ(keeper.GetVersion(), meta.range_epoch().version() + 1);
    ASSERT_EQ(keeper.GetEndKey(), "c");
}


}
