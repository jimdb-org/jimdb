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

#include "range.h"

#include "storage/meta_store.h"
#include "base/util.h"

#include "range_logger.h"

namespace jimkv {
namespace ds {
namespace range {

uint64_t Range::GetPeerID() const {
    basepb::Peer peer;
    if (meta_.FindPeerByNodeID(node_id_)) {
        return peer.id();
    }
    return 0;
}

void Range::AddPeer(const basepb::Peer &peer) {
    raft::ConfChange cch;

    if (meta_.FindPeerByNodeID(peer.node_id())) {
        RLOG_WARN("AddPeer existed: {}", peer.ShortDebugString());
        return;
    }

    cch.type = raft::ConfChangeType::kAdd;
    cch.peer.type = peer.type() == basepb::PeerType_Learner ? raft::PeerType::kLearner
        : raft::PeerType::kNormal;
    cch.peer.node_id = peer.node_id();
    cch.peer.peer_id = peer.id();

    dspb::PeerTask pt;

    auto ap = pt.mutable_peer();
    ap->set_id(peer.id());
    ap->set_node_id(peer.node_id());
    ap->set_type(peer.type());

    auto ep = pt.mutable_verify_epoch();
    ep->set_conf_ver(meta_.GetConfVer());

    pt.SerializeToString(&cch.context);
    raft_->ChangeMemeber(cch);

    RLOG_INFO("AddPeer NodeId: {} version: {} conf_ver: {}",
            peer.node_id(), meta_.GetVersion(), meta_.GetConfVer());
}

void Range::DelPeer(const basepb::Peer &peer) {
    raft::ConfChange cch;

    basepb::Peer old_peer;
    if (!meta_.FindPeerByNodeID(peer.node_id(), &old_peer) || old_peer.id() != peer.id()) {
        RLOG_WARN("DelPeer NodeId: {} peer: {} info mismatch, Current Peer NodeId: {} peer: {}",
                peer.node_id(), peer.id(), old_peer.node_id(), old_peer.id());
        return;
    }

    cch.type = raft::ConfChangeType::kRemove;

    cch.peer.type = peer.type() == basepb::PeerType_Learner ? raft::PeerType::kLearner
        : raft::PeerType::kNormal;
    cch.peer.node_id = peer.node_id();
    cch.peer.peer_id = peer.id();
    dspb::PeerTask pt;

    auto ap = pt.mutable_peer();
    ap->set_id(peer.id());
    ap->set_node_id(peer.node_id());
    ap->set_type(peer.type());

    auto ep = pt.mutable_verify_epoch();
    ep->set_conf_ver(meta_.GetConfVer());

    pt.SerializeToString(&cch.context);
    raft_->ChangeMemeber(cch);

    RLOG_INFO("DelPeer NodeId: {} version: {} conf_ver: {}",
            peer.node_id(), meta_.GetVersion(), meta_.GetConfVer());
}

Status Range::ApplyMemberChange(const raft::ConfChange &cc, uint64_t index) {
    RLOG_INFO("start ApplyMemberChange: {}, current conf ver: {} at index {}",
            cc.ToString(), meta_.GetConfVer(), index);

    Status ret;
    bool updated = false;
    switch (cc.type) {
        case raft::ConfChangeType::kAdd:
            ret = applyAddPeer(cc, &updated);
            break;
        case raft::ConfChangeType::kRemove:
            ret = applyDelPeer(cc, &updated);
            break;
        case raft::ConfChangeType::kPromote:
            ret = applyPromotePeer(cc, &updated);
            break;
        default:
            ret = Status(Status::kNotSupported, "ApplyMemberChange not support type", "");
    }

    if (!ret.ok()) {
        RLOG_ERROR("ApplyMemberChange({}) failed: {}", cc.ToString(), ret.ToString());
        return ret;
    }

    if (updated) {
        ret = context_->MetaStore()->AddRange(meta_.Get());
        if (!ret.ok()) {
            RLOG_ERROR("save meta failed: {}", ret.ToString());
            return ret;
        }

        // notify master the newest peers if we are leader
        scheduleHeartbeat(false);

        RLOG_INFO("ApplyMemberChange({}) successfully. new conf_ver: {}", cc.ToString(), meta_.GetConfVer());
    }

    apply_index_ = index;
    auto s = context_->MetaStore()->SaveApplyIndex(id_, apply_index_);
    if (!s.ok()) {
        RLOG_ERROR("save apply index error {}", s.ToString());
        return s;
    }
    return Status::OK();
}

Status Range::applyAddPeer(const raft::ConfChange &cc, bool *updated) {
    dspb::PeerTask pt;
    if (!pt.ParseFromArray(cc.context.data(), static_cast<int>(cc.context.size()))) {
        return Status(Status::kInvalidArgument, "deserialize add peer context",
                EncodeToHex(cc.context));
    }

    auto ret = meta_.AddPeer(pt.peer(), pt.verify_epoch().conf_ver());
    *updated = ret.ok();
    if (ret.code() == Status::kStaleEpoch || ret.code() == Status::kExisted) {
        RLOG_WARN("ApplyAddPeer(peer: {}) failed: {}",
                pt.peer().ShortDebugString(), ret.ToString());
        return Status::OK();
    } else {
        return ret;
    }
}

Status Range::applyDelPeer(const raft::ConfChange &cc, bool *updated) {
    dspb::PeerTask pt;
    if (!pt.ParseFromArray(cc.context.data(), static_cast<int>(cc.context.size()))) {
        return Status(Status::kInvalidArgument, "deserialize del peer context",
                EncodeToHex(cc.context));
    }

    auto ret = meta_.DelPeer(pt.peer(), pt.verify_epoch().conf_ver());
    *updated = ret.ok();
    if (ret.code() == Status::kStaleEpoch || ret.code() == Status::kNotFound) {
        RLOG_WARN("ApplyDelPeer(peer: {}) failed: {}",
                pt.peer().ShortDebugString(), ret.ToString());
        return Status::OK();
    } else {
        return ret;
    }
}

Status Range::applyPromotePeer(const raft::ConfChange &cc, bool *updated) {
    auto ret = meta_.PromotePeer(cc.peer.node_id, cc.peer.peer_id);
    *updated = ret.ok();
    if (ret.code() == Status::kNotFound) {
        RLOG_WARN("ApplyPromote(peer: {}) failed: {}", cc.peer.ToString(), ret.ToString());
        return Status::OK();
    } else {
        return ret;
    }
}

}  // namespace range
}  // namespace ds
}  // namespace jimkv
