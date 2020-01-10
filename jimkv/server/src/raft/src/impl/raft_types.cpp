// Copyright 2015 The etcd Authors
// Portions Copyright 2019 The JimDB Authors.
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

#include "raft_types.h"

#include <sstream>

namespace jimkv {
namespace raft {
namespace impl {

std::string FsmStateName(FsmState state) {
    switch (state) {
        case FsmState::kFollower:
            return "follower";
        case FsmState::kCandidate:
            return "candidate";
        case FsmState::kLeader:
            return "leader";
        case FsmState::kPreCandidate:
            return "pre-candidate";
        default:
            return "unknown";
    }
}

std::string ReplicateStateName(ReplicaState state) {
    switch (state) {
        case ReplicaState::kProbe:
            return "probe";
        case ReplicaState::kReplicate:
            return "replicate";
        case ReplicaState::kSnapshot:
            return "snapshot";
        default:
            return "invalid";
    }
}

Status EncodePeer(const Peer& peer, pb::Peer* pb_peer) {
    assert(pb_peer != nullptr);
    pb_peer->set_node_id(peer.node_id);
    pb_peer->set_peer_id(peer.peer_id);
    switch (peer.type) {
        case PeerType::kNormal:
            pb_peer->set_type(pb::PEER_NORMAL);
            break;
        case PeerType::kLearner:
            pb_peer->set_type(pb::PEER_LEARNER);
            break;
        default:
            return Status(Status::kInvalidArgument, "EncodePeer(): unknown peer type",
                          PeerTypeName(peer.type));
    }
    return Status::OK();
}

Status DecodePeer(const pb::Peer& pb_peer, Peer* peer) {
    assert(peer != nullptr);
    peer->node_id = pb_peer.node_id();
    peer->peer_id = pb_peer.peer_id();
    switch (pb_peer.type()) {
        case pb::PEER_NORMAL:
            peer->type = PeerType::kNormal;
            break;
        case pb::PEER_LEARNER:
            peer->type = PeerType::kLearner;
            break;
        default:
            return Status(Status::kInvalidArgument, "DecodePeer(): unkown peer type:",
                          std::to_string(pb_peer.type()));
    }
    return Status::OK();
}

Status EncodeConfChange(const ConfChange& cc, std::string* pb_str) {
    assert(pb_str != nullptr);

    pb::ConfChange pb_cc;
    auto s = EncodePeer(cc.peer, pb_cc.mutable_peer());
    if (!s.ok()) return s;

    pb_cc.set_context(cc.context);
    switch (cc.type) {
        case ConfChangeType::kAdd:
            pb_cc.set_type(pb::CONF_ADD_PEER);
            break;
        case ConfChangeType::kRemove:
            pb_cc.set_type(pb::CONF_REMOVE_PEER);
            break;
        case ConfChangeType::kPromote:
            pb_cc.set_type(pb::CONF_PROMOTE_PEER);
            break;
        default:
            return Status(Status::kInvalidArgument,
                          "EncodeConfChange(): unknown confchange type",
                          ConfChangeTypeName(cc.type));
    }

    bool ret = pb_cc.SerializeToString(pb_str);
    if (!ret) {
        return Status(Status::kCorruption,
                      "EncodeConfChange(): protobuf SerializeToString failed", "false");
    }
    return Status::OK();
}

Status DecodeConfChange(const std::string& pb_str, ConfChange* cc) {
    pb::ConfChange pb_cc;
    bool ret = pb_cc.ParseFromString(pb_str);
    if (!ret) {
        return Status(Status::kCorruption,
                      "DecodeConfChange(): protobuf ParseFromString failed", "false");
    }

    auto s = DecodePeer(pb_cc.peer(), &cc->peer);
    if (!s.ok()) return s;

    cc->context = pb_cc.context();

    switch (pb_cc.type()) {
        case pb::CONF_ADD_PEER:
            cc->type = ConfChangeType::kAdd;
            break;
        case pb::CONF_REMOVE_PEER:
            cc->type = ConfChangeType::kRemove;
            break;
        case pb::CONF_PROMOTE_PEER:
            cc->type = ConfChangeType::kPromote;
            break;
        default:
            return Status(Status::kInvalidArgument,
                          "DecodeConfChange(): unknown confchange type",
                          std::to_string(pb_cc.type()));
    }
    return Status::OK();
}

bool IsLocalMsg(MessagePtr& msg) {
    switch (msg->type()) {
        case pb::LOCAL_MSG_HUP:
        case pb::LOCAL_MSG_PROP:
        case pb::LOCAL_MSG_TICK:
            return true;
        default:
            return false;
    }
}

bool IsResponseMsg(MessagePtr& msg) {
    switch (msg->type()) {
        case pb::APPEND_ENTRIES_RESPONSE:
        case pb::VOTE_RESPONSE:
        case pb::HEARTBEAT_RESPONSE:
        case pb::SNAPSHOT_ACK:
        case pb::PRE_VOTE_RESPONSE:
            return true;
        default:
            return false;
    }
}

} /* namespace impl */
} /* namespace raft */
} /* namespace jimkv */
