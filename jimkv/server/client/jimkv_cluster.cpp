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


#include "jimkv_cluster.h"

#include "util.h"

#include "jimkv_message.h"
#include "jimkv_log.h"

namespace jimkv {
namespace sdk {

static const uint32_t kCountRoutePerUpdate = 10;

JimCluster::JimCluster(uint64_t cluster_id, const std::string& host, uint32_t conn_count) :
    cluster_id_(cluster_id),
    conn_count_(conn_count)
{
    routes_ = std::make_shared<JimRoute>();
    master_ = std::make_shared<JimMaster>(cluster_id, host);
}

bool JimCluster::AddNode(uint64_t node_id, const std::string& ip, uint32_t port) {
    auto node = std::make_shared<JimNode>(node_id, ip, port, conn_count_);
    node->CreateConn();

    std::lock_guard<shared_mutex> lock(nodes_lock_);
    nodes_[node_id] = node;
    return true;
}

NodePtr JimCluster::GetNode(uint64_t node_id) {
    shared_lock<shared_mutex> lock(nodes_lock_);
    auto it = nodes_.find(node_id);
    if (it != nodes_.end()) {
        return it->second;
    }

    return nullptr;
}

void JimCluster::DeleteNode(uint64_t node_id) {
    std::lock_guard<shared_mutex> lock(nodes_lock_);
    nodes_.erase(node_id);
}

void JimCluster::DeleteRoute(uint64_t range_id) {
    routes_->Erase(range_id);
}

bool JimCluster::AddTable(uint64_t db_id, uint64_t table_id) {
    mspb::GetRouteRequest req;
    req.mutable_header()->set_cluster_id(cluster_id_);
    req.set_db_id(db_id);
    req.set_table_id(table_id);
    req.set_rangetype(basepb::RangeType::RNG_Data);

    return UpdateRoute(req);
}

RangePtr JimCluster::UpdateTable(uint64_t db_id, uint64_t table_id,
    const std::string& key)
{
    mspb::GetRouteRequest req;
    req.mutable_header()->set_cluster_id(cluster_id_);
    req.set_db_id(db_id);
    req.set_table_id(table_id);
    req.set_key(key);
    req.set_max(kCountRoutePerUpdate);
    req.set_rangetype(basepb::RangeType::RNG_Data);

    int try_get = 3;
    while (try_get--) {
        if (!UpdateRoute(req)) {
            CBLOG_WARN("route update fail, key:%s", EncodeToOct(key).c_str());
            continue;
        }
        auto range = routes_->GetRange(key);
        if (range == nullptr) {
            CBLOG_WARN("route not found, key:%s", EncodeToOct(key).c_str());
        } else {
            return range;
        }
    }

    return nullptr;
}

void JimCluster::UpdateRange(const basepb::Range& range) {
    auto node_id = range.leader();
    if (node_id == 0) {
        return CBLOG_WARN("leader invalid, node_id: 0");
    }

    if (GetNode(node_id) == nullptr) {
        master_->GetNode(node_id,
            [this](mspb::GetNodeResponse& resp) {
                auto node = resp.node();
                return AddNode(node.id(), node.ip(), node.server_port());
            });
    }
    routes_->Insert(range);
}

void JimCluster::UpdateRange(uint64_t range_id, uint64_t leader,
        const basepb::RangeEpoch& epoch)
{
    if (leader == 0) {
        return CBLOG_WARN("leader invalid, node_id: 0");
    }

    if (routes_->Update(range_id, leader, epoch)) {
        if (GetNode(leader) == nullptr) {
            master_->GetNode(leader,
                [this](mspb::GetNodeResponse& resp) {
                    auto node = resp.node();
                    return AddNode(node.id(), node.ip(), node.server_port());
                });
        }
    }
}

bool JimCluster::UpdateRoute(mspb::GetRouteRequest& req) {
    return master_->GetRoute(req, [this](mspb::GetRouteResponse &resp) {
            for (auto &range: resp.routes()) {
                UpdateRange(range);
            }
            return true;
        });
}

bool JimCluster::SendCmd(CmdPtr cmd, int timeout) {
    auto expire = asio::steady_timer::clock_type::now() + asio::chrono::milliseconds(timeout);
    //auto expire = std::chrono::system_clock::now() + std::chrono::milliseconds(timeout);
    if (cmd->has_sub_command()) {
        auto sub_cmd = cmd->sub_command();
        for (auto &sub : sub_cmd) {
            if (!SendSubCmd(sub, expire)) {
                return false;
            }
        }
        return true;
    } else {
        return SendSubCmd(cmd, expire);
    }
}

bool JimCluster::SendSubCmd(CmdPtr cmd, asio::steady_timer::time_point expire) {
    if (cmd->req_type() == dspb::RangeRequest::ReqCase::kKvScan ||
            cmd->req_type() == dspb::RangeRequest::ReqCase::kKvDelRange)
    {
        auto msg = std::make_shared<JimMessage>(cmd);
        msg->expire_at_set(expire);
        return SendRangeMsg(msg);
   }

    auto &key = cmd->encode_key();
    auto range = routes_->GetRange(key);
    if (range == nullptr) {
        CBLOG_WARN("not found route (key):%s", EncodeToOct(key).c_str());
        range = UpdateTable(cmd->db_id(), cmd->table_id(), key);
        if (range == nullptr) {
            CBLOG_WARN("update table fail, key: %s", EncodeToOct(key).c_str());
            return false;
        }
    }

    auto node = GetNode(range->leader());
    if (node == nullptr) {
        CBLOG_WARN("node %" PRIu64 " not found.", range->leader());
        return false;
    }

    auto msg = std::make_shared<JimMessage>(cmd);
    if (msg->Serialize(range)) {
        msg->expire_at_set(expire);
        return node->SendMsg(msg);
    }
    return false;
}

bool JimCluster::SendSubMsg(MsgPtr msg, JimScope& scope, RangePtr rng) {
    auto node = GetNode(rng->leader());
    if (node == nullptr) {
        CBLOG_WARN("node %" PRIu64 " not found.", rng->leader());
        return false;
    }
    auto sub_msg = std::make_shared<JimMessage>(msg->cmd(), scope);
    if (!sub_msg->Serialize(rng)) {
        CBLOG_WARN("serialize failed");
        return false;
    }

    sub_msg->expire_at_set(msg->expire_at());
    msg->AddSubMsg(sub_msg);

    if (!node->SendMsg(sub_msg)) {
        msg->Complete();
        return false;
    }
    return true;
}

bool JimCluster::SplitScope(MsgPtr msg, JimScope& scope) {
    std::string start_key(scope.start());
    std::string end_key(scope.end());

    auto cmd = msg->cmd();

    while (cmd->valid()) {
        auto range = routes_->GetRange(start_key);
        if (range == nullptr) {
            CBLOG_WARN("not found route (key):%s", EncodeToOct(cmd->encode_key()).c_str());

            range = UpdateTable(cmd->db_id(), cmd->table_id(), start_key);
            if (range == nullptr) {
                CBLOG_WARN("update table fail, key: %s", EncodeToOct(start_key).c_str());
                break;
            }
        }

        if (end_key < range->end_key()) {
            JimScope ks(start_key, end_key);
            if (!SendSubMsg(msg, ks, range)) {
                break;
            }
            msg->SendEnd();
            return true;
        } else {
            JimScope ks(start_key, range->end_key());
            if (!SendSubMsg(msg, ks, range)) {
                break;
            }

            start_key = range->end_key();
        }
    }

    cmd->set_valid(false);
    msg->SendEnd();

    return false;
}

bool JimCluster::SendRangeMsg(MsgPtr msg) {
    std::string start_key = msg->cmd()->encode_key();
    std::string end_key = start_key;
    end_key[end_key.size() -1 ] += 1;

    JimScope scope(start_key, end_key);
    return SplitScope(msg, scope);
}

bool JimCluster::ReSendMsg(MsgPtr msg) {
    CBLOG_INFO("resend msg key: %s", EncodeToOct(msg->start_key()).c_str());

    if (!msg->has_parent()) {
        if (msg->timeout()) {
            msg->Complete();
            return false;
        } else {
            return SendSubCmd(msg->cmd(), msg->expire_at());
        }
    }

    // range operator
    auto parent = msg->parent();
    if (parent->timeout()) {
        msg->Complete();
        return false;
    }

    parent->SendBegin();
    parent->Complete(); // set cur msg completed

    JimScope scope(msg->start_key(), msg->end_key());
    return SplitScope(parent, scope);
}

} // namespace sdk
} // namespace jimkv

