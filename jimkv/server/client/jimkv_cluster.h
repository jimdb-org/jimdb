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

_Pragma ("once");

#include <string>
#include <memory>
#include <map>
#include <chrono>

#include "asio/steady_timer.hpp"

#include "base/shared_mutex.h"

#include "jimkv_scope.h"
#include "jimkv_route.h"
#include "jimkv_node.h"
#include "jimkv_command.h"
#include "jimkv_message.h"
#include "jimkv_master.h"

namespace jimkv {
namespace sdk {

class JimCluster final {
public:
    JimCluster(uint64_t cluster_id, const std::string& host, uint32_t conn_count);
    ~JimCluster() = default;

    JimCluster(const JimCluster&) = delete;
    JimCluster& operator=(const JimCluster&) = delete;

    bool SendCmd(CmdPtr cmd, int timeout);

    bool ReSendMsg(MsgPtr msg);

    bool AddTable(uint64_t db_id, uint64_t table_id);

    void UpdateRange(const basepb::Range& range);
    void UpdateRange(uint64_t range_id, uint64_t leader,
        const basepb::RangeEpoch& epoch);

    void DeleteNode(uint64_t node_id);
    void DeleteRoute(uint64_t range_id);
private:
    bool AddNode(uint64_t node_id, const std::string& ip, uint32_t port);
    NodePtr GetNode(uint64_t node_id);

    RangePtr UpdateTable(uint64_t db_id, uint64_t table_id, const std::string& key);
    bool UpdateRoute(mspb::GetRouteRequest& req);

    void GetAllRange(CmdPtr cmd, JimRoute& routes);

    bool SendSubCmd(CmdPtr cmd, asio::steady_timer::time_point expire);

    bool SendSubMsg(MsgPtr msg, JimScope& scope, RangePtr rng);
    bool SendRangeMsg(MsgPtr msg);
    bool SplitScope(MsgPtr msg, JimScope& scope);
private:
    uint64_t cluster_id_;
    uint32_t conn_count_;
    MasterPtr master_;

    RoutePtr routes_;

    std::map<uint64_t, NodePtr> nodes_;
    shared_mutex nodes_lock_;
};

using ClusterPtr = std::shared_ptr<JimCluster>;

} // namespace sdk
} // namespace jimkv

