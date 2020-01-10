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

#include "server/context_server.h"
#include "net/server.h"
#include "proto/gen/dspb/admin.pb.h"

#include "db/rocksdb_impl/manager_impl.h"
#include "db/mass_tree_impl/manager_impl.h"

namespace jimkv {
namespace ds {
namespace admin {

class AdminServer {
public:
    explicit AdminServer(server::ContextServer* context);
    ~AdminServer();

    Status Start(uint16_t port);
    Status Stop();

    AdminServer(const AdminServer&) = delete;
    AdminServer& operator=(const AdminServer&) = delete;

private:
    void onMessage(const net::Context& ctx, const net::MessagePtr& msg);
    Status checkAuth(const dspb::AdminAuth& auth);
    Status execute(const dspb::AdminRequest& req, dspb::AdminResponse* resp);

    Status setConfig(const dspb::SetConfigRequest& req, dspb::SetConfigResponse* resp);
    Status getConfig(const dspb::GetConfigRequest& req, dspb::GetConfigResponse* resp);
    Status getInfo(const dspb::GetInfoRequest& req, dspb::GetInfoResponse* resp);
    Status forceSplit(const dspb::ForceSplitRequest& req, dspb::ForceSplitResponse* resp);
    Status compaction(const dspb::CompactionRequest& req, dspb::CompactionResponse* resp);
    Status clearQueue(const dspb::ClearQueueRequest& req, dspb::ClearQueueResponse* resp);
    Status getPending(const dspb::GetPendingsRequest& req, dspb::GetPendingsResponse* resp);
    Status flushDB(const dspb::FlushDBRequest& req, dspb::FlushDBResponse* resp);
    Status profile(const dspb::ProfileRequest& req, dspb::ProfileResponse *resp);
    Status dump(const dspb::DumpRequest& req, dspb::DumpResponse *resp);

private:
    server::ContextServer* context_ = nullptr;
    std::unique_ptr<net::Server> net_server_;
    // TODO: worker thread
};

std::pair<db::RocksDBManager*, Status> GetRocksdbMgr(server::ContextServer* ctx);
std::pair<db::MasstreeDBManager*, Status> GetMasstreeMgr(server::ContextServer* ctx);

} // namespace admin
} // namespace ds
} // namespace jimkv
