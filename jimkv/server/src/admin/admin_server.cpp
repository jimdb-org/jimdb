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

#include "admin_server.h"

#include "net/session.h"
#include "common/logger.h"
#include "server/range_server.h"
#include "server/worker.h"

namespace jimkv {
namespace ds {
namespace admin {

using namespace dspb;

AdminServer::AdminServer(server::ContextServer* context) :
    context_(context) {
}

AdminServer::~AdminServer() {
    Stop();
}

Status AdminServer::Start(uint16_t port) {
    net::ServerOptions sops;
    sops.io_threads_num = 0;
    sops.max_connections = 200;
    net_server_.reset(new net::Server(sops, "admin"));

    auto ret = net_server_->ListenAndServe("0.0.0.0", port,
            [this](const net::Context& ctx, const net::MessagePtr& msg) {
                onMessage(ctx, msg);
            });
    if (!ret.ok()) return ret;

    FLOG_INFO("[Admin] server listen on 0.0.0.0:{}", port);

    return Status::OK();
}

Status AdminServer::Stop() {
    net_server_->Stop();
    return Status::OK();
}

Status AdminServer::checkAuth(const AdminAuth& auth) {
    // TODO:
    return Status::OK();
}

Status AdminServer::execute(const AdminRequest& req, AdminResponse* resp) {
    switch (req.req_case()) {
        case AdminRequest::kSetCfg:
            return setConfig(req.set_cfg(), resp->mutable_set_cfg());
        case AdminRequest::kGetCfg:
            return getConfig(req.get_cfg(), resp->mutable_get_cfg());
        case AdminRequest::kGetInfo:
            return getInfo(req.get_info(), resp->mutable_get_info());
        case AdminRequest::kForceSplit:
            return forceSplit(req.force_split(), resp->mutable_force_split());
        case AdminRequest::kCompaction:
            return compaction(req.compaction(), resp->mutable_compaction());
        case AdminRequest::kClearQueue:
            return clearQueue(req.clear_queue(), resp->mutable_clear_queue());
        case AdminRequest::kGetPendings:
            return getPending(req.get_pendings(), resp->mutable_get_pendings());
        case AdminRequest::kFlushDb:
            return flushDB(req.flush_db(), resp->mutable_flush_db());
        case AdminRequest::kProfile:
            return profile(req.profile(), resp->mutable_profile());
        case AdminRequest::kDump:
            return dump(req.dump(), resp->mutable_dump());
        default:
            return Status(Status::kNotSupported, "admin type", std::to_string(req.req_case()));
    }
}

std::pair<db::RocksDBManager*, Status> GetRocksdbMgr(server::ContextServer *ctx) {
    if (ds_config.engine_type != EngineType::kRocksdb) {
        return {nullptr, Status(Status::kNotSupported,
                      "compaction engine", EngineTypeName(ds_config.engine_type))};
    }

    // engine type is rocksdb now
    auto db = dynamic_cast<db::RocksDBManager*>(ctx->db_manager);
    if (db == nullptr) {
        return {nullptr, Status(Status::kNotSupported,
                                "compaction engine", EngineTypeName(ds_config.engine_type))};
    }
    return { db, Status::OK() };
}

std::pair<db::MasstreeDBManager*, Status> GetMasstreeMgr(server::ContextServer* ctx) {
    if (ds_config.engine_type != EngineType::kMassTree) {
        return { nullptr, Status(Status::kNotSupported,
                      "dump", EngineTypeName(ds_config.engine_type)) };
    }

    // engine type is masstree now
    auto db = dynamic_cast<db::MasstreeDBManager*>(ctx->db_manager);
    if (db == nullptr) {
        return { nullptr, Status(Status::kNotSupported,
                      "unknown db instance", typeid(ctx->db_manager).name()) };
    }
    return { db, Status::OK() };
}

void AdminServer::onMessage(const net::Context& ctx, const net::MessagePtr& msg) {
    AdminRequest req;
    if (!req.ParseFromArray(msg->body.data(), static_cast<int>(msg->body.size()))) {
        FLOG_ERROR("[Admin] deserialize failed from {}, head: {}",
                ctx.remote_addr, msg->head.DebugString());
    }
    FLOG_INFO("[Admin] recv from {}, detail: {}", ctx.remote_addr, req.ShortDebugString());

    AdminResponse resp;
    Status ret = checkAuth(req.auth());
    if (ret.ok()) {
        ret = execute(req, &resp);
    }

    if (!ret.ok()) {
        FLOG_WARN("[Admin] handle cmd from {} error: {}, cmd: {}", ctx.remote_addr,
                ret.ToString(), req.ShortDebugString());
        resp.set_code(static_cast<uint32_t>(ret.code()));
        resp.set_error_msg(ret.ToString());
    }

    std::vector<uint8_t> resp_body;
    resp_body.resize(resp.ByteSizeLong());
    if (resp.SerializeToArray(resp_body.data(), static_cast<int>(resp_body.size()))) {
        ctx.Write(msg->head, std::move(resp_body));
    }
}

Status AdminServer::forceSplit(const ForceSplitRequest& req, ForceSplitResponse* resp) {
    auto rng = context_->range_server->Find(req.range_id());
    if (rng == nullptr) {
        return Status(Status::kNotFound, "range", std::to_string(req.range_id()));
    }
    FLOG_INFO("[Admin] force split range {}, version: {}", req.range_id(), req.version());
    return rng->ForceSplit(req.version(), resp->mutable_split_key());
}

Status AdminServer::compaction(const CompactionRequest& req, CompactionResponse* resp) {
    auto ret = GetRocksdbMgr(context_);
    if (!ret.second.ok()) {
        return ret.second;
    }
    // db handle got
    Status s;
    rocksdb::CompactRangeOptions options;
    if (req.range_id() == 0) {
        s = ret.first->CompactRange(options, nullptr, nullptr);
    } else {
        auto rng = context_->range_server->Find(req.range_id());
        if (rng == nullptr) {
            return Status(Status::kNotFound, "range", std::to_string(req.range_id()));
        }
        auto meta = rng->GetMeta();
        resp->set_begin_key(meta.start_key());
        resp->set_end_key(meta.end_key());
        rocksdb::Slice begin = meta.start_key();
        rocksdb::Slice end = meta.end_key();
        s = ret.first->CompactRange(options, &begin, &end);
    }
    return s;
}

Status AdminServer::clearQueue(const ClearQueueRequest& req, ClearQueueResponse* resp) {
    bool clear_fast = false, clear_slow = false;
    switch (req.queue_type()) {
        case ClearQueueRequest_QueueType_FAST_WORKER:
            clear_fast = true;
            break;
        case ClearQueueRequest_QueueType_SLOW_WORKER:
            clear_slow = true;
            break;
        case ClearQueueRequest_QueueType_ALL:
            clear_fast = true;
            clear_slow = true;
            break;
        default:
            return Status(Status::kInvalidArgument, "queue type", std::to_string(req.queue_type()));
    }
    resp->set_cleared(context_->worker->ClearQueue(clear_fast, clear_slow));
    FLOG_WARN("[Admin] {} queue cleared: {}",
            ClearQueueRequest_QueueType_Name(req.queue_type()).c_str(), resp->cleared());
    return Status::OK();
}

Status AdminServer::getPending(const GetPendingsRequest& req, GetPendingsResponse* resp) {
    return Status(Status::kNotSupported);
}

Status AdminServer::flushDB(const FlushDBRequest& req, FlushDBResponse* resp) {
    auto ret = GetRocksdbMgr(context_);
    if (!ret.second.ok()) {
        return ret.second;
    }
    // flush
    rocksdb::FlushOptions fops;
    fops.wait = req.wait();
    return ret.first->Flush(fops);
}

Status AdminServer::dump(const dspb::DumpRequest& req, dspb::DumpResponse *resp) {
    auto ret = GetMasstreeMgr(context_);
    if (ret.second.ok()) {
        return ret.second;
    }
    return ret.first->DumpTree("/tmp");
}

} // namespace admin
} // namespace ds
} // namespace jimkv
