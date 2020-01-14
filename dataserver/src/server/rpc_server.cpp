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

#include <common/server_config.h>
#include "rpc_server.h"

#include "common/logger.h"
#include "common/rpc_request.h"
#include "common/server_config.h"
#include "worker.h"
#include "dspb/function.pb.h"


namespace jim {
namespace ds {
namespace server {

RPCServer::RPCServer(const net::ServerOptions& ops) :
    ops_(ops) {
}

RPCServer::~RPCServer() {
    Stop();
}

Status RPCServer::Start(const std::string& ip, uint16_t port, Worker* worker) {
    assert(net_server_ == nullptr);
    net_server_.reset(new net::Server(ops_, "rpc"));
    worker_ = worker;
    auto ret = net_server_->ListenAndServe("0.0.0.0", port,
                                           [this](const net::Context& ctx, const net::MessagePtr& msg) {
                                               onMessage(ctx, msg);
                                           });
    if (ret.ok()) {
        FLOG_INFO("RPC Server listen on 0.0.0.0:{}", port);
    }
    return ret;
}

Status RPCServer::Stop() {
    if (net_server_) {
        net_server_->Stop();
        net_server_.reset();
        FLOG_INFO("RPC Server stopped");
    }
    return Status::OK();
}

static bool isSlowProcessor(const dspb::Processor& p) {
    return p.type() == dspb::DATA_SAMPLE_TYPE || p.type() == dspb::AGGREGATION_TYPE
        || p.type() == dspb::STREAM_AGGREGATION_TYPE;
}

static bool maybeSlow(const RPCRequest& rpc) {
    assert(rpc.sch_req || rpc.range_req);
    // has force fast flag
    if (rpc.msg->head.ForceFastFlag()) {
        return false;
    }
    // schedule request maybe slow
    if (!rpc.range_req) {
        return true;
    }

    const auto& req = *(rpc.range_req);
    if (!req.has_select_flow()) {
        return false;
    }
    for (const auto& p : req.select_flow().processors()) {
        if (isSlowProcessor(p)) {
            return true;
        }
    }
    return false;
}


void RPCServer::onMessage(const net::Context& ctx, const net::MessagePtr& msg) {
    auto rpc = new RPCRequest(ctx, msg);
    if (!rpc->Parse()) {
        FLOG_WARN("parse rpc request failed from {}, msgid={}", ctx.remote_addr, msg->head.msg_id);
        return;
    }

    if (!maybeSlow(*rpc)) {
        worker_->Deal(rpc); // if not slow, deal in io thread in place
    } else {
        worker_->Push(rpc); // if slow, push to work queue
    }
}

} /* namespace server */
} /* namespace ds  */
} /* namespace jim */
