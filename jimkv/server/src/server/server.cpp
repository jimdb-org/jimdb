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

#include "server.h"

#include <iostream>

#include "common/server_config.h"

#include "admin/admin_server.h"
#include "master/client_impl.h"
#include "common/masstree_env.h"

#include "db/mass_tree_impl/manager_impl.h"
#include "db/rocksdb_impl/manager_impl.h"

#include "node_address.h"
#include "range_server.h"
#include "run_status.h"
#include "version.h"
#include "worker.h"
#include "rpc_server.h"

namespace jimkv {
namespace ds {
namespace server {

static void buildRPCOptions(net::ServerOptions& opt) {
    opt.io_threads_num = ds_config.rpc_config.io_threads_num;
    opt.max_connections = 50000;
}

DataServer::DataServer() {
    // start masstree rcu incr epoch thread
    StartRCUThread(ds_config.masstree_config.rcu_interval_ms);

    context_ = new ContextServer;
    context_->run_status = new RunStatus();

    context_->worker = new Worker();

    context_->range_server = new RangeServer;

    net::ServerOptions sopt;
    buildRPCOptions(sopt);
    context_->rpc_server = new RPCServer(sopt);

    // create master worker
    context_->master_client = new master::MasterClientImpl(
            ds_config.cluster_config.cluster_id, ds_config.cluster_config.master_host);
}

DataServer::~DataServer() {
    delete context_->rpc_server;
    delete context_->worker;
    delete context_->master_client;
    delete context_->run_status;
    delete context_->range_server;
    delete context_->raft_server;
    delete context_->db_manager;
    delete context_;
}

bool DataServer::startRaftServer() {
    raft::RaftServerOptions ops;
    const auto& raft_config = ds_config.raft_config;

    ops.node_id = context_->node_id;
    ops.consensus_threads_num = static_cast<uint8_t>(raft_config.consensus_threads);
    ops.consensus_queue_capacity = raft_config.consensus_queue;
    ops.read_option = raft_config.read_option;
    ops.tick_interval = std::chrono::milliseconds(raft_config.tick_interval_ms);
    ops.max_size_per_msg = raft_config.max_msg_size;

    ops.transport_options.listen_ip = raft_config.ip_addr;
    ops.transport_options.listen_port = static_cast<uint16_t>(raft_config.port);
    ops.transport_options.send_io_threads = raft_config.transport_send_threads;
    ops.transport_options.recv_io_threads = raft_config.transport_recv_threads;
    ops.transport_options.connection_pool_size = raft_config.connection_pool_size;
    // setup resolver
    auto resolver = std::make_shared<NodeAddress>(context_->master_client);
    if (ds_config.b_test) {
        for (const auto& p : ds_config.test_config.nodes) {
            resolver->AddNodeAddress(p.first, p.second);
        }
    }
    ops.transport_options.resolver = resolver;

    ops.snapshot_options.ack_timeout_seconds = raft_config.snapshot_wait_ack_timeout;
    ops.snapshot_options.max_send_concurrency = raft_config.snapshot_send_concurrency;
    ops.snapshot_options.max_apply_concurrency = raft_config.snapshot_apply_concurrency;

    auto rs = raft::CreateRaftServer(ops);
    context_->raft_server = rs.release();
    auto s = context_->raft_server->Start();
    if (!s.ok()) {
        FLOG_ERROR("RaftServer Start error ... : {}", s.ToString());
        return false;
    }

    s = context_->raft_server->SetOptions({{"electable", "0"}});
    if (!s.ok()) {
        FLOG_ERROR("RaftServer disable electable failed: {}", s.ToString());
        return false;
    }

    FLOG_DEBUG("RaftServer Started...");
    return true;
}

void DataServer::registerToMaster() {
    std::string version = GetGitDescribe();
    FLOG_INFO("Version: {}", version);

    mspb::RegisterNodeRequest req;
    req.set_server_port(static_cast<uint32_t>(ds_config.rpc_config.port));
    req.set_raft_port(static_cast<uint32_t>(ds_config.raft_config.port));
    req.set_admin_port(static_cast<uint32_t>(ds_config.manager_config.port));
    req.set_version(version);

    switch (ds_config.engine_type) {
        case EngineType::kMassTree:
            req.set_st_engine(mspb::StorageEngine::STE_MassTree);
            break;
        case EngineType::kRocksdb:
            req.set_st_engine(mspb::StorageEngine::STE_RocksDB);
            break;
        default:
            req.set_st_engine(mspb::StorageEngine::STE_Invalid);
    }

    while (true) {
        mspb::RegisterNodeResponse resp;
        auto s = context_->master_client->NodeRegister(req, resp);
        if (!s.ok()) {
            FLOG_ERROR("Register to master failed. {}", s.ToString());
            sleep(3);
        } else {
            context_->node_id = resp.node_id();
            FLOG_INFO("Register to master success. node_id: {}", resp.node_id());
            break;
        }
    }
}

bool DataServer::Start() {
    if (!ds_config.b_test) {
        registerToMaster();
    } else {
        context_->node_id = ds_config.test_config.node_id;
    }

    if (!startRaftServer()) {
        return false;
    }

    if (context_->range_server->Init(context_) != 0) {
        return false;
    }

    // start worker
    auto ret = context_->worker->Start(ds_config.worker_config.schedule_worker_num,
            ds_config.worker_config.slow_worker_num, context_->range_server);
    if (!ret.ok()) {
        FLOG_ERROR("start worker failed: {}", ret.ToString());
        return false;
    }

    // start rpc server
    const auto& rpc_config = ds_config.rpc_config;
    ret = context_->rpc_server->Start(rpc_config.ip_address, rpc_config.port, context_->worker);
    if (!ret.ok()) {
        FLOG_ERROR("start rpc server failed: {}", ret.ToString());
        return false;
    }

    // start admin server
    admin_server_.reset(new admin::AdminServer(context_));
    ret = admin_server_->Start(static_cast<uint16_t>(ds_config.manager_config.port));
    if (!ret.ok()) {
        FLOG_ERROR("start admin server failed: {}", ret.ToString());
        return false;
    }

    if (context_->range_server->Start() != 0) {
        return false;
    }

    if (ds_config.b_test && !ds_config.test_config.is_leader) {
        // in test mode, current node is not leader, disable electable
    } else {
        // server is ready, raft can be electable
        ret = context_->raft_server->SetOptions({{"electable", "1"}});
        if (!ret.ok()) {
            FLOG_ERROR("RaftServer enable electable failed: {}", ret.ToString());
            return false;
        }
    }

    return true;
}

void DataServer::Stop() {
    if (admin_server_) {
        admin_server_->Stop();
    }
    if (context_->rpc_server) {
        context_->rpc_server->Stop();
    }

    if (context_->worker != nullptr) {
        context_->worker->Stop();
    }
    if (context_->range_server != nullptr) {
        context_->range_server->Stop();
    }
    if (context_->raft_server != nullptr) {
        context_->raft_server->Stop();
    }
}

void DataServer::ServerCron() {
    // collect db usage
    context_->run_status->UpdateDBUsage(context_->db_manager);

    // node heartbeat
    if (!ds_config.b_test) {
        heartbeat();
    }

    triggerRCU();

    // print metrics
    context_->run_status->PrintStatistics();
    // print db metric
    FLOG_INFO("DB Metric: {}", context_->db_manager->MetricInfo(false));
    // print worker pending request size
    context_->worker->PrintQueueSize();
}

void DataServer::heartbeat() {
    mspb::NodeHeartbeatRequest req;
    req.set_node_id(context_->node_id);

    mspb::NodeHeartbeatResponse resp;
    auto s = context_->master_client->NodeHeartbeat(req, resp);
    if (!s.ok()) {
        FLOG_ERROR("NodeHeartbeat failed: {}", s.ToString());
    } else {
        FLOG_INFO("NodeHeartbeat succuss.");
    }
}

void DataServer::triggerRCU() {
    if (ds_config.engine_type == EngineType::kMassTree) {
        context_->raft_server->PostToAllApplyThreads([this] {
            auto db = dynamic_cast<db::MasstreeDBManager*>(context_->db_manager);
            if (db != nullptr) {
                db->RCUFree(false);
            }
        });
    } else if (ds_config.engine_type == EngineType::kRocksdb &&
        ds_config.rocksdb_config.enable_txn_cache) {
        context_->raft_server->PostToAllApplyThreads([this] {
            auto db = dynamic_cast<db::RocksDBManager*>(context_->db_manager);
            if (db != nullptr) {
                db->RCUFree(false);
            }
        });
    }
}

} /* namespace server */
} /* namespace ds  */
} /* namespace jimkv */
