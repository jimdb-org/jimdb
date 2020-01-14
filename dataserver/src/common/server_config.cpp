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

#include "server_config.h"

#include <iostream>
#include <tuple>
#include <sstream>

#include "base/system_info.h"
#include "common/logger.h"
#include "common/string_util.h"

namespace jim {

ServerConfig ds_config;

std::string EngineTypeName(EngineType et) {
    switch (et) {
        case EngineType::kRocksdb:
            return "rocksdb";
        case EngineType ::kMassTree:
            return "masstree";
        default:
            return "<unknown>";
    }
}

bool ServerConfig::LoadFromFile(const std::string& conf_file, bool btest) {
    ds_config.b_test = btest;
    if (ds_config.b_test) {
        FLOG_WARN("server run in test mode!");
    }

    INIReader reader(conf_file);
    if (reader.ParseError() < 0) {
        std::cerr << "parse config file(\"" << conf_file <<  "\") failed: " << reader.ParseError() << std::endl;
        return false;
    }
    return load(reader);
}

bool ServerConfig::load(const INIReader& reader) {
    // common config options
    pid_file = reader.Get("", "pid_file", "");
    if (pid_file.empty()) {
        FLOG_CRIT("[Config] invalid pid_file: {}", ds_config.pid_file);
        return false;
    }

    docker = reader.GetBoolean("", "docker", false);
    if (docker) {
        FLOG_INFO("server run on docker");
        SetSysinfoConfig(true);
    }

    data_path = reader.Get("", "data_path", "");
    meta_path = reader.Get("", "meta_path", "");
    if (!validateDataPath(data_path, kMetaPathSuffix, meta_path)) {
        return false;
    }

    if (!loadLoggerConfig(reader)) return false;
    if (!loadEngineConfig(reader)) return false;
    if (!loadClusterConfig(reader)) return false;
    if (!loadRPCConfig(reader)) return false;
    if (!loadWorkerConfig(reader)) return false;
    if (!loadRangeConfig(reader)) return false;
    if (!loadRaftConfig(reader)) return false;
    if (!loadManagerConfig(reader)) return false;
    if (ds_config.b_test) {
        if (!loadTestConfig(reader)) return false;
    }
    return true;
}

bool ServerConfig::loadLoggerConfig(const INIReader& reader) {
    bool ret = false;
    const char *section = "log";

    logger_config.path = reader.Get(section, "log_path", "");
    if (logger_config.path.empty()) {
        FLOG_CRIT("[Config] log.log_path is required");
        return false;
    }
    logger_config.name = reader.Get(section, "log_name", "data_server");
    logger_config.level = reader.Get(section, "log_level", "info");
    logger_config.flush_interval = reader.GetInteger(section, "flush_interval", 3);

    logger_config.max_files = reader.GetInteger(section, "max_files",
            logger_config.max_files);

    std::tie(logger_config.max_size, ret) =
            loadNeBytesValue(reader, section, "max_size", logger_config.max_size);
    return ret;
}

bool ServerConfig::loadEngineConfig(const INIReader& reader) {
    const char *section = "engine";

    auto engine_name = reader.Get(section, "name", "");
    if (engine_name == "masstree" || engine_name == "mass-tree") {
        engine_type = EngineType::kMassTree;
        return masstree_config.Load(reader, data_path);
    } else if (engine_name == "rocksdb"){
        engine_type = EngineType::kRocksdb;
        return rocksdb_config.Load(reader, data_path);
    } else {
        FLOG_CRIT("[Config] unknown engine name: {}", engine_name);
        return false;
    }
}

bool ServerConfig::loadRPCConfig(const INIReader& reader) {
    const char *section = "rpc";
    bool ret = false;

    std::tie(rpc_config.port, ret) = loadUInt16(reader, section, "port");
    if (!ret) return false;

    std::tie(rpc_config.io_threads_num, ret) = loadThreadNum(reader, section, "io_threads_num", 8);
    if (!ret) return false;

    rpc_config.ip_address = reader.Get(section, "ip_addr", "0.0.0.0");

    return true;
}

bool ServerConfig::loadWorkerConfig(const INIReader& reader) {
    const char *section = "worker";

    bool ret = false;
    std::tie(worker_config.schedule_worker_num, ret) = loadThreadNum(reader, section, "schedule_worker", 4);
    if (!ret) return false;

    std::tie(worker_config.slow_worker_num, ret) = loadThreadNum(reader, section, "slow_worker", 8);
    if (!ret) return false;

    std::tie(worker_config.task_timeout_ms, ret) =
            loadIntegerAtLeast<size_t>(reader, section, "task_timeout", 10000, 5000);
    if (!ret) return false;

    return true;
}

bool ServerConfig::loadClusterConfig(const INIReader& reader) {
    const char *section = "cluster";

    cluster_config.cluster_id = reader.GetInteger(section, "cluster_id", 0);
    if (cluster_config.cluster_id == 0 && !b_test) {
        FLOG_CRIT("invalid config {}.{}: {}", section, "cluster_id", cluster_config.cluster_id);
        return false;
    }

    cluster_config.node_interval_secs = reader.GetInteger(section, "node_heartbeat_interval", 10);
    if (cluster_config.node_interval_secs <= 0) {
        cluster_config.node_interval_secs = 10;
    }

    cluster_config.range_interval_secs = reader.GetInteger(section, "range_heartbeat_interval", 10);
    if (cluster_config.range_interval_secs <= 0) {
        cluster_config.range_interval_secs = 10;
    }

    auto master_addrs = reader.Get(section, "master_host", "");
    std::stringstream ss(master_addrs);
    std::string host;
    while (std::getline(ss, host, '\n')) {
        cluster_config.master_host.push_back(std::move(host));
    }
    if (master_addrs.empty() && !b_test) {
        FLOG_CRIT("[Config] invalid cluster.master_host: {}", master_addrs);
        return false;
    }

    return true;
}

bool ServerConfig::loadRangeConfig(const INIReader& reader) {
    const char *section = "range";

    bool ret = true;
    std::tie(range_config.worker_threads, ret) = loadThreadNum(reader, section, "worker_threads", 1);
    if (!ret) return false;

    range_config.recover_skip_fail = reader.GetBoolean(section, "recover_skip_fail", true);

    std::tie(range_config.recover_concurrency, ret) =
            loadThreadNum(reader, section, "recover_concurrency", 8);
    if (!ret) return false;

    std::tie(range_config.check_size, ret) =
            loadNeBytesValue(reader, section, "check_size", range_config.check_size);
    if (!ret) return false;

    std::tie(range_config.split_size, ret) =
            loadNeBytesValue(reader, section, "split_size", range_config.split_size);
    if (!ret) return false;

    std::tie(range_config.max_size, ret) =
            loadNeBytesValue(reader, section, "max_size", range_config.max_size);
    if (!ret) return false;

    if (range_config.check_size >= range_config.split_size) {
        FLOG_CRIT("[Config] load range config error, valid config: check_size < split_size; ");
        return false;
    }
    if (range_config.split_size >= range_config.max_size) {
        FLOG_CRIT("[Config] load range config error, valid config: split_size < max_size; ");
        return false;
    }

    range_config.index_split_ratio = reader.GetInteger(section, "index_split_ratio", 3);
    if (range_config.index_split_ratio == 0 || range_config.index_split_ratio > 100) {
        FLOG_CRIT("[Config] load range config error, valid config: index_split_ratio in [0, 100], current: {}; ",
                range_config.index_split_ratio);
        return false;
    }

    return true;
}

bool ServerConfig::loadRaftConfig(const INIReader& reader) {
    const char *section = "raft";
    std::string s_read_option;
    raft_config.disabled = reader.GetBoolean(section, "disabled", false);
    if (raft_config.disabled) {
        FLOG_WARN("[Config] raft is disabled");
    }

    raft_config.ip_addr = reader.Get(section, "ip_addr", "0.0.0.0");

    bool ret = false;
    std::tie(raft_config.port, ret) = loadUInt16(reader, section, "port");
    if (!ret) return false;

    s_read_option = reader.Get(section, "read_option", "read_unsafe");
    if (!s_read_option.compare("lease_only")) {
        raft_config.read_option = jim::raft::LEASE_ONLY;
    } else if(!s_read_option.compare("read_safe")){
        raft_config.read_option = jim::raft::READ_SAFE;
    } else {
        raft_config.read_option = jim::raft::READ_UNSAFE;
    }

    raft_config.in_memory_log = reader.GetBoolean(section, "in_memory_log", false);
    if (raft_config.in_memory_log) {
        FLOG_WARN("[Config] raft use in memory log");
    }

    raft_config.log_path = reader.Get(section, "log_path", "");
    if (!validateDataPath(data_path, kRaftPathSuffix, raft_config.log_path)) {
        return false;
    }

    std::tie(raft_config.log_file_size, ret) =
            loadNeBytesValue(reader, section, "log_file_size", raft_config.log_file_size);
    if (!ret) return false;

    std::tie(raft_config.max_log_files, ret) =
            loadIntegerAtLeast<size_t>(reader, section, "max_log_files", raft_config.max_log_files, 1);
    if (!ret) return false;

    raft_config.allow_log_corrupt = reader.GetBoolean(section, "allow_log_corrupt", true);

    std::tie(raft_config.consensus_threads, ret) =
            loadThreadNum(reader, section, "consensus_threads", 4);
    if (!ret) return false;

    std::tie(raft_config.consensus_queue, ret) =
            loadIntegerAtLeast<size_t>(reader, section, "consensus_queue", 10000, 100);
    if (!ret) return false;

    std::tie(raft_config.transport_send_threads, ret) =
            loadThreadNum(reader, section, "transport_send_threads", 4);
    if (!ret) return false;

    std::tie(raft_config.transport_recv_threads, ret) =
            loadThreadNum(reader, section, "transport_recv_threads", 4);
    if (!ret) return false;

    std::tie(raft_config.connection_pool_size, ret) =
            loadIntegerAtLeast<size_t>(reader, section, "connection_pool_size", 4, 1);
    if (!ret) return false;

    std::tie(raft_config.tick_interval_ms, ret) =
            loadIntegerAtLeast<size_t>(reader, section, "tick_interval", 500, 100);
    if (!ret) return false;

    std::tie(raft_config.max_msg_size, ret) =
            loadNeBytesValue(reader, section, "max_msg_size", 1024 * 1024);
    if (!ret) return false;

    std::tie(raft_config.snapshot_wait_ack_timeout, ret) =
            loadIntegerAtLeast<size_t>(reader, section, "snapshot_wait_ack_timeout", 30, 10);
    if (!ret) return false;
    
    std::tie(raft_config.snapshot_send_concurrency, ret) =
            loadThreadNum(reader, section, "snapshot_send_concurrency", 5);
    if (!ret) return false;

    std::tie(raft_config.snapshot_apply_concurrency, ret) =
            loadThreadNum(reader, section, "snapshot_apply_concurrency", 5);
    if (!ret) return false;


    return ret;
}

bool ServerConfig::loadManagerConfig(const INIReader& reader) {
    const char *section = "manager";
    bool ret = false;
    std::tie(manager_config.port, ret) = loadUInt16(reader, section, "port");
    return ret;
}

bool ServerConfig::loadTestConfig(const INIReader& reader) {
    long node_id;
    long range_id;
    long conf_id;
    long version;
    std::string peers_str;
    std::vector<std::string> peers;
    const char* section = "test";

    node_id = reader.GetInteger(section, "node_id", 1);
    if (node_id <= 0) {
        FLOG_WARN("node id is not invalid, please check");
        return false;
    }
    test_config.node_id = static_cast<uint64_t>(node_id);

    range_id = reader.GetInteger(section, "range_id", 1);
    if (range_id <= 0) {
        FLOG_WARN("range_id is not invalid, please check");
        return false;
    }
    test_config.range_id = static_cast<uint64_t>(range_id);

    conf_id = reader.GetInteger(section, "range_conf_id", 0);
    if (conf_id < 0) {
        FLOG_WARN("conf_id is not invalid, please check!");
        return false;
    }
    test_config.conf_id = static_cast<uint64_t>(conf_id);

    version = reader.GetInteger(section, "range_version", 0);
    if (version < 0) {
        FLOG_WARN("version is not invalid, please check!");
        return false;
    }
    test_config.version = static_cast<uint64_t>(version);

    test_config.start_key = reader.Get(section, "start_key", "");
    if (test_config.start_key.empty()) {
        FLOG_WARN("start key is not invalid, please check");
        return false;
    }

    test_config.end_key = reader.Get(section, "end_key", "");
    if (test_config.end_key.empty()) {
        FLOG_WARN("end key is not invalid, please check");
        return false;
    }

    peers_str = reader.Get(section, "peers", "");
    if (peers_str.empty()) {
        FLOG_WARN("peers is not invalid, please check!");
        return false;
    }
    SplitString(peers_str, ',', peers);
    FLOG_DEBUG("node size is {}", peers.size());
    bool first_node = true;
    for (auto it = peers.begin(); it < peers.end(); it++) {
        std::vector<std::string> v_node;
        SplitString(*it, '@', v_node);
        if(v_node.size() != 2) {
            FLOG_WARN("peers is not invalid, please check");
            return false;
        }
        node_id = strtol(v_node[0].c_str(), NULL, 10);
        test_config.nodes[node_id] = v_node[1];
        if (first_node && test_config.node_id == static_cast<uint64_t>(node_id)) {
            test_config.is_leader = true;
        }
        first_node = false;
    }
    return true;
}

void ServerConfig::Print() const {
    rocksdb_config.Print();
}

} // namespace jim
