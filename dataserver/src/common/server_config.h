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

_Pragma("once");

#include <string>
#include <vector>

#include "common/logger.h"
#include "common/rocksdb_config.h"
#include "common/masstree_options.h"
#include "raft/options.h"

namespace jim {

enum class EngineType {
    kInvalid = 0,
    kMassTree = 1,
    kRocksdb = 2,
};

std::string EngineTypeName(EngineType et);

struct ServerConfig {
    // write pid and lock file for exclusively running
    std::string pid_file;

    // server run in test mode
    bool b_test = false;

    // server run on docker
    bool docker = false;

    // base data path,
    // if some type of data path is not configured, will use {data_path}/{type_suffix} as default
    std::string data_path;

    // path to store meta datas, such as range meta
    // can be empty, default: {data_path}/meta
    std::string meta_path;

    // engine configs
    EngineType engine_type = EngineType::kInvalid;
    MasstreeOptions masstree_config;
    RocksDBConfig rocksdb_config;

    // cluster configs
    struct {
        uint64_t cluster_id = 0; // cluster id
        std::vector<std::string> master_host; // master server host:port
        int node_interval_secs = 10;   // node heartbeat interval
        int range_interval_secs = 10;  // range heartbeat interval, in seconds
    } cluster_config;

    struct {
        uint64_t node_id;
        uint64_t range_id;
        bool is_leader;
        std::map<uint64_t, std::string> nodes;
        std::string start_key;
        std::string end_key;
        uint64_t conf_id;
        uint64_t version;
    } test_config;

    // logging configs
    LoggerConfig logger_config;

    struct {
        uint16_t port = 0;
        size_t io_threads_num = 10;
        std::string ip_address = "0.0.0.0";
    } rpc_config;

    struct {
        size_t schedule_worker_num = 4;  // schedule worker thread num; eg. request from master
        size_t slow_worker_num = 4;  // fast worker thread num; eg. put/get command
        size_t task_timeout_ms = 10000; // default 10s
    } worker_config;

    struct {
        bool recover_skip_fail = true;
        size_t recover_concurrency = 4;
        bool enable_split = true;
        uint64_t check_size = 32UL * 1024 * 1024;
        uint64_t split_size = 64UL * 1024 * 1024;;
        uint64_t max_size = 128UL * 1024 * 1024;
        size_t index_split_ratio = 3; // index range's split_size = {split_size} / index_split_ratio
        int worker_threads = 1;
    } range_config;

    struct {
        bool disabled = false;
        std::string ip_addr = "0.0.0.0";
        uint16_t port = 0;  // raft server port
        uint16_t read_option = jim::raft::READ_UNSAFE;
        bool in_memory_log = false;
        std::string log_path; // default: {data_path}/raft
        uint64_t log_file_size = 16 * 1024 * 1024; // default: 16MB
        size_t max_log_files = 5;
        bool allow_log_corrupt = true;
        size_t consensus_threads = 4;
        size_t consensus_queue = 10000;
        size_t transport_send_threads = 4;
        size_t transport_recv_threads = 4;
        size_t connection_pool_size = 4;
        size_t tick_interval_ms = 500;
        size_t max_msg_size = 1024 * 1024; // byte
        size_t snapshot_wait_ack_timeout = 30; // in seconds
        size_t snapshot_send_concurrency = 5; // send threads
        size_t snapshot_apply_concurrency = 5; // apply threads
    } raft_config;

    struct {
        std::string ip_addr = "0.0.0.0";
        uint16_t port = 0;
    } manager_config;

public:
    bool LoadFromFile(const std::string& conf_file, bool btest);
    void Print() const;

private:
    bool load(const INIReader& reader);
    bool loadLoggerConfig(const INIReader& reader);
    bool loadRPCConfig(const INIReader& reader);
    bool loadEngineConfig(const INIReader& reader);
    bool loadWorkerConfig(const INIReader& reader);
    bool loadClusterConfig(const INIReader& reader);
    bool loadRangeConfig(const INIReader& reader);
    bool loadRaftConfig(const INIReader& reader);
    bool loadManagerConfig(const INIReader& reader);
    bool loadTestConfig(const INIReader& reader);
};

extern ServerConfig ds_config;

} // namespace jim

