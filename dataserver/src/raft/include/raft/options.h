// Copyright 2015 The etcd Authors
// Portions Copyright 2019 The JIMDB Authors.
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

#include <chrono>
#include <memory>
#include <vector>

#include "raft/node_resolver.h"
#include "raft/statemachine.h"

namespace jim {
namespace raft {

const uint16_t LEASE_ONLY = 1;
const uint16_t READ_SAFE = 2;
const uint16_t READ_UNSAFE = 3;

const uint16_t READ_SUCCESS = 0; //read verify success, majority followers return success
const uint16_t READ_FAILURE = 1; //read verify failed, majority followers return failed
const uint16_t READ_WAIT = 2;    //verify wait, return number less than majority

const uint16_t READ_FLAG = 1;

const uint16_t WRITE_FLAG = 2;

struct TransportOptions {
    bool use_inprocess_transport = false;

    std::string listen_ip = "0.0.0.0";

    uint16_t listen_port = 0;

    std::shared_ptr<NodeResolver> resolver;

    size_t send_io_threads = 4;

    size_t recv_io_threads = 4;

    size_t connection_pool_size = 4;

    Status Validate() const;
};

struct SnapshotOptions {
    uint8_t max_send_concurrency = 5;

    uint8_t max_apply_concurrency = 5;

    size_t max_size_per_msg = 64 * 1024;

    size_t ack_timeout_seconds = 30;

    Status Validate() const;
};

struct RaftServerOptions {
    uint64_t node_id = 0;

    bool enable_pre_vote = true;
    uint16_t read_option = LEASE_ONLY;
    bool auto_promote_learner = true;
    uint64_t promote_gap_threshold = 500;
    uint64_t promote_gap_percent = 5;

    std::chrono::milliseconds tick_interval = std::chrono::milliseconds(500);

    unsigned heartbeat_tick = 1;

    unsigned election_tick = 5;

    unsigned inactive_tick = 10;

    unsigned status_tick = 4;

    int max_inflight_msgs = 128;

    uint64_t max_size_per_msg = 1024 * 1024;

    int entry_batch_size = 64;

    uint8_t consensus_threads_num = 4;
    size_t consensus_queue_capacity = 100000;

    TransportOptions transport_options;
    SnapshotOptions snapshot_options;

    Status Validate() const;
};

struct RaftOptions {
    // raft group id
    uint64_t id = 0;

    std::vector<Peer> peers;

    std::shared_ptr<StateMachine> statemachine;

    bool use_memory_storage = false;

    std::string storage_path;
    size_t log_file_size = 1024 * 1024 * 16;
    size_t max_log_files = 5;
    bool allow_log_corrupt = false;

    uint64_t applied = 0;
    uint64_t leader = 0;
    uint64_t term = 0;

    Status Validate() const;
};

} /* namespace raft */
} /* namespace jim */
