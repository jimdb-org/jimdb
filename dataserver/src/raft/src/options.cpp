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

#include "raft/options.h"

namespace jim {
namespace raft {

Status TransportOptions::Validate() const {
    if (use_inprocess_transport) return Status::OK();

    if (listen_port == 0) {
        return Status(Status::kInvalidArgument, "raft transport options", "listen port");
    }
    if (!resolver) {
        return Status(Status::kInvalidArgument, "raft transport options",
                      "node resolver");
    }
    if (send_io_threads == 0) {
        return Status(Status::kInvalidArgument, "raft transport options",
                      "send_io_threads");
    }
    if (recv_io_threads == 0) {
        return Status(Status::kInvalidArgument, "raft transport options",
                      "recv_io_threads");
    }
    return Status::OK();
}

Status SnapshotOptions::Validate() const {
    if (max_send_concurrency == 0) {
        return Status(Status::kInvalidArgument, "raft snapshot options",
                      "max_send_concurrency");
    }
    if (max_apply_concurrency == 0) {
        return Status(Status::kInvalidArgument, "raft snapshot options",
                      "max_apply_concurrency");
    }
    if (max_size_per_msg == 0) {
        return Status(Status::kInvalidArgument, "raft snapshot options",
                      "max_size_per_msg");
    }
    return Status::OK();
}

Status RaftServerOptions::Validate() const {
    if (node_id == 0) {
        return Status(Status::kInvalidArgument, "raft server options", "node id");
    }

    if (auto_promote_learner) {
        if (promote_gap_threshold == 0) {
            return Status(Status::kInvalidArgument, "raft server options",
                          "caught up threshold");
        }
        if (promote_gap_percent == 0 || promote_gap_percent >= 100) {
            return Status(Status::kInvalidArgument, "raft server options",
                          "caught up percent");
        }
    }

    if (heartbeat_tick == 0) {
        return Status(Status::kInvalidArgument, "raft server options", "heartbeat tick");
    }
    if (election_tick <= heartbeat_tick) {
        return Status(Status::kInvalidArgument, "raft server options", "election tick");
    }

    if (max_inflight_msgs <= 0) {
        return Status(Status::kInvalidArgument, "raft server options",
                      "max inflight msgs");
    }
    if (max_size_per_msg == 0) {
        return Status(Status::kInvalidArgument, "raft server options",
                      "max size per msg");
    }

    if (consensus_threads_num == 0) {
        return Status(Status::kInvalidArgument, "raft server options",
                      "consensus threads num");
    }
    if (consensus_queue_capacity <= 0) {
        return Status(Status::kInvalidArgument, "raft server options",
                      "consensus queue capacity");
    }

    auto s = snapshot_options.Validate();
    if (!s.ok()) return s;

    s = transport_options.Validate();
    if (!s.ok()) return s;

    return Status::OK();
}

Status RaftOptions::Validate() const {
    if (id == 0) {
        return Status(Status::kInvalidArgument, "raft options", "id");
    }

    if (peers.empty()) {
        return Status(Status::kInvalidArgument, "raft options", "peers");
    }

    if (!statemachine) {
        return Status(Status::kInvalidArgument, "raft options", "statemachine");
    }

    if (!use_memory_storage) {
        if (storage_path.empty()) {
            return Status(Status::kInvalidArgument, "raft options",
                          "logger storage path");
        }
        if (log_file_size == 0) {
            return Status(Status::kInvalidArgument, "raft options", "log file size");
        }
        if (max_log_files == 0) {
            return Status(Status::kInvalidArgument, "raft options", "max log files");
        }
    }

    if (leader != 0) {
        // check the specified leader is in peers
        bool found = false;
        for (const auto& p : peers) {
            if (p.node_id == leader) {
                found = true;
                break;
            }
        }
        if (!found) {
            return Status(Status::kInvalidArgument, "raft options",
                          "could not find specified in peers");
        }

        // a valid term is required
        if (term == 0) {
            return Status(Status::kInvalidArgument, "raft options",
                          "a valid term is required when a leader is specified");
        }
    }

    return Status::OK();
}

} /* namespace raft */
} /* namespace jim */
