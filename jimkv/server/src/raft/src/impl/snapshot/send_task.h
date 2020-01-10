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

#include <atomic>
#include <condition_variable>
#include <mutex>

#include "raft/include/raft/snapshot.h"

#include "../transport/transport.h"
#include "task.h"

namespace jimkv {
namespace raft {
namespace impl {

class SendSnapTask : public SnapTask {
public:
    struct Options {
        size_t max_size_per_msg = 64 * 1024;
        size_t wait_ack_timeout_secs = 10;
    };

    SendSnapTask(const SnapContext& context, pb::SnapshotMeta&& meta,
                 const std::shared_ptr<Snapshot>& data);
    ~SendSnapTask();

    void SetTransport(transport::Transport* trans) { transport_ = trans; }
    void SetOptions(const Options& opt) { opt_ = opt; }

    Status RecvAck(MessagePtr& msg);

    void Cancel() override;
    bool IsCanceled() const { return canceled_; }

    std::string Description() const override;

private:
    void run(SnapResult* result) override;

    Status waitAck(int64_t seq, size_t timeout_secs);
    Status nextMsg(int64_t seq, MessagePtr& msg, bool& over);

private:
    pb::SnapshotMeta meta_;
    std::shared_ptr<Snapshot> data_;

    transport::Transport* transport_ = nullptr;
    Options opt_;

    std::atomic<int64_t> ack_seq_ = {0};
    bool rejected_ = false;
    std::atomic<bool> canceled_ = {false};
    mutable std::mutex mu_;
    std::condition_variable cv_;
};

} /* namespace impl */
} /* namespace raft */
} /* namespace jimkv */
