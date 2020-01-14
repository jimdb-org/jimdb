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

#include <atomic>
#include <mutex>
#include <condition_variable>

#include "raft/include/raft/statemachine.h"
#include "../transport/transport.h"

#include "task.h"
#include "types.h"

namespace jim {
namespace raft {
namespace impl {

class ApplySnapTask : public SnapTask {
public:
    struct Options {
        size_t wait_data_timeout_secs = 10;
    };

    ApplySnapTask(const SnapContext& context,
            const std::shared_ptr<StateMachine>& sm);
    ~ApplySnapTask();

    void SetOptions(const Options& opt) { opt_ = opt; }
    void SetTransport(transport::Transport* transport) { transport_= transport; }

    Status RecvData(const MessagePtr& msg);

    void Cancel() override;
    bool IsCanceled() const { return canceled_; }

    std::string Description() const override;

private:
    void run(SnapResult* result) override;

    Status waitNextData(MessagePtr* data);
    Status applyData(MessagePtr data, bool &over);
    void sendAck(int64_t seq, bool reject = false);

private:
    std::shared_ptr<StateMachine> sm_;

    transport::Transport *transport_ = nullptr;
    Options opt_;

    std::atomic<bool> canceled_ = {false};
    std::atomic<int64_t> prev_seq_ = {0};
    MessagePtr next_data_;
    mutable std::mutex mu_;
    std::condition_variable cv_;

    uint64_t snap_index_ = 0;
};

} /* namespace impl */
} /* namespace raft */
} /* namespace jim */
