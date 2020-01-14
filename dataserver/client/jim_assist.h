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

_Pragma ("once");

#include <thread>
#include <mutex>
#include <queue>
#include <condition_variable>

#include "jim_message.h"

namespace jim {
namespace sdk {

class JimAssist final {
public:
    JimAssist() = default;
    ~JimAssist() = default;

    JimAssist(const JimAssist&) = delete;
    JimAssist& operator=(const JimAssist&) = delete;

    void Start();
    void Stop();

    void AddReSendMsg(MsgPtr msg);
private:
    void runLoop();

    MsgPtr GetMsg();
    void ReSendMsg();
private:
    std::queue<MsgPtr> resend_msg_;

    bool running_ = true;
    std::thread thread_;

    std::mutex mutex_;
    std::condition_variable cond_;
};

using AssistPtr = std::shared_ptr<JimAssist>;

} // namespace sdk
} // namespace jim

