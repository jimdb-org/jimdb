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

#include <gtest/gtest.h>

#include "base/timer.h"
#include "base/util.h"
#include "base/status.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {

using namespace jimkv;

class TestTimer : public Timer {
public:
    explicit TestTimer(int timeout_ms) : timeout_millisecs_(timeout_ms) {
        expired_at_ = std::chrono::steady_clock::now() +
                std::chrono::milliseconds(timeout_ms);
    }
    ~TestTimer() = default;

    void OnTimeout() override {
        std::lock_guard<std::mutex> lock(mu_);
        notified_ = true;
        notifed_at_ = std::chrono::steady_clock::now();
        cond_.notify_one();
    };

    Status WaitNotify() {
        std::unique_lock<std::mutex> lock(mu_);
        auto ret = cond_.wait_until(lock,
                expired_at_ + std::chrono::milliseconds(10), [this]{ return notified_; });
        if (!ret) {
            return Status(Status::kTimedOut, "wait no result", "");
        }
        if (notifed_at_ < expired_at_) {
            return Status(Status::kUnexpected, "too early", "");
        }
        auto delay = std::chrono::duration_cast<std::chrono::milliseconds>(notifed_at_ - expired_at_).count();
        if (delay > 10) {
            return Status(Status::kTimedOut, "too late", std::to_string(delay));
        }
        return Status::OK();
    }

    int GetTimeoutMS() const { return timeout_millisecs_; }

private:
    int timeout_millisecs_ = 0;
    std::chrono::steady_clock::time_point expired_at_;
    std::chrono::steady_clock::time_point notifed_at_;
    bool notified_ = false;
    std::mutex mu_;
    std::condition_variable cond_;
};

TEST(Timer, Basic) {
    TimerQueue queue;
    std::vector<std::shared_ptr<TestTimer>> timers;
    for (auto i = 0; i < 10000; ++i) {
        auto timeout = 50 + randomInt() % 500;
        auto timer = std::make_shared<TestTimer>(timeout);
        queue.Push(timer, timeout);
        timers.push_back(timer);
    }
    for (const auto& timer: timers) {
        auto s = timer->WaitNotify();
        EXPECT_TRUE(s.ok()) << s.ToString() << ", timeout: " << timer->GetTimeoutMS();
    }
}


} /* namespace  */
