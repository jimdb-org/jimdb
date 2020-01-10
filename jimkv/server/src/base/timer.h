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

#include <memory>
#include <chrono>
#include <thread>
#include <queue>
#include <condition_variable>

namespace jimkv {

class Timer {
public:
    virtual ~Timer() = default;
    virtual void OnTimeout() = 0;
};

class TimerQueue {
public:
    explicit TimerQueue(const std::string& name = "timer");
    virtual ~TimerQueue();

    TimerQueue(const TimerQueue&) = delete;
    TimerQueue& operator=(const TimerQueue&) = delete;

    void Push(const std::shared_ptr<Timer>& timer, int timeout_msec);
    void Stop();

private:
    void run();
    // false means exit
    bool waitOneExpire(std::shared_ptr<Timer>& timer);

private:
    struct Item {
        std::shared_ptr<Timer> timer;
        std::chrono::steady_clock::time_point expire_at;

        bool Expired() const {
            return expire_at <= std::chrono::steady_clock::now();
        }

        bool operator > (const Item& other) const {
            return expire_at > other.expire_at;
        }
    };

    using QueueType = std::priority_queue<Item, std::vector<Item>, std::greater<Item>>;

private:
    bool stopped_ = false;
    QueueType queue_;
    std::mutex mu_;
    std::condition_variable cond_;
    std::thread thr_;
};


} /* namespace jimkv */


