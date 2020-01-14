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

#include "timer.h"

#include "base/util.h"

namespace jim {

TimerQueue::TimerQueue(const std::string& name) {
    thr_ = std::thread([this] { run(); });
    AnnotateThread(thr_.native_handle(), name.c_str());
}

TimerQueue::~TimerQueue() {
    Stop();
}

void TimerQueue::Push(const std::shared_ptr<Timer>& timer, int timeout_msec) {
    Item item;
    item.timer = timer;
    item.expire_at = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_msec);

    std::lock_guard<std::mutex> lock(mu_);
    queue_.push(item);
    cond_.notify_one();
}

void TimerQueue::Stop() {
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (stopped_) return;
        stopped_ = true;
    }
    cond_.notify_one();
    thr_.join();
}

bool TimerQueue::waitOneExpire(std::shared_ptr<Timer>& timer) {
    std::unique_lock<std::mutex> lock(mu_);
    while (true) {
        if (!stopped_ && queue_.empty()) {
            cond_.wait(lock);
        }
        if (stopped_) return false;
        if (queue_.top().Expired()) {
            timer = queue_.top().timer;
            queue_.pop();
            return true;
        } else {
            cond_.wait_until(lock, queue_.top().expire_at);
        }
    }
}

void TimerQueue::run() {
    while (true) {
        std::shared_ptr<Timer> sp_timer;
        auto ret = waitOneExpire(sp_timer);
        if (!ret) { // over
            return;
        }
        try {
            if (sp_timer) {
                sp_timer->OnTimeout();
            }
        } catch (std::exception& e) {
            fprintf(stderr, "OnTimeout exception: %s", e.what());
        }
    }
}


} /* namespace jim */

