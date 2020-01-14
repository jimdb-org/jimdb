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

#include "work_thread.h"

#include "base/util.h"
#include "common/logger.h"

namespace jim {
namespace raft {
namespace impl {

WorkThread::WorkThread(size_t queue_capcity, const std::string& name)
    : capacity_(queue_capcity), running_(true) {
    assert(capacity_ > 0);

    thr_.reset(new std::thread(std::bind(&WorkThread::run, this)));
    AnnotateThread(thr_->native_handle(), name.c_str());
    tid_ = thr_->get_id();
}

WorkThread::~WorkThread() { shutdown(); }

bool WorkThread::tryPost(Work&& work) {
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (!running_) return false;
        if (queue_.size() >= capacity_) {
            FLOG_WARN("queue is full!!!, please wait");
            return false;
        } else {
            queue_.push(std::move(work));
            ++que_size_;
        }
    }
    cv_.notify_one();
    return true;
}

void WorkThread::post(Work&& work) {
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (!running_) {
            return;
        }
        queue_.push(std::move(work));
        ++que_size_;
    }
    cv_.notify_one();
}

void WorkThread::waitPost(Work&& work) {
    std::unique_lock<std::mutex> lock(mu_);
    while (queue_.size() >= capacity_ && running_) {
        cv_.wait(lock);
    }

    if (running_) {
        queue_.push(std::move(work));
        ++que_size_;
        lock.unlock();
        cv_.notify_one();
    }
}

void WorkThread::shutdown() {
    {
        std::unique_lock<std::mutex> lock(mu_);
        if (!running_) return;
        running_ = false;
    }
    cv_.notify_one();
    thr_->join();
}

void WorkThread::run() {
    std::queue<Work> works;
    while (running_) {
        std::unique_lock<std::mutex> lock(mu_);

        if (queue_.empty()) {
            cv_.wait(lock);
        }

        // batch pull works
        works.swap(queue_);
        que_size_ = 0;
        lock.unlock();
        cv_.notify_one(); // notify queue is not full

        while (!works.empty()) {
            works.front()();
            works.pop();
        }
    }
}

size_t WorkThread::size() const {
    return que_size_;
}

bool WorkThread::isFull() const {
    return que_size_ >= capacity_;
}

bool WorkThread::inCurrentThread() const {
    return std::this_thread::get_id() == this->tid_;
}

} /* namespace impl */
} /* namespace raft */
} /* namespace jim */
