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

#include "worker.h"

#include <stdio.h>
#include <chrono>

#include "dspb/function.pb.h"
#include "base/util.h"
#include "common/logger.h"

#include "range_server.h"

namespace jimkv {
namespace ds {
namespace server {

static const size_t kWorkThreadCapacity = 100000;

Worker::WorkThread::WorkThread(WorkThreadGroup* group, const std::string& name, size_t max_capacity)
    : parent_group_(group) {
    thr_ = std::thread([this] { this->runLoop(); });
    AnnotateThread(thr_.native_handle(), name.c_str());
}

Worker::WorkThread::~WorkThread() {
    if (thr_.joinable()) {
        thr_.join();
    }
}

bool Worker::WorkThread::Push(RPCRequest* msg) {
    que_.push(msg);
    return true;
}

uint64_t Worker::WorkThread::PendingSize() const { return que_.unsafe_size(); }

uint64_t Worker::WorkThread::Clear() {
    uint64_t count = 0;
    RPCRequest* task = nullptr;
    while (running_) {
        if (que_.try_pop(task)) {
            delete task;
            ++count;
        } else {
            break;
        }
    }
    return count;
}

void Worker::WorkThread::runLoop() {
    RPCRequest* task = nullptr;
    while (running_) {
        if (que_.try_pop(task)) {
            parent_group_->DealTask(task);
        } else {
            if (!running_) {
                return;
            }
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
    }
}

Worker::WorkThreadGroup::WorkThreadGroup(RangeServer* rs, int num, size_t capacity_per_thread,
                                         const std::string& name)
    : rs_(rs), thread_num_(num), capacity_(capacity_per_thread), name_(name) {}

Worker::WorkThreadGroup::~WorkThreadGroup() {
    for (const auto& thr : threads_) {
        delete thr;
    }
    threads_.clear();
}

void Worker::WorkThreadGroup::Start() {
    for (int i = 0; i < thread_num_; ++i) {
        char buf[16] = {'\0'};
        snprintf(buf, 16, "%s:%d", name_.c_str(), i);
        auto thr = new WorkThread(this, buf, capacity_);
        threads_.push_back(thr);
    }
}

void Worker::WorkThreadGroup::Stop() {
    for (auto th : threads_) {
        th->Stop();
    }
}

bool Worker::WorkThreadGroup::Push(RPCRequest* msg) {
    auto idx = ++round_robin_counter_ % threads_.size();
    return threads_[idx]->Push(msg);
}

void Worker::WorkThreadGroup::DealTask(RPCRequest* req_ptr) {
    // transfer request ownership to range server
    std::unique_ptr<RPCRequest> req(req_ptr);
    rs_->DealTask(std::move(req));
}

uint64_t Worker::WorkThreadGroup::PendingSize() const {
    uint64_t size = 0;
    for (auto thr : threads_) {
        size += thr->PendingSize();
    }
    return size;
}

uint64_t Worker::WorkThreadGroup::Clear() {
    uint64_t size = 0;
    for (auto thr : threads_) {
        size += thr->Clear();
    }
    return size;
}

Status Worker::Start(int schedule_worker_size, int slow_worker_size, RangeServer* range_server) {
    FLOG_INFO("Worker Start begin, schedule_worker_size: {}, slow_worker_size: {}",
              schedule_worker_size, slow_worker_size);

    range_server_ = range_server;

    schedule_workers_.reset(
        new WorkThreadGroup(range_server, schedule_worker_size, kWorkThreadCapacity, "sched_worker"));
    schedule_workers_->Start();

    slow_workers_.reset(
        new WorkThreadGroup(range_server, slow_worker_size, kWorkThreadCapacity, "slow_worker"));
    slow_workers_->Start();

    FLOG_INFO("Worker Start end ...");

    return Status::OK();
}

void Worker::Stop() {
    schedule_workers_->Stop();
    slow_workers_->Stop();
}

void Worker::Push(RPCRequest* task) {
    if (task->sch_req) {
        schedule_workers_->Push(task);
    } else {
        slow_workers_->Push(task);
    }
}

void Worker::Deal(RPCRequest* req) {
    std::unique_ptr<RPCRequest> req_ptr(req);
    range_server_->DealTask(std::move(req_ptr));
}

size_t Worker::ClearQueue(bool schedule, bool slow) {
    size_t count = 0;
    if (schedule) {
        count += schedule_workers_->Clear();
    }
    if (slow) {
        count += slow_workers_->Clear();
    }
    return count;
}

void Worker::PrintQueueSize() {
    FLOG_INFO("worker schedule queue size:{}", schedule_workers_->PendingSize());
    FLOG_INFO("worker slow queue size:{}", slow_workers_->PendingSize());
}

} /* namespace server */
} /* namespace ds  */
} /* namespace jimkv */
