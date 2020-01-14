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

#include "worker.h"

#include <cassert>
#include "base/util.h"

#include "task.h"
#include "worker_pool.h"

namespace jim {
namespace raft {
namespace impl {

SnapWorker::SnapWorker(SnapWorkerPool* pool, const std::string& name) : pool_(pool) {
    thr_ = std::thread([this] { runTask(); });
    AnnotateThread(thr_.native_handle(), name.c_str());
}

SnapWorker::~SnapWorker() {
    {
        std::lock_guard<std::mutex> lock(mu_);
        running_ = false;
        if (task_ != nullptr) {
            task_->Cancel();
        }
    }
    cv_.notify_one();
    thr_.join();
}

void SnapWorker::Post(const std::shared_ptr<SnapTask>& task) {
    {
        std::lock_guard<std::mutex> lock(mu_);
        assert(task_ == nullptr);
        task_ = task;
    }
    cv_.notify_one();
}

void SnapWorker::runTask() {
    while (true) {
        std::shared_ptr<SnapTask> task;
        {
            std::unique_lock<std::mutex> lock(mu_);
            while (task_ == nullptr && running_) {
                cv_.wait(lock);
            }
            if (!running_) return;
            task = std::move(task_);
            task_ = nullptr;
        }

        pool_->addRunning(task);
        task->Run();
        pool_->removeRunning(task);

        // mark as free
        pool_->addFreeWorker(this);
    }
}

} /* namespace impl */
} /* namespace raft */
} /* namespace jim */
