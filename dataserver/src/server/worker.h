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

#include <thread>
#include <vector>
#include <atomic>
#include <tbb/concurrent_queue.h>

#include "common/rpc_request.h"

namespace jim {
namespace ds {
namespace server {

class RangeServer;

using RPCHandler = std::function<void(RPCRequest*)>;

class Worker final {
public:
    Worker() = default;
    ~Worker() = default;

    Worker(const Worker &) = delete;
    Worker &operator=(const Worker &) = delete;

    Status Start(int schedule_worker_size, int slow_worker_size, RangeServer* range_server);
    void Stop();

    void Push(RPCRequest* req);
    void Deal(RPCRequest* req);

    void PrintQueueSize();
    size_t ClearQueue(bool schedule, bool slow);
    uint64_t ScheduleQueueSize() const { return schedule_workers_->PendingSize(); }
    uint64_t SlowQueueSize() const { return slow_workers_->PendingSize(); }

private:
    class WorkThreadGroup;

    class WorkThread {
    public:
        WorkThread(WorkThreadGroup* group, const std::string& name, size_t max_capacity);
        ~WorkThread();

        WorkThread(const WorkThread&) = delete;
        WorkThread& operator=(const WorkThread&) = delete;

        void Stop() {running_ = false;}
        bool Push(RPCRequest* req);
        uint64_t PendingSize() const;
        uint64_t Clear();

    private:
        void runLoop();

    private:
        bool running_ = true;
        WorkThreadGroup* parent_group_ = nullptr;
        tbb::concurrent_queue<RPCRequest*> que_;
        std::thread thr_;
    };

    class WorkThreadGroup {
    public:
        WorkThreadGroup(RangeServer* rs, int num, size_t capacity_per_thread, const std::string& name);
        ~WorkThreadGroup();

        WorkThreadGroup(const WorkThreadGroup&) = delete;
        WorkThreadGroup& operator=(const WorkThreadGroup&) = delete;

        void Start();
        void Stop();
        bool Push(RPCRequest* msg);
        void DealTask(RPCRequest* req_ptr);
        uint64_t PendingSize() const;
        uint64_t Clear();

    private:
        RangeServer* const rs_;
        const int thread_num_ = 0;
        const size_t capacity_ = 0;
        const std::string name_;

        std::atomic<uint64_t> round_robin_counter_ = {0};
        std::vector<WorkThread*> threads_;
    };

private:
    RangeServer *range_server_ = nullptr;
    std::unique_ptr<WorkThreadGroup> schedule_workers_; // handle requests from master
    std::unique_ptr<WorkThreadGroup> slow_workers_; //  handle slow range request
};

} /* namespace server */
} /* namespace ds  */
} /* namespace jim */
