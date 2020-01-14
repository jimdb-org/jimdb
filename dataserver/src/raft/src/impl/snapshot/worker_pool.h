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

#include <memory>
#include <vector>
#include <list>
#include <map>
#include <mutex>
#include "task.h"

namespace jim {
namespace raft {
namespace impl {

class SnapWorker;

class SnapWorkerPool final {
public:
    SnapWorkerPool(const std::string& name, size_t size);
    ~SnapWorkerPool();

    SnapWorkerPool(const SnapWorkerPool&) = delete;
    SnapWorkerPool& operator=(const SnapWorkerPool&) = delete;

    bool Post(const SnapTaskPtr& task);

    size_t RunningsCount() const;

    void GetRunningTasks(std::vector<SnapTaskPtr> *tasks);

private:
    class FreeList {
    public:
        void Add(SnapWorker *w);
        SnapWorker* Get();

    private:
        std::list<SnapWorker*> workers_;
        std::mutex mu_;
    };

    class RunningMap {
    public:
        size_t Size() const;
        void Add(SnapTaskPtr task);
        void Remove(const SnapTaskPtr& task);
        void GetAll(std::vector<SnapTaskPtr> *tasks);

    private:
        std::map<std::string, SnapTaskPtr> tasks_;
        mutable std::mutex mu_;
    };

private:
    friend class SnapWorker;

    void addFreeWorker(SnapWorker* w);

    void addRunning(const SnapTaskPtr& task);
    void removeRunning(const SnapTaskPtr& task);

private:
    std::vector<SnapWorker*> all_workers_;

    FreeList free_workers_;
    RunningMap running_tasks_;
};

} /* namespace impl */
} /* namespace raft */
} /* namespace jim */
