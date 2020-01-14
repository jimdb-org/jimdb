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

#include <condition_variable>
#include <mutex>
#include <queue>
#include <thread>
#include <atomic>
#include <functional>

namespace jim {
namespace raft {
namespace impl {

using Work = std::function<void()>;

class WorkThread {
public:
    WorkThread(size_t queue_capcity, const std::string& name = "raft-worker");
    ~WorkThread();

    WorkThread(const WorkThread&) = delete;
    WorkThread& operator=(const WorkThread&) = delete;

    bool tryPost(Work&& work);

    void post(Work&& work);

    void waitPost(Work&& work);

    void shutdown();

    size_t size() const;

    bool isFull() const;

    bool inCurrentThread() const;

private:
    void run();

private:
    const size_t capacity_ = 0;

    bool running_ = false;
    std::thread::id tid_;

    std::queue<Work> queue_;
    std::atomic<size_t> que_size_ = {0};
    mutable std::mutex mu_;
    std::condition_variable cv_;
    std::unique_ptr<std::thread> thr_;
};

} /* namespace impl */
} /* namespace raft */
} /* namespace jim */
