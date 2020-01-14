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

#include <atomic>
#include <thread>
#include <vector>
#include <functional>

#include "asio/executor_work_guard.hpp"
#include "asio/io_context.hpp"

namespace jim {
namespace net {

class IOContextPool final {
public:
    // size: pool(threads) size
    IOContextPool(size_t size, const std::string& name);
    ~IOContextPool();

    IOContextPool(const IOContextPool&) = delete;
    IOContextPool& operator=(const IOContextPool&) = delete;

    void Start();
    void Stop();

    size_t Size() const { return pool_size_; }

    // must check Size()>0 before
    asio::io_context& GetIOContext();
    asio::io_context& GetIOContext(uint32_t round_num);

private:
    void runLoop(const std::shared_ptr<asio::io_context>& ctx, int i);

private:
    using WorkGuard = asio::executor_work_guard<asio::io_context::executor_type>;

    const size_t pool_size_ = 0;
    const std::string pool_name_;

    std::vector<std::shared_ptr<asio::io_context>> io_contexts_;
    std::vector<WorkGuard> work_guards_;
    std::atomic<bool> stopped_ = { false };

    std::atomic<uint64_t> round_robin_counter_ = {0};

    std::vector<std::thread> threads_;
};

}  // namespace net
}  // namespace jim
