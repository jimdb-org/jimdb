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

#include "masstree_env.h"

#include <unistd.h>
#include <memory>
#include <mutex>
#include <thread>

#include "base/util.h"

volatile mrcu_epoch_type globalepoch = 1;     // global epoch, updated by main thread regularly
volatile mrcu_epoch_type active_epoch = 1;
kvtimestamp_t initial_timestamp = 0;

namespace jim {

static std::mutex g_thread_info_mutex;

static threadinfo* createThreadInfo() noexcept {
    std::lock_guard<std::mutex> lock(g_thread_info_mutex);
    return threadinfo::make(threadinfo::TI_PROCESS, -1);
}

struct ThreadInfoDeleter {
    void operator()(threadinfo*) const {}
};

thread_local static std::unique_ptr<threadinfo, ThreadInfoDeleter> g_thread_info(createThreadInfo());

RCUGuard::RCUGuard() {
    // TODO: check reentry
    g_thread_info->rcu_start();
}

RCUGuard::~RCUGuard() {
    g_thread_info->rcu_stop();
}

threadinfo& GetThreadInfo() {
    return *g_thread_info;
}

uint64_t ThreadCounter(threadcounter c) {
    uint64_t count = 0;
    std::lock_guard<std::mutex> lock(g_thread_info_mutex);
    for (threadinfo* ti = threadinfo::allthreads; ti; ti = ti->next()) {
        prefetch((const void*) ti->next());
        count += ti->counter(c);
    }
    return count;
}

void EpochIncr() {
    globalepoch += 1;
    auto& ti = *g_thread_info; // ensure created,
    {
        std::lock_guard<std::mutex> lock(g_thread_info_mutex);
        active_epoch = ti.min_active_epoch();
    }
}

void RegisterRCU(mrcu_callback* cb) {
    g_thread_info->rcu_register(cb);
}

void RunRCUFree(bool wait) {
    auto prev_epoch = globalepoch;
    EpochIncr();
    while (wait && active_epoch <= prev_epoch) {
        usleep(100);
        EpochIncr();
    }
    g_thread_info->rcu_start();
    g_thread_info->rcu_stop();
}

void StartRCUThread(size_t interval_ms) {
    std::thread t([=] {
        while (true) {
            EpochIncr();
            std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
    }});
    AnnotateThread(t.native_handle(), "rcu-epoch");
    t.detach();
}

} // namespace jim
