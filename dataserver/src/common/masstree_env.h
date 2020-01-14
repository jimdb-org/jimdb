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

#include "masstree-beta/config.h"
#include "masstree-beta/kvthread.hh"

namespace jim {

class RCUGuard final {
public:
    RCUGuard();
    ~RCUGuard();

    RCUGuard(const RCUGuard&) = delete;
    RCUGuard& operator=(const RCUGuard&) = delete;
};

threadinfo& GetThreadInfo();

uint64_t ThreadCounter(threadcounter c);

void EpochIncr();

void RegisterRCU(mrcu_callback* cb);

void RunRCUFree(bool wait = true);

void StartRCUThread(size_t interval_ms);

} // namespace jim
