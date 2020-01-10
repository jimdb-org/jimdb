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

_Pragma("once");

#include <atomic>
#include <chrono>
#include <string>

namespace jimkv {
namespace ds {
namespace storage {

struct MetricStat {
    uint64_t keys_read_per_sec = 0;
    uint64_t keys_write_per_sec = 0;
    uint64_t bytes_read_per_sec = 0;
    uint64_t bytes_write_per_sec = 0;

    std::string ToString() const;
};

class Metric {
public:
    Metric();
    ~Metric();

    void AddRead(uint64_t keys, uint64_t bytes);
    void AddWrite(uint64_t keys, uint64_t bytes);

    void Reset();

    // should only called by one thread
    void Collect(MetricStat* stat);

    // collect gobal metric
    static void CollectAll(MetricStat* stat);

private:
    using TimePoint = std::chrono::time_point<std::chrono::steady_clock>;

    std::atomic<uint64_t> keys_read_counter_{0};
    std::atomic<uint64_t> keys_write_counter_{0};
    std::atomic<uint64_t> bytes_read_counter_{0};
    std::atomic<uint64_t> bytes_write_counter_{0};

    TimePoint last_collect_;
};

extern Metric g_metric;

}  // namespace storage
}  // namespace ds
}  // namespace jimkv
