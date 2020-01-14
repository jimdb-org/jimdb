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

#include <cstdint>
#include <string>

namespace jim {
namespace raft {
namespace bench {

struct BenchConfig {
    std::size_t range_num = 1;
    std::size_t requets_num = 1000000;
    std::size_t bench_threads = 4;
    std::size_t bench_concurrency = 100;

    // logger config
    struct {
        std::string path = "./logs";
        std::string level = "info";
    } logger_config;

    struct {
        std::string log_path = "./raft";
        bool use_memory_log = false;
        bool use_inprocess_transport = false;
        std::size_t work_threads = 4;
    } raft_config;

    bool LoadFromFile(const std::string& file);
    void Print();
};

extern BenchConfig bench_config;

} /* namespace bench */
} /* namespace raft */
} /* namespace jim */
