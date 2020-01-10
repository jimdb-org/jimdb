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

#include "config.h"

#include <iostream>
#include <sstream>
#include "common/logger.h"
#include "INIReader.h"

namespace jimkv {
namespace raft {
namespace bench {

BenchConfig bench_config;

bool BenchConfig::LoadFromFile(const std::string& conf_file) {
    INIReader reader(conf_file);
    if (reader.ParseError() < 0) {
        FLOG_CRIT("parse {} failed: {}", conf_file, reader.ParseError());
        return false;
    }

    // request config
    range_num = reader.GetInteger("", "range_num", 1);
    requets_num = reader.GetInteger("", "request_num", 1000000);
    bench_threads = reader.GetInteger("", "bench_threads", 4);
    bench_concurrency = reader.GetInteger("", "bench_concurrency", 100);


    // logger config
    {
        const char *section = "logger";
        logger_config.path = reader.Get(section, "path", logger_config.path);
        logger_config.level = reader.Get(section, "level", logger_config.level);
    }

    // raft config
    {
        const char *section = "raft";
        raft_config.log_path = reader.Get(section, "log_path", "./logs");
        raft_config.use_memory_log = reader.GetBoolean(section, "use_memory_log", false);
        raft_config.use_inprocess_transport = reader.GetBoolean(section, "use_inprocess_transport", false);
        raft_config.work_threads = reader.GetInteger(section, "work_threads", 4);
        raft_config.work_threads = std::min(raft_config.work_threads, range_num);
    }

    return true;
}

void BenchConfig::Print() {
    std::ostringstream ss;
    ss << "use_memory_log: \t" << raft_config.use_memory_log<< std::endl;
    ss << "use_inprocess_transport: \t" << raft_config.use_inprocess_transport << std::endl;
    ss << "raft threads: \t" << raft_config.work_threads << std::endl;
    ss << "range_num: \t" << range_num << std::endl;
    ss << "request num: \t" << requets_num << std::endl;
    ss << "bench threads: \t" << bench_threads << std::endl;
    ss << "bench concurrency: \t" << bench_concurrency << std::endl;

    std::cout << "configs: " << std::endl;
    std::cout << ss.str() << std::endl;
}

} /* namespace bench */
} /* namespace raft */
} /* namespace jimkv */