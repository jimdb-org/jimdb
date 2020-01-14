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

#include <sys/time.h>
#include <unistd.h>
#include <getopt.h>
#include <future>
#include <iostream>
#include <thread>
#include <vector>
#include <functional>

#include "address.h"
#include "config.h"
#include "node.h"
#include "common/logger.h"

using namespace jim;
using namespace jim::raft;
using namespace jim::raft::bench;

struct BenchContext {
    std::atomic<int64_t> counter = {0};
    std::vector<std::shared_ptr<Range>> leaders;
};

void runBenchmark(BenchContext *ctx) {
    while (true) {
        std::vector<std::shared_future<bool>> futures;
        for (size_t i = 0; i < bench_config.bench_concurrency; ++i) {
            auto num = ctx->counter.fetch_sub(1);
            if (num > 0) {
                futures.push_back(
                    (ctx->leaders)[num % ctx->leaders.size()]->AsyncRequest());
            } else {
                break;
            }
        }
        if (!futures.empty()) {
            for (auto &f : futures) {
                f.wait();
                assert(f.valid());
                assert(f.get());
            }
        } else {
            return;
        }
    }
}

void printUsage(char *name) {
    std::cout << name << " Usage: " << std::endl;
    std::cout << "\t--conf=<config file>: required, configure file" << std::endl;
    std::cout << std::endl;
}

int main(int argc, char *argv[]) {
    struct option longopts[] = {
            { "conf",    required_argument,  NULL,   'c' },
            { "help",    no_argument,        NULL,   'h' },
            { NULL,      0,                  NULL,    0  }
    };
    int ch = 0;
    std::string conf_file;
    while ((ch = getopt_long(argc, argv, "c:h", longopts, NULL)) != -1) {
        switch (ch) {
            case 'c':
                conf_file = optarg;
                break;
            default:
                printUsage(argv[0]);
                return 0;
        }
    }
    if (conf_file.empty()) {
        printUsage(argv[0]);
        return -1;
    }
    if (!bench_config.LoadFromFile(conf_file)) {
        std::cerr << "load configure file failed!" << std::endl;
        ::exit(EXIT_FAILURE);
    }
    bench_config.Print();

    // init logger
    jim::LoggerConfig logger_config;
    logger_config.name = "raft-bench";
    logger_config.path = bench_config.logger_config.path;
    logger_config.level = bench_config.logger_config.level;
    jim::LoggerInit(logger_config);

    auto addr_mgr = std::make_shared<bench::NodeAddress>(3);

    std::vector<std::shared_ptr<bench::Node>> cluster;
    for (size_t i = 1; i <= 3; ++i) {
        auto node = std::make_shared<bench::Node>(i, addr_mgr);
        node->Start();
        cluster.push_back(node);
    }

    BenchContext context;
    context.counter = bench_config.requets_num;
    context.leaders.resize(bench_config.range_num);
    for (uint64_t i = 1; i <= bench_config.range_num; ++i) {
        for (auto &n : cluster) {
            auto r = n->GetRange(i);
            r->WaitLeader();
            if (r->IsLeader()) {
                context.leaders[i-1] = r;
            }
        }
    }

    struct timeval start, end, taken;
    gettimeofday(&start, NULL);

    std::vector<std::thread> threads;
    for (size_t i = 0; i < bench_config.bench_threads; ++i) {
        threads.emplace_back(std::thread(std::bind(&runBenchmark, &context)));
    }
    for (auto &t : threads) {
        t.join();
    }

    // end
    gettimeofday(&end, NULL);
    timersub(&end, &start, &taken);

    std::cout << std::string(50, '-') << std::endl;
    std::cout << bench_config.requets_num << " requests taken " << taken.tv_sec << "s " << taken.tv_usec / 1000 << "ms" << std::endl;
    std::cout << "ops: " << (bench_config.requets_num * 1000) / (taken.tv_sec * 1000 + taken.tv_usec / 1000) << std::endl;
    std::cout << std::string(50, '-') << std::endl;
    return 0;
}
