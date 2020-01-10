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

#include <gtest/gtest.h>
#include "storage/cm_sketch.h"
#include "zipf/zipf.hpp"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {

using namespace jimkv;
using namespace jimkv::ds::storage;

uint64_t AverageErr(uint32_t d, uint32_t w, uint32_t total, uint32_t maxVal, double s) {
    CMSketch sketch(d, w);
    std::map<uint64_t, uint32_t> map;
    zipf_distribution<uint32_t,double> zipf(maxVal, s);
    std::mt19937 g1(0x01020304);
    
    for (uint32_t i = 0; i < total; ++i) {
        auto val = zipf(g1);
        sketch.Insert(std::to_string(val));
        ++map[val];
    }
    
    total = 0;
    for (auto entry : map) {
        
        auto estimate = sketch.Query(std::to_string(entry.first));
        // std::cout << "first:" << entry.first
        //           << " second:" << entry.second
        //           << " estimate:" << estimate
        //           << " total:" << total
        //           << std::endl;
        if (entry.second > estimate) {
            total += (entry.second - estimate);
        } else {
            total += (estimate - entry.second);
        }
    }
    std::cout << "total:" << total
              << "map.size:" << map.size() << std::endl;
    return total / map.size();
}

TEST(CMSketch, testHash) {
   
    // std::cout.setf(std::ios::hex, std::ios::basefield);
    // std::cout.setf(std::ios::showbase | std::ios::uppercase);
    uint64_t hash[2];
    std::string str = "&";
    CMSketch::Hash(str, hash[0], hash[1]);
    ASSERT_EQ(hash[0], (uint64_t)0X143BE95D0B2DF87B);
    ASSERT_EQ(hash[1], (uint64_t)0X7A14AA872ED2150F);

    str = "abcdefgadfadfadf";
    CMSketch::Hash(str, hash[0], hash[1]);
    ASSERT_EQ(hash[0], (uint64_t)0X9F011B0A598FC6CE);
    ASSERT_EQ(hash[1], (uint64_t)0X6F2759936EFEC9EE);
}

TEST(CMSketch, testCMSketch) {
    uint32_t depth = 8;
    uint32_t width = 2048;
    uint32_t total = 10000;
    uint32_t maxVal = 10000000;
    // auto tmp = AverageErr(depth, width, total, maxVal, 1.2);
    // std::cout << "tmp:" << tmp << std::endl;
    // tmp = AverageErr(depth, width, total, maxVal, 2.0);
    // std::cout << "tmp:" << tmp << std::endl;
    // tmp = AverageErr(depth, width, total, maxVal, 4.0);
    // std::cout << "tmp:" << tmp << std::endl;
    // std::cout.setf(std::ios::dec, std::ios::basefield);
    ASSERT_EQ(AverageErr(depth, width, total, maxVal, 1.2), (uint64_t)1);
    ASSERT_EQ(AverageErr(depth, width, total, maxVal, 2.0), (uint64_t)2);
    ASSERT_EQ(AverageErr(depth, width, total, maxVal, 3.0), (uint64_t)2);
}

}
