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

#include <gtest/gtest.h>
#include "storage/fm_sketch.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {

using namespace jim;
using namespace jim::ds::storage;

std::vector<uint32_t> GenSamples(int32_t count) {
    uint32_t len = count + 1001;
    std::vector<uint32_t> vt(len, 2);
    vt[0] = 0;
    uint32_t start = 1000;
    for (uint32_t i = start; i < len; ++i) {
        vt[i] = i;
    }

    uint32_t id = start;
    while (id < len) {
        vt[id] += 1;
        id += 3;
    }
    id = start;
    while (id < len) {
        vt[id] += 2;
        id += 5;
    }
    return vt;
}
    
FMSketch BuildFmSketch(std::vector<uint32_t>& vtData, uint32_t maxSize) {
    FMSketch sketch(maxSize);
    for (auto data : vtData) {
        sketch.Insert(std::to_string(data));
    }
    return sketch;
}

TEST(CMSketch, testCMSketch) {
    uint32_t maxSize = 1000;
    auto samples = GenSamples(10000);
    auto sketch = BuildFmSketch(samples, maxSize);
    ASSERT_EQ(sketch.NumDist(), (uint32_t)6976);

    uint32_t count = 100000;
    auto rc = GenSamples(count);
    sketch = BuildFmSketch(rc, maxSize);
    ASSERT_EQ(sketch.NumDist(), (uint32_t)69888);

    std::vector<uint32_t> pk(count, 0);
    for (uint32_t n = 0; n < count; ++n) {
        pk[n] = n;
    }
    sketch = BuildFmSketch(pk, maxSize);
    ASSERT_EQ(sketch.NumDist(), (uint32_t)95616);

    maxSize = 2;
    sketch = FMSketch(maxSize);
    sketch.InsertValue(1);
    sketch.InsertValue(2);
    ASSERT_EQ(sketch.GetSetLen(), maxSize);

    sketch.InsertValue(4);
    ASSERT_EQ(sketch.GetSetLen(), maxSize);
}

}
