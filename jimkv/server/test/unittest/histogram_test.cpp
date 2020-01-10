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
#include "storage/histogram.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {

using namespace jimkv;
using namespace jimkv::ds::storage;
TEST(Histogram, histogram) {
    uint64_t bucketMax = 3;
    Histogram hist(bucketMax);
    ASSERT_EQ(hist.BucketsSize(), (uint64_t)0);

    for (int n = 0; n < 3; ++n) {
        hist.Append(std::to_string(n));
    }
    // b0: [0]
    // b1: [1]
    // b2: [2]
    ASSERT_EQ(hist.BucketsSize(), (uint64_t)3);    
    ASSERT_EQ(hist.PerBucketLimit(), (uint64_t)1);
    ASSERT_EQ(hist.DistNum(), (uint64_t)3);
    dspb::Histogram dspbHistogram;
    hist.ToProto(dspbHistogram);
    std::cout << "dspbHistogram:" << dspbHistogram.DebugString() << std::endl;

    hist.Append(std::to_string(3));
    // b0: [0, 1]
    // b1: [2, 3]
    ASSERT_EQ(hist.BucketsSize(), (uint64_t)2);    
    ASSERT_EQ(hist.PerBucketLimit(), (uint64_t)2);
    ASSERT_EQ(hist.DistNum(), (uint64_t)4);
    dspbHistogram.Clear();
    hist.ToProto(dspbHistogram);
    std::cout << "dspbHistogram:" << dspbHistogram.DebugString() << std::endl;

    for (int n = 0; n < 3; ++n) {
        hist.Append(std::to_string(3));
    }
    // b1: [0, 1]
    // b2: [2, 3, 3, 3, 3]
    ASSERT_EQ(hist.BucketsSize(), (uint64_t)2);    
    ASSERT_EQ(hist.PerBucketLimit(), (uint64_t)2);
    ASSERT_EQ(hist.DistNum(), (uint64_t)4);
    dspbHistogram.Clear();
    hist.ToProto(dspbHistogram);
    std::cout << "dspbHistogram:" << dspbHistogram.DebugString() << std::endl;

    for (int n = 0; n < 5; ++n) {
        hist.Append(std::to_string(4));
    }
    // b0: [0, 1]
    // b1: [2, 3, 3, 3, 3]
    // b2: [4, 4, 4, 4, 4]
    ASSERT_EQ(hist.BucketsSize(), (uint64_t)3);    
    ASSERT_EQ(hist.PerBucketLimit(), (uint64_t)2);
    ASSERT_EQ(hist.DistNum(), (uint64_t)5);
    dspbHistogram.Clear();
    hist.ToProto(dspbHistogram);
    std::cout << "dspbHistogram:" << dspbHistogram.DebugString() << std::endl;

    hist.Append(std::to_string(5));
    // b0: [0, 1, 2, 3, 3, 3, 3]
    // b1: [4, 4, 4, 4, 4]
    // b2: [5]
    ASSERT_EQ(hist.BucketsSize(), (uint64_t)3);    
    ASSERT_EQ(hist.PerBucketLimit(), (uint64_t)4);
    ASSERT_EQ(hist.DistNum(), (uint64_t)6);
    dspbHistogram.Clear();
    hist.ToProto(dspbHistogram);
    std::cout << "dspbHistogram:" << dspbHistogram.DebugString() << std::endl;
}
}
