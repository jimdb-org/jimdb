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

#include "histogram.h"
#include <algorithm>

namespace jimkv {
namespace ds {
namespace storage {
void Histogram::Append(const std::string& data) {
    if (!buckets_.empty() && buckets_.back().upper_bound_ == data) {
        buckets_.back().CountRepeated();
        return;
    }
    ++num_dist_;
    if (buckets_.size() >= buckets_max_num_ && LastBucketFull()) {
        MergeBuckets();
    }

    if (!LastBucketFull()) {
        buckets_.back().Append(data);
        return;
    }

    uint64_t count = 1;
    if (!buckets_.empty()) {
        count += buckets_.back().count_;
    }
    Bucket bucket(count, data, data, 1);
    buckets_.push_back(bucket);
}

bool Histogram::LastBucketFull() {
    if (buckets_.empty()) {
        return true;
    }
    uint64_t count;
    if (buckets_.size() == 1) {
        count = buckets_[0].count_;
    } else {
        auto len = buckets_.size();
        count = buckets_[len - 1].count_ - buckets_[len - 2].count_;
    }
    return count >= per_bucket_limit_;
}

void Histogram::MergeBuckets() {
    auto bucketNum = (buckets_max_num_ + 1) / 2;
    if (bucketNum > 1) {
        std::swap(buckets_[0].upper_bound_, buckets_[1].upper_bound_);
        buckets_[0].count_ = buckets_[1].count_;
        buckets_[0].repeats_ = buckets_[1].repeats_;
    }
    uint64_t index;
    auto max_index = buckets_.size() - 1;
    for (uint64_t i = 1; i < bucketNum; ++i) {
        index = i * 2;
        if (index == max_index) {
            std::swap(buckets_[i], buckets_[max_index]);
            continue;
        }
        std::swap(buckets_[i].lower_bound_, buckets_[index].lower_bound_);
        std::swap(buckets_[i].upper_bound_, buckets_[index + 1].upper_bound_);
        buckets_[i].count_ = buckets_[index + 1].count_;
        buckets_[i].repeats_ = buckets_[index + 1].repeats_;
    }
    buckets_.resize(bucketNum);
    per_bucket_limit_ *= 2;
}
}// end storage
}// end ds
}// end jimkv
