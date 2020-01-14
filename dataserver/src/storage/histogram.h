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
#include <vector>
#include <string>
#include "dspb/stats.pb.h"

namespace jim {
namespace ds {
namespace storage {
class Bucket{
public:
    Bucket(uint64_t count, const std::string& upper_bound,
           const std::string& lower_bound, uint64_t repeats)
        :count_(count),upper_bound_(upper_bound),lower_bound_(lower_bound),repeats_(repeats){}
    
    Bucket(){}

    void CountRepeated(){
        ++count_;
        ++repeats_;
    }

    void Append(const std::string& upper){
        upper_bound_ = upper;
        ++count_;
        repeats_ = 1;
    }

    void ToProto(dspb::Bucket& bucket){
        bucket.set_count(count_);
        bucket.set_lower_bound(lower_bound_);
        bucket.set_upper_bound(upper_bound_);
        bucket.set_repeats(repeats_);
    }

public:
    uint64_t count_;
    std::string upper_bound_;
    std::string lower_bound_;
    uint64_t repeats_;
};

class Histogram {
 public:
    Histogram(uint64_t num)
        :buckets_max_num_(num),per_bucket_limit_(1){}
    Histogram() = delete;

    void ToProto(dspb::Histogram& histogram) {
        histogram.set_num_dist(num_dist_);
        dspb::Bucket bucketTmp;
        for (auto& bucket : buckets_) {
            bucket.ToProto(bucketTmp);
            histogram.add_buckets()->CopyFrom(bucketTmp);
        }
    }

    void Append(const std::string& data);
    bool LastBucketFull();
    void MergeBuckets();

    uint64_t BucketsSize() { return buckets_.size(); }
    uint64_t PerBucketLimit() { return per_bucket_limit_; }
    uint64_t DistNum() { return num_dist_; }

 private:
    uint64_t num_dist_ = 0;
    std::vector<Bucket> buckets_;
    const uint64_t buckets_max_num_;
    uint64_t per_bucket_limit_;
};
}//end storage
}//end ds
}//end jim
