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
#include <random>
#include "base/status.h"
#include "dspb/stats.pb.h"
#include "basepb/basepb.pb.h"
#include "processor.h"
#include "store.h"
#include "cm_sketch.h"
#include "fm_sketch.h"

namespace jimkv {
namespace ds {
namespace storage {
class SampleCollector {
public:
    SampleCollector(uint64_t sampleMaxSize, uint32_t fmSketchMax, uint32_t d, uint32_t w) :sampleMaxSize_(sampleMaxSize) {
        fmSketch_ = std::unique_ptr<FMSketch>(new FMSketch(fmSketchMax));
        cmSketch_ = std::unique_ptr<CMSketch>(new CMSketch(d, w));
    }

    SampleCollector(const SampleCollector& other) {
        sampleMaxSize_ = other.sampleMaxSize_;
        fmSketch_ = std::unique_ptr<FMSketch>(new FMSketch(other.fmSketch_->GetMaxSize()));
        cmSketch_ = std::unique_ptr<CMSketch>(new CMSketch(other.cmSketch_->GetDepth(), other.cmSketch_->GetWidth()));
    }

    void ToProto(dspb::SampleCollector& sample) {
        sample.set_null_count(nullCount_);
        sample.set_count(count_);        
        fmSketch_->ToProto(*sample.mutable_fm_sketch());
        cmSketch_->ToProto(*sample.mutable_cm_sketch());
        sample.set_total_size(totalSize_);
        for (auto& v : samples_) {
            sample.add_samples(v);
        }
    }

    void Collect(const char* data) {
        if (data == nullptr) {
            ++nullCount_;
            return;
        }
        ++count_;
        fmSketch_->Insert(data);
        cmSketch_->Insert(data);
        totalSize_ += std::string(data).length();
        if (samples_.size() < sampleMaxSize_) {
            samples_.push_back(data);
            return;
        }

        std::uniform_int_distribution<uint64_t> dist(0, count_-1);
        if (dist(rd_) < sampleMaxSize_) {
            std::uniform_int_distribution<uint64_t> dist1(0, sampleMaxSize_-1);
            samples_[dist1(rd_)] = data;
        }
    }
private:
    std::vector<std::string> samples_;
    uint64_t nullCount_ = 0;
    uint64_t count_ = 0;
    uint64_t sampleMaxSize_;
    std::unique_ptr<FMSketch> fmSketch_;
    std::unique_ptr<CMSketch> cmSketch_;
    uint64_t totalSize_ = 0;
    std::random_device rd_;
};

class StatsCol{
public:
    StatsCol(const dspb::ColumnsStatsRequest& req, Store& s);
    StatsCol() = delete;
    ~StatsCol(){}

    Status StatsColResp(dspb::ColumnsStatsResponse& resp);

private:
    uint32_t colLen_;
    uint32_t bucketMax_;
    uint32_t sampleMax_;
    uint32_t fmSketchMax_;
    uint32_t cmSketchDepth_;
    uint32_t cmSketchWidth_;
    basepb::KeySchema keySchema_;
    std::unique_ptr<Processor> processor_;
    uint32_t pk_id_ = std::numeric_limits<uint32_t>::max();
    bool has_pk_ = false;
    
    std::vector<uint64_t> cols_;
};
}// end storage
}// end ds
}// end jimkv
