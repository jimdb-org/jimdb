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
#include <set>
#include "dspb/stats.pb.h"
#include "murmur3.h"

namespace jim {
namespace ds {
namespace storage {
class FMSketch {

 public:
    FMSketch(uint32_t size) : maxSize_(size){}
    FMSketch(const FMSketch& other) {
        mask_ = other.mask_;
        maxSize_= other.maxSize_;
        set_ = other.set_;
    }
    uint32_t GetMaxSize() { return maxSize_; }
    void Insert(const std::string& data){
        uint64_t hash[2];
        MurmurHash3_x64_128(data.c_str(), data.length(),0, hash);
        InsertValue(hash[0]);
    }

    void ToProto(dspb::FMSketch& sketch){
        sketch.set_mask(mask_);
        for (auto& v : set_) {
            sketch.add_set(v);
        }
    }

    uint64_t NumDist() {
        return (mask_ + 1) * set_.size();
    }

    void InsertValue(uint64_t val){
        if ((val & mask_) != 0) {
            return;
        }
        set_.insert(val);
        if (set_.size() > maxSize_) {
            auto mask = (mask_ << 1) | 1;
            std::set<uint64_t>::iterator it;
            for (it = set_.begin(); it != set_.end();) {
                if ((*it & mask) != 0) {
                    it = set_.erase(it);
                } else {
                    ++it;
                }
            }
            mask_ = mask;
        }
    }

    uint32_t GetSetLen() { return set_.size(); }
 private:
    uint64_t mask_ = 0;
    uint32_t maxSize_;
    std::set<uint64_t> set_;
};

}// end storage
}// end ds
}// end jim
