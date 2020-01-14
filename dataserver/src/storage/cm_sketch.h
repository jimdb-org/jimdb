
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
#include "murmur3.h"

namespace jim {
namespace ds {
namespace storage {
class CMSketch {
 public:
    CMSketch(uint32_t d, uint32_t w)
        :depth_(d), width_(w){
        std::vector<uint32_t> row(w, 0);
        std::vector<std::vector<uint32_t>> table(d, row);
        table_ = table;
    }

    CMSketch(const CMSketch& other) {
        depth_ = other.depth_;
        width_ = other.width_;
        count_ = other.count_;
        table_ = other.table_;
    }

    uint32_t GetDepth() { return depth_; }
    uint32_t GetWidth() { return width_; }
    uint32_t GetCount() { return count_; }

    static void Hash(const std::string& data, uint64_t& outLow, uint64_t& outHigh) {
        uint64_t hash[2];
        MurmurHash3_x64_128(data.c_str(), data.length(),0, hash);
        outLow = hash[0];
        outHigh = hash[1];
    }

    uint32_t Query(const std::string& data) {
        uint64_t low,high;
        Hash(data, low, high);
        std::vector<uint32_t> vals(depth_, 0);
        uint32_t minCounter = std::numeric_limits<uint32_t>::max();
       
        for (uint32_t i = 0; i < depth_; ++i) {
            uint32_t j = wrapping_add(low, wrapping_mul(high, (uint64_t)i)) % width_;
            auto noise = (count_ - table_[i][j]) / (width_ - 1);
            vals[i] = table_[i][j] > noise ? table_[i][j] - noise : 0;
            minCounter = std::min(minCounter, table_[i][j]);
            // std::cout << "i:" << i
            //           << " j:" << j
            //           << " noise:" << noise
            //           << " cout:" << count_
            //           << " table_[i][j]:" << table_[i][j]
            //           << " val:" << vals[i]
            //           << " minCounter:" << minCounter
            //           << std::endl;
        }
        
        std::sort(vals.begin(), vals.end(), std::greater<uint32_t>());
        
        return std::min(minCounter,
                        vals[(depth_ - 1) / 2]
                        + (vals[depth_ / 2] - vals[(depth_ - 1) / 2]) / 2);
    }

    void Insert(const std::string& data) {
        count_ = wrapping_add(count_, (uint32_t)1);

        uint64_t low,high;
        Hash(data, low, high);
        uint32_t uint32_max = std::numeric_limits<uint32_t>::max();
        for (uint32_t i = 0; i < depth_; ++i) {
            uint32_t j = wrapping_add(low, wrapping_mul(high, (uint64_t)i)) % width_;
            if (table_[i][j] != uint32_max) {
                ++table_[i][j];
            }
        }
    }

    void ToProto(dspb::CMSketch& sketch) {
        for (uint32_t i = 0; i < table_.size(); ++i) {
            auto row = sketch.add_rows();
            auto tableRow = table_[i];
            for (uint32_t j = 0; j < tableRow.size(); ++j) {
                row->add_counters(tableRow[j]);
            }
        } 
    }

 private:
    template<typename T>
    T wrapping_add(T a, T b) {
        T sum = a;
        T bit = b;
        while(bit) {
            T tmp = sum;
            sum = tmp ^ bit;
            bit = (tmp & bit) << 1;
        }
        return sum;
    }

    template<typename T>
        T wrapping_mul(T a, T b) {
        T multiplier = a < 0 ? wrapping_add(~a, (T)1) : a;
        T multiplicand = b < 0 ? wrapping_add(~b, (T)1) : b;

        T res = 0;
        while(multiplier) {
            if (multiplier & 0x01) {
                res = wrapping_add(res, multiplicand);
            }
            multiplicand = multiplicand << 1;
            multiplier = multiplier >> 1;
        }

        if ((a ^ b) < 0) {
            res = wrapping_add(~res, (T)1);
        }

        return res;
    }
 private:
    uint32_t depth_;
    uint32_t width_;
    uint32_t count_ = 0;
    std::vector<std::vector<uint32_t>> table_;
};

}// end storage
}// end ds
}// end jim
