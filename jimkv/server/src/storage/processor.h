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

#include "base/status.h"
#include <vector>
#include <string>

namespace jimkv {
namespace ds {
namespace storage {

class RowResult;

struct ProcessorStat {
    uint64_t count_ = 0;
    uint64_t time_ns_ = 0;
    ProcessorStat(uint64_t count, uint64_t time) {
        count_ = count;
        time_ns_ = time;
    }
};

class Processor {

public:
    Processor() = default;

    virtual ~Processor() = default;

    Processor(const Processor &) = delete;

    Processor &operator=(const Processor &) = delete;

    virtual Status next(RowResult &row) {return Status::OK();};

    virtual const std::string get_last_key() {return "";};

    virtual const std::vector<uint64_t> get_col_ids() {return {};};

    virtual void get_stats(std::vector<ProcessorStat> &stats) {}

protected:
    uint64_t rows_count_ = 0;
    uint64_t time_processed_ns_ = 0;

    bool gather_trace_ = false;
private:
    ;
};

} /* namespace storage */
} /* namespace ds */
} /* namespace jimkv */
