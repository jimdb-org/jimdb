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

#include "processor_limit.h"
#include <chrono>

namespace jimkv {
namespace ds {
namespace storage {

Limit::Limit(const dspb::Limit &limit, std::unique_ptr<Processor> processor, bool gather_trace)
    : limit_(limit.count()),
      offset_(limit.offset()),
      end_(offset_ + limit_),
      processor_(std::move(processor)){
    gather_trace_ = gather_trace;
}

Limit::~Limit() {
}

Status Limit::next(RowResult &row) {
    if (index_ >= end_ || limit_ == 0) {
        return Status(
                Status::kNoMoreData, "limit is reached",
                std::to_string(offset_) + "," + std::to_string(index_)  + "," + std::to_string(end_)
                );

    }

    std::chrono::system_clock::time_point time_begin;
    if (gather_trace_) {
        time_begin = std::chrono::system_clock::now();
    }
    Status s;
    while (true) {
        row.Reset();
        s = processor_->next(row);
        if (!s.ok()) {
            row.Reset();
            break;
        }

        if ( ++index_ > offset_ ) {
            break;
        }
    }
    if (gather_trace_) {
        ++rows_count_;
        time_processed_ns_ += std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now() - time_begin).count();
    }
    return s;
}

void Limit::get_stats(std::vector<ProcessorStat> &stats) {
    processor_->get_stats(stats);
    stats.emplace_back(rows_count_, time_processed_ns_);
}

} /* namespace storage */
} /* namespace ds */
} /* namespace jimkv */
