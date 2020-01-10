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

#include "processor_selection.h"
#include "util.h"
#include <chrono>

namespace jimkv {
namespace ds {
namespace storage {

Selection::Selection(const dspb::Selection &selection, std::unique_ptr<Processor> processor, bool gather_trace)
        :processor_(std::move(processor)){
    gather_trace_ = gather_trace;
    for (auto f : selection.filter())
    {
        exprs_.push_back( new dspb::Expr(f));
    }
}

Selection::~Selection()
{

    std::for_each(exprs_.begin(),
            exprs_.end(),
            []( dspb::Expr * ptr ) {
        delete ptr;
    });

    exprs_.clear();
}

Status Selection::next(RowResult &row)
{
    std::chrono::system_clock::time_point time_begin;
    if (gather_trace_) {
        time_begin = std::chrono::system_clock::now();
    }
    Status s;
    bool matched = true;
    while (true) {
        row.Reset();
        s = processor_->next(row);
        if (!s.ok()) {
            row.Reset();
            break;
        }

        for ( auto e : exprs_ ) {

            s = filterExpr(row, *e, matched);
            if (!s.ok()) {
                row.Reset();
                return s;
            }

            if (!matched) {
                break;
            }
        }

        if (matched) {
            break;
        }
    }
    if (gather_trace_) {
        ++rows_count_;
        time_processed_ns_ += std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now() - time_begin).count();
    }
    return s;
}

const std::string Selection::get_last_key()
{
    return processor_->get_last_key();
}

const std::vector<uint64_t> Selection::get_col_ids()
{
    return processor_->get_col_ids();
}

void Selection::get_stats(std::vector<ProcessorStat> &stats) {
    processor_->get_stats(stats);
    stats.emplace_back(rows_count_, time_processed_ns_);
}

} /* namespace storage */
} /* namespace ds */
} /* namespace jimkv */
