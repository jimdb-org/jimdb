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

#include "processor_order_by.h"
#include <chrono>

namespace jimkv {
namespace ds {
namespace storage {

OrderBy::OrderBy(const dspb::Ordering &ordering, std::unique_ptr<Processor> processor, bool gather_trace)
    : ordering_(ordering),
    count_(std::min(ordering_.count(), max_count_)),
    processor_(std::move(processor)){
    gather_trace_ = gather_trace;
    status_check_ordering_ = OrderingCheck();
    if (status_check_ordering_.ok()) {
        for (const auto &column : ordering_.columns()) {
            RowColumnInfo colInfo;
            colInfo.col_id = column.expr().column().id();
            colInfo.asc = column.asc();
            col_order_by_infos_.push_back(std::move(colInfo));
        }
        FetchOrderByRows();
    }
}

OrderBy::~OrderBy() {
}

Status OrderBy::OrderingCheck() {
    if (count_ == 0) {
        return Status(
                Status::kInvalid,
                " orderby count == 0 ",
                ""
            );
    }

    if (ordering_.columns_size() <= 0) {
        return Status(
                Status::kInvalid,
                " orderby columns is empty ",
                ""
            );
    }
    return Status::OK();
}

void OrderBy::FetchOrderByRows() {
    std::chrono::system_clock::time_point time_begin;
    if (gather_trace_) {
        time_begin = std::chrono::system_clock::now();
    }
    Status s;
    do {
        RowResult row;
        s = processor_->next(row);
        if (!s.ok()) {
            break;
        }
        row.SetColumnInfos(col_order_by_infos_);
        set_result_.insert(row);
        if (set_result_.size() == count_) {
            break;
        }
    } while (s.ok());
    
    while(s.ok()) {
        RowResult row;
        s = processor_->next(row);
        if (!s.ok()) {
            break;
        }
        auto it = set_result_.end();
        --it;
        row.SetColumnInfos(col_order_by_infos_);
        if (row < (*it)) {
            set_result_.erase(it);
            set_result_.insert(row);
        }
    }
    set_result_it_ = set_result_.begin();
    if (gather_trace_) {
        time_processed_ns_ += std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now() - time_begin).count();
    }
}

Status OrderBy::next(RowResult &row) {
    if (!status_check_ordering_.ok()) {
        return status_check_ordering_;
    }

    if (set_result_it_ == set_result_.end()) {
        return Status(
                Status::kNoMoreData,
                " last key: ",
                EncodeToHexString(get_last_key())
            );
    }

    row = *set_result_it_++;
    if (gather_trace_) {
        ++rows_count_;
    }
    last_key_ = row.GetKey();
    return Status::OK();
}

void OrderBy::get_stats(std::vector<ProcessorStat> &stats) {
    processor_->get_stats(stats);
    stats.emplace_back(rows_count_, time_processed_ns_);
}

} /* namespace storage */
} /* namespace ds */
} /* namespace jimkv */
