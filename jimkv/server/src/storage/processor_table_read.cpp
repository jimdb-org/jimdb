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

#include "processor_table_read.h"
#include <chrono>

namespace jimkv {
namespace ds {
namespace storage {

TableRead::TableRead(const dspb::TableRead & table_read, const dspb::KeyRange & range_default, Store & s, bool gather_trace )
    : str_last_key_(""),
    over_(false),
    range_default_(range_default),
    key_schema_( s.GetKeySchema()),
    row_fetcher_(new RowFetcher( s, table_read, range_default.start_key(), range_default.end_key())){
    gather_trace_ = gather_trace;
    for (const auto & col : table_read.columns()) {
        col_ids_.push_back(col.id());
    }
}

TableRead::~TableRead()
{

}

const std::string TableRead::get_last_key()
{
    return str_last_key_;
}

Status TableRead::next( RowResult & row)
{
    Status s;

    if (over_) {
        return Status(
                Status::kNoMoreData,
                " last key: ",
                EncodeToHexString(get_last_key())
            );
    }
    std::chrono::system_clock::time_point time_begin;
    if (gather_trace_) {
        time_begin = std::chrono::system_clock::now();
    }
    s = row_fetcher_->Next( row, over_);

    if (over_) {
        s = Status( Status::kNoMoreData, " last key: ", EncodeToHexString(get_last_key()) );
    }
    if (s.ok()) {
        str_last_key_ =  row.GetKey();
    }

    if (gather_trace_) {
        ++rows_count_;
        time_processed_ns_ += std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now() - time_begin).count();
    }
    return s;

}

const std::vector<uint64_t> TableRead::get_col_ids()
{
    return col_ids_;
}

void TableRead::get_stats(std::vector<ProcessorStat> &stats) {
    stats.emplace_back(rows_count_, time_processed_ns_);
}

} /* namespace storage */
} /* namespace ds */
} /* namespace jimkv */

