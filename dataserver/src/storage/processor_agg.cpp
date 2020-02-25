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

#include "processor_agg.h"
#include <chrono>

namespace jim {
namespace ds {
namespace storage {

Aggregation::Aggregation(const dspb::Aggregation &agg, std::unique_ptr<Processor> processor, bool gather_trace)
    : processor_(std::move(processor)) ,
    agg_(agg),
    has_fetch_row_(false){
    gather_trace_ = gather_trace;
    agg_group_by_columns_.reserve(agg.group_by_size());
    for (auto &g : agg.group_by()) {
        RowColumnInfo colInfo;
        colInfo.col_id = g.column().id();
        colInfo.asc = true;
        agg_group_by_columns_.push_back(std::move(colInfo));
    }

    // no group by
    if (agg_group_by_columns_.empty()) {
        for (auto &f : agg.func()) {
            plain_calculators_.push_back(AggreCalculator::New(f));
        }
    }
}

Aggregation::~Aggregation() {
}

Status Aggregation::check(const dspb::Aggregation &agg) {
    std::set<u_int64_t> group_col_ids;
    for (auto &g : agg.group_by()) {
        group_col_ids.insert(g.column().id());
        // std::cout << "group_by column id: " << g.column().id() << std::endl;
    }

    for (auto &f : agg.func()) {
        // std::cout << "f.child(0).column().id() :" << f.child(0).column().id() << std::endl;
        // std::cout << "f.child(0).expr_type() :" << f.child(0).expr_type() << std::endl;
        if (f.child(0).expr_type() == dspb::Column 
            && group_col_ids.find(f.child(0).column().id()) == group_col_ids.end()) {
            return Status( Status::kNotSupported,
                        "func column id is not in group by ids",
                        "column id:" + std::to_string(f.column().id()));
        }
    }
    return Status::OK();
}

Status Aggregation::FetchRow(const dspb::Aggregation &agg) {

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

        if (!agg_group_by_columns_.empty()) {
            row.SetColumnInfos(agg_group_by_columns_);
            if (unordered_map_res[row].empty()) {
                for (auto &f : agg.func()) {
                    auto cal = AggreCalculator::New(f);
                    if (cal) {
                        unordered_map_res[row].push_back(std::move(cal));
                    }
                }
            }
            for (auto &cal : unordered_map_res[row]) {
                cal->Add(row.GetField(cal->GetColumnId()));
            }
        } else {
            for (auto &cal : plain_calculators_) {
                cal->Add(row.GetField(cal->GetColumnId()));
            }
        }
    } while (s.ok());

    if (!plain_calculators_.empty()) {
        unordered_map_res[RowResult()] = std::move(plain_calculators_);
    }

    if (gather_trace_) {
        time_processed_ns_ += std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now() - time_begin).count();
    }
    return s;
}

Status Aggregation::next(RowResult &row) {

    Status s;

    if (!has_fetch_row_) {

        s = FetchRow(agg_);
        if (s.code() == Status::kNoMoreData) {
            s = Status::OK();
        } else if (!s.ok()) {
            return s;
        }

        has_fetch_row_ = true;
        unordered_map_it_ = unordered_map_res.begin();
    }

    if (unordered_map_it_ == unordered_map_res.end()) {
        return Status( Status::kNoMoreData, "group by is reached", "" );
    }

    row = unordered_map_it_->first;
    std::string buf;
    for (auto& cal : unordered_map_it_->second) {
        auto f = cal->Result();
        EncodeFieldValue(&buf, f.get());
        if (cal->isAvg()) {
            EncodeIntValue(&buf, kNoColumnID, cal->Count());
        }
    }

    for (auto &column : agg_group_by_columns_) {
        EncodeFieldValue(&buf, row.GetField(column.col_id));
    }

    row.SetAggFields(std::move(buf));
    if (gather_trace_) {
        ++rows_count_;
    }

    ++unordered_map_it_;
    return Status::OK();
}

void Aggregation::get_stats(std::vector<ProcessorStat> &stats) {
    processor_->get_stats(stats);
    stats.emplace_back(rows_count_, time_processed_ns_);
}

} /* namespace storage */
} /* namespace ds */
} /* namespace jim */
