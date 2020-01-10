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

#include "processor_stream_agg.h"

namespace jimkv {
namespace ds {
namespace storage {

StreamAggregation::StreamAggregation(const dspb::Aggregation &agg, std::unique_ptr<Processor> processor, bool gather_trace)
    : processor_(std::move(processor)){
    gather_trace_ = gather_trace;
    for (auto &g : agg.group_by()) {
        RowColumnInfo colInfo;
        colInfo.col_id = g.column().id();
        colInfo.asc = true;
        agg_group_by_columns_.push_back(std::move(colInfo));
    }

    for (auto &f : agg.func()) {
        funcs_.push_back(std::unique_ptr<dspb::Expr>(new dspb::Expr(f)));
    }
}

StreamAggregation::~StreamAggregation() {
}

Status StreamAggregation::check(const dspb::Aggregation &agg) {
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

Status StreamAggregation::next(RowResult &row) {
    if (end_row_) {
        return Status(
            Status::kNoMoreData, "group by is reached",
            "end"
            );
    }
    std::chrono::system_clock::time_point time_begin;
    if (gather_trace_) {
        time_begin = std::chrono::system_clock::now();
    }
    std::vector<std::unique_ptr<AggreCalculator>> cals;
    for (auto &f : funcs_) {
        auto cal = AggreCalculator::New(*f);
        if (cal) {
            cals.push_back(std::move(cal));
        }
    }
    if (!first_row_) {
        for (auto &cal : cals) {
            cal->Add(row_.GetField(cal->GetColumnId()));
        }
    }
    bool neet_group = false;
    Status s;
    do {
        s = processor_->next(row);
        if (!s.ok()) {
            if (first_row_) {
                return Status(
                    Status::kNoMoreData, "group by is reached",
                    "end"
                    );
            } else {
                end_row_ = true;
            }
            break;
        }

        if (!agg_group_by_columns_.empty()) {
            row.SetColumnInfos(agg_group_by_columns_);
        }

        if (first_row_) {
            row_ = row;
            first_row_ = false;
        } else if (!(row_ == row)) {
            neet_group = true;
            break;
        }

        for (auto &cal : cals) {
            cal->Add(row.GetField(cal->GetColumnId()));
        }
        
    } while (s.ok());

    std::string buf;
    for (auto& cal : cals) {
        auto f = cal->Result();
        EncodeFieldValue(&buf, f.get());
        if (cal->isAvg()) {
            EncodeIntValue(&buf,kNoColumnID, cal->Count());
        }
    }

    if (neet_group || end_row_) {
        for (auto &column : agg_group_by_columns_) {
            EncodeFieldValue(&buf, row_.GetField(column.col_id));
        }
    
        row_.SetAggFields(std::move(buf));
        std::swap(row_, row);
    } else {
        for (auto &column : agg_group_by_columns_) {
            EncodeFieldValue(&buf, row.GetField(column.col_id));
        }
        
        row.SetAggFields(std::move(buf));
    }
    if (gather_trace_) {
        ++rows_count_;
        time_processed_ns_ += std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now() - time_begin).count();
    }
    return Status::OK();
}

void StreamAggregation::get_stats(std::vector<ProcessorStat> &stats) {
    processor_->get_stats(stats);
    stats.emplace_back(rows_count_, time_processed_ns_);
}

} /* namespace storage */
} /* namespace ds */
} /* namespace jimkv */
