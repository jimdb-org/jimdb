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
#include "common/ds_encoding.h"
#include "processor.h"
#include "proto/gen/dspb/processorpb.pb.h"
#include "proto/gen/dspb/txn.pb.h"
#include "row_fetcher.h"
#include "aggregate_calc.h"

#include <unordered_map>
#include <string>
#include <vector>

namespace jimkv {
namespace ds {
namespace storage {

class StreamAggregation : public Processor {

public:
    StreamAggregation(const dspb::Aggregation &agg, std::unique_ptr<Processor> processor, bool gather_trace);
    ~StreamAggregation();

    StreamAggregation() = delete;
    StreamAggregation(const StreamAggregation &) = delete;
    StreamAggregation &operator=(const StreamAggregation &) = delete;

    static Status check(const dspb::Aggregation &agg);

    virtual Status next(RowResult &row) override;

    virtual const std::string get_last_key() override {
        return processor_->get_last_key();
    }

    virtual const std::vector<uint64_t> get_col_ids() override {
        return processor_->get_col_ids();
    }

    virtual void get_stats(std::vector<ProcessorStat> &stats) override;
private:
    void FetchRow(const dspb::Aggregation &agg);

private:

    std::unique_ptr<Processor> processor_;
    std::vector<RowColumnInfo> agg_group_by_columns_;
    RowResult row_;
    bool first_row_ = true;
    bool end_row_ = false;
    std::vector< std::unique_ptr<dspb::Expr>> funcs_;
};

} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */
