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

#include "base/status.h"
#include "common/ds_encoding.h"
#include "processor.h"
#include "proto/gen/dspb/processorpb.pb.h"
#include "proto/gen/dspb/txn.pb.h"
#include "row_fetcher.h"
#include "aggregate_calc.h"

#include <map>
#include <string>
#include <set>

namespace jim {
namespace ds {
namespace storage {

class OrderBy : public Processor {

public:
    OrderBy(const dspb::Ordering &ordering, std::unique_ptr<Processor> processor, bool gather_trace);
    ~OrderBy();

    OrderBy() = delete;
    OrderBy(const OrderBy &) = delete;
    OrderBy &operator=(const OrderBy &) = delete;

    virtual Status next(RowResult &row) override;

    virtual const std::string get_last_key() override {
        return last_key_;
    }

    virtual const std::vector<uint64_t> get_col_ids() override {
        return processor_->get_col_ids();
    }

    virtual void get_stats(std::vector<ProcessorStat> &stats) override;
private:
    Status OrderingCheck();
    void FetchOrderByRows();
    
private:
    const u_int64_t max_count_ = 1024;
    const dspb::Ordering &ordering_;
    u_int64_t count_;
    Status status_check_ordering_;

    std::unique_ptr<Processor> processor_;
    std::multiset<RowResult> set_result_;
    std::multiset<RowResult>::const_iterator set_result_it_;
    std::vector<RowColumnInfo> col_order_by_infos_;
    std::string last_key_;
};

} /* namespace storage */
} /* namespace ds */
} /* namespace jim */
