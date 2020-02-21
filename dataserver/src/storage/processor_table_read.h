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

#include "processor.h"
#include "base/status.h"
#include "proto/gen/dspb/txn.pb.h"
#include "proto/gen/dspb/processorpb.pb.h"
#include "row_fetcher.h"
#include "common/ds_encoding.h"

#include <vector>
#include <string>
#include <map>

namespace jim {
namespace ds {
namespace storage {

class TableRead : public Processor {

public:
    TableRead( const dspb::TableRead & table_read, const dspb::KeyRange & range_default,
            Store & s , bool gather_trace, bool require_version = true);
    ~TableRead();

    TableRead() = delete;
    TableRead(const TableRead & ) = delete;
    TableRead& operator = (const TableRead & ) = delete;

    virtual Status next( RowResult & row) override;

    virtual const std::string get_last_key() override;

    virtual const std::vector<uint64_t> get_col_ids() override;

    virtual void get_stats(std::vector<ProcessorStat> &stats) override;

private:
    std::string str_last_key_;
    bool over_;
    std::chrono::system_clock::time_point begin_time_;

    basepb::KeySchema key_schema_;
    std::unique_ptr<RowFetcher> row_fetcher_;
    std::vector<uint64_t> col_ids_;

};

} /* namespace storage */
} /* namespace ds */
} /* namespace jim */

