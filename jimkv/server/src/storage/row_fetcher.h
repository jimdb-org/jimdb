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

#include "kv_fetcher.h"
#include "row_decoder.h"
#include "store.h"

namespace jimkv {
namespace ds {
namespace storage {

class RowFetcher {
public:
    RowFetcher(Store& s, const dspb::SelectRequest& req);
    RowFetcher(Store& s, const dspb::TableRead& req,
            const std::string & start_key, const std::string & end_key);
    RowFetcher(Store& s, const dspb::IndexRead& req,
            const std::string & start_key, const std::string & end_key);
    RowFetcher(Store& s, const dspb::DataSample& req,
               const std::string & start_key, const std::string & end_key);

    ~RowFetcher();

    RowFetcher(const RowFetcher&) = delete;
    RowFetcher& operator=(const RowFetcher&) = delete;

    Status Next(RowResult& row, bool &over);

    Status Next(const dspb::SelectRequest& req, dspb::Row& row, bool& over);

private:
    void addMetric(const KvRecord& kv);

    // decode row from kv
    Status getTxnRow(const dspb::SelectRequest& req, KvRecord&& kv, dspb::Row& row, bool& found);
    Status addIntent(const dspb::SelectRequest& req, dspb::TxnValue& val, dspb::Row& row);

private:
    Store& store_;

    std::unique_ptr<Decoder> decoder_;
    std::unique_ptr<KvFetcher> kv_fetcher_;
    size_t iter_count_ = 0;
};

} /* namespace storage */
} /* namespace ds */
} /* namespace jimkv */
