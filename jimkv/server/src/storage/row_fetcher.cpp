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

#include "row_fetcher.h"

#include <iostream>

#include "base/util.h"

#include "util.h"
#include "common/ds_encoding.h"
#include "common/logger.h"

namespace jimkv {
namespace ds {
namespace storage {

static const size_t kIteratorTooManyKeys = 1000;

RowFetcher::RowFetcher(Store& s, const dspb::SelectRequest& req) :
    store_(s),
    decoder_(Decoder::CreateDecoder(s.GetKeySchema(), req)),
    kv_fetcher_(KvFetcher::Create(s, req)) {
}

RowFetcher::RowFetcher(Store& s, const dspb::TableRead & req,
        const std::string & start_key, const std::string & end_key) :
    store_(s),
    decoder_(Decoder::CreateDecoder(s.GetKeySchema(), req)),
    kv_fetcher_(KvFetcher::Create(s, req, start_key, end_key)) {

}

RowFetcher::RowFetcher(Store& s, const dspb::IndexRead & req,
        const std::string & start_key, const std::string & end_key) :
    store_(s),
    decoder_(Decoder::CreateDecoder(s.GetKeySchema(), req)),
    kv_fetcher_(KvFetcher::Create(s, req, start_key, end_key)) {
}

RowFetcher::RowFetcher(Store& s, const dspb::DataSample & req,
        const std::string & start_key, const std::string & end_key) :
    store_(s),
    decoder_(Decoder::CreateDecoder(s.GetKeySchema(), req)),
    kv_fetcher_(KvFetcher::Create(s, req, start_key, end_key)) {
}

RowFetcher::~RowFetcher() = default;

Status RowFetcher::Next(RowResult& row, bool &over) {
    Status s;
    KvRecord kv;
    if (kv_fetcher_ == nullptr ) {
        over = true;
        return Status( Status::kNoMoreData, "RowFetcher kv_fetcher_ is null.", "");
    }

    bool matched = false;
    while (true) {
        kv.Clear();

        s = kv_fetcher_->Next(kv);
        if (!s.ok() || !kv.Valid()) {
            over = true;
            break;
        }
        addMetric(kv);

        // 非唯一索引没有value
        // if (!kv.HasValue()) continue;

        matched = false;
        row.Reset();
        row.SetKey(std::move(kv.key));
        row.SetValue(std::move(kv.value));
        s = decoder_->DecodeAndFilter( row.GetKey(), row.GetValue(), row, matched);
        if (!s.ok() || matched) {
            over = !s.ok();
            FLOG_DEBUG("RowFetche Next over status info: {} ", s.ToString());
            break;
        }
    }
    return s;
}

Status RowFetcher::Next(const dspb::SelectRequest& req, dspb::Row& row, bool& over) {
    Status s;
    KvRecord kv;

    if (kv_fetcher_ == nullptr ) {
        over = true;
        return Status( Status::kNoMoreData, "RowFetcher kv_fetcher_ is null.", "");
    }
    bool found = false;
    while (true) {
        kv.Clear();

        s = kv_fetcher_->Next(kv);
        if (!s.ok() || !kv.Valid()) {
            over = true;
            break;
        }
        addMetric(kv);

        row.Clear();
        found = false;
        s = getTxnRow(req, std::move(kv), row, found);
        if (!s.ok() || found) {
            over = !s.ok();
            break;
        }
    }
    return s;
}

void RowFetcher::addMetric(const KvRecord& kv) {
    store_.addMetricRead(1, kv.Size());
    ++iter_count_;
    if (iter_count_ % kIteratorTooManyKeys == kIteratorTooManyKeys - 1) {
        FLOG_WARN("range[{}] iterator too many keys({}), filters: {}",
                store_.GetRangeID(), iter_count_, decoder_->DebugString());
    }
}

Status RowFetcher::addIntent(const dspb::SelectRequest& req, dspb::TxnValue& val, dspb::Row& row) {
    const auto& intent = val.intent();
    FLOG_DEBUG("select txn intent: {}", intent.ShortDebugString());

    bool has_intent_row = false;
    if (intent.typ() == dspb::INSERT) {
        RowResult result;
        result.SetKey(intent.key());
        result.SetValue(intent.value());
        bool matched = false;
        auto s = decoder_->DecodeAndFilter( result.GetKey(), result.GetValue(), result, matched);
        if (!s.ok()) {
            return s;
        }
        if (matched) {
            result.SetVersion(val.version());
            result.EncodeTo(req, row.mutable_intent()->mutable_value());
            has_intent_row = true;
        }
    }

    if (row.has_value() || has_intent_row) {
        auto row_intent = row.mutable_intent();
        row_intent->set_txn_id(val.txn_id());
        if (intent.typ() == dspb::INSERT && !has_intent_row) {
            row_intent->set_op_type(dspb::DELETE);
        } else {
            row_intent->set_op_type(intent.typ());
        }
        const auto& primary = intent.is_primary() ? intent.key(): val.primary_key();
        row_intent->set_primary_key(primary);
        row_intent->set_timeout(isExpired(val.expired_at()));
    }
    return Status::OK();
}

Status RowFetcher::getTxnRow(const dspb::SelectRequest& req, KvRecord&& kv, dspb::Row& row, bool& found) {
    assert(kv.Valid());
    Status s;
    if (kv.HasValue()) {
        bool matched = false;
        RowResult result;
        result.SetKey(std::move(kv.key));
        result.SetValue(std::move(kv.value));
        s = decoder_->DecodeAndFilter( result.GetKey(), result.GetValue(), result, matched);
        if (!s.ok()) {
            return s;
        }
        if (matched) {
            result.EncodeTo(req, row.mutable_value());
        }
    }

    if (kv.HasIntent()) {
        dspb::TxnValue txn_value;
        if (!txn_value.ParseFromString(kv.intent)) {
            return Status(Status::kCorruption, "parse txn value", EncodeToHex(kv.intent));
        }
        assert(txn_value.intent().key() == kv.key);
        s = addIntent(req, txn_value, row);
        if (!s.ok()) {
            return s;
        }
    }

    found = row.has_intent() || row.has_value();
    if (found) {
        row.set_key(kv.key);
    }
    return s;
}


} /* namespace storage */
} /* namespace ds */
} /* namespace jimkv */
