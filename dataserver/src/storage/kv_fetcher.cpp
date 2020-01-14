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

#include "kv_fetcher.h"
#include <random>

namespace jim {
namespace ds {
namespace storage {

std::unique_ptr<KvFetcher> KvFetcher::Create(Store& store, const dspb::SelectRequest& req) {
    if (!req.key().empty()) {
        return std::unique_ptr<KvFetcher>(new PointerKvFetcher(store, req.key(), true));
    } else {
        return std::unique_ptr<KvFetcher>(new TxnRangeKvFetcher(store, req.scope().start(), req.scope().limit()));
    }
}

std::unique_ptr<KvFetcher> KvFetcher::Create(Store& store, const dspb::ScanRequest& req) {
    if (req.only_one()) {
        return std::unique_ptr<KvFetcher>(new PointerKvFetcher(store, req.start_key(), true));
    } else {
        return std::unique_ptr<KvFetcher>(new TxnRangeKvFetcher(store, req.start_key(), req.end_key()));
    }
}

std::unique_ptr<KvFetcher> KvFetcher::Create(
        Store& store,
        const dspb::TableRead & req,
        const std::string & start_key,
        const std::string & end_key
) {

    if (req.type() == dspb::DEFAULT_RANGE_TYPE) {
        return Create(store, start_key, end_key);
    } else if (req.type() == dspb::PRIMARY_KEY_TYPE) {
        std::vector<std::string> vec;
        vec.resize(req.pk_keys_size());
        for (const auto & key : req.pk_keys()) {
            vec.push_back(key);
        }

        return std::unique_ptr<KvFetcher>( new PointersKvFetcher( store, vec, false ) );
    } else if (req.type() == dspb::KEYS_RANGE_TYPE) {

        return std::unique_ptr<KvFetcher>( new TxnRangeKvFetcher( store, req.range().start_key(), req.range().end_key()));
    } else {

        return nullptr;// nerver to here.
    }

    return nullptr;

}

std::unique_ptr<KvFetcher> KvFetcher::Create(
        Store& store,
        const dspb::IndexRead & req,
        const std::string & start_key,
        const std::string & end_key
) {

    if (req.type() == dspb::DEFAULT_RANGE_TYPE) {
        return Create(store, start_key, end_key);
    } else if (req.type() == dspb::PRIMARY_KEY_TYPE) {
        std::vector<std::string> vec;
        vec.resize(req.index_keys_size());
        for (const auto & key : req.index_keys()) {
            vec.push_back(key);
        }

        return std::unique_ptr<KvFetcher>( new PointersKvFetcher( store, vec, false ) );
    } else if (req.type() == dspb::KEYS_RANGE_TYPE) {

        return std::unique_ptr<KvFetcher>( new TxnRangeKvFetcher( store, req.range().start_key(), req.range().end_key()));
    } else {

        return nullptr;// nerver to here.
    }

    return nullptr;
}

std::unique_ptr<KvFetcher> KvFetcher::Create(
        Store& store,
        const dspb::DataSample & req,
        const std::string & start_key,
        const std::string & end_key
) {

    uint64_t kv_count = store.KvCount();
    double ratio = req.ratio();

    if (ratio <= 0 || ratio > 1 ) {
        return nullptr;
    } else {
        uint64_t sample_count = kv_count * req.ratio();
        return std::unique_ptr<KvFetcher>(
                new RangeKvSampleFetcher( store, req.range().start_key(), req.range().end_key(), sample_count)
        );
    }
}

std::unique_ptr<KvFetcher> KvFetcher::Create(Store& store, const std::string & start_key, const std::string & end_key)
{
    return std::unique_ptr<KvFetcher>( new TxnRangeKvFetcher( store, start_key, end_key));
}

PointerKvFetcher::PointerKvFetcher(Store& s, const std::string& key, bool fetch_intent) :
    store_(s), key_(key), fetch_intent_(fetch_intent) {
}

Status PointerKvFetcher::Next(KvRecord& rec) {
    rec.Clear();

    if (fetched_) { // only once
        return Status::OK();
    }

    fetched_ = true;

    auto s = store_.Get(key_, &rec.value, false);
    if (s.ok()) {
        rec.MarkHasValue();
    } else if (s.code() != Status::kNotFound) { // error
        return s;
    }

    if (fetch_intent_) {
        s = store_.GetTxnValue(key_, rec.intent);
        if (s.ok()) {
            rec.MarkHasIntent();
        } else if (s.code() != Status::kNotFound) { // error
            return s;
        }
    }

    if (rec.Valid()) {
        rec.key = key_;
    }

    return Status::OK();
}

PointersKvFetcher::PointersKvFetcher(Store& s, const std::vector<std::string> & keys, bool fetch_intent)
        : store_(s),
          keys_(keys),
          fetch_intent_(fetch_intent)
{

    iter_ = keys_.begin();
}

Status PointersKvFetcher::Next(KvRecord& rec) {
    rec.Clear();

    while (iter_ != keys_.end()) {
        if (store_.keyInScop(*iter_)) {
            auto s = store_.Get(*iter_, &rec.value, false);
            if (s.ok()) {
                rec.MarkHasValue();
            } else if (s.code() == Status::kNotFound) {
                ++iter_;
                continue;
            } else {
                return s;
            }

            if (fetch_intent_) {
                auto s = store_.GetTxnValue(*iter_, rec.intent);
                if (s.ok()) {
                    rec.MarkHasIntent();
                } else if (s.code() == Status::kNotFound) {
                    ;// do notohing
                } else {
                    return s;
                }
            }

            if (rec.Valid()) {
                rec.key = *iter_;
            }

            break;
        } else {
            ++iter_;
        }
    }

    if (iter_ == keys_.end()) {
        return Status( Status::kNoMoreData, "PointersKvFetcher iter_ reached the end", "");
    } else {
        ++iter_;
    }

    return Status::OK();
}

RangeKvFetcher::RangeKvFetcher(Store& s, const std::string& start, const std::string& limit) {
    if (start > limit) {
        status_ = Status(Status::kOutOfBound, "range error. ", "[" + start + "," + limit + ")");
    } else {
        iter_ = s.NewIterator(start, limit);
        if (iter_ == nullptr) {
            status_ = Status(Status::kIOError, "create iterator failed", "null");
        }
    }
}

Status RangeKvFetcher::Next(KvRecord& rec) {
    rec.Clear();

    if (!status_.ok()) {
        return status_;
    }
    // iter is over
    if (!iter_->Valid()) {
        status_ = iter_->status();
        return status_;
    }

    rec.key = iter_->Key();
    rec.value = iter_->Value();
    rec.MarkHasValue();
    iter_->Next();

    return status_;
}

TxnRangeKvFetcher::TxnRangeKvFetcher(Store& s, const std::string& start, const std::string& limit) {
    auto stort_start_key = s.GetStartKey();
    auto store_end_key = s.GetEndKey();
    if (start > limit
        || (!limit.empty() && limit < stort_start_key)
        || (!start.empty() && start > store_end_key)) {
        status_ = Status(Status::kOutOfBound, "range error. ", 
            "store range:[" + stort_start_key + "," + store_end_key + ") input range:[" + start + "," + limit + ")");
    } else {
        status_ = s.NewIterators(data_iter_, txn_iter_, start, limit);
    }
}

bool TxnRangeKvFetcher::valid() {
    if (over_ || !status_.ok()) {
        return false;
    }

    auto s = data_iter_->status();
    if (!s.ok()) {
        status_ = std::move(s);
        return false;
    }
    s = txn_iter_->status();
    if (!s.ok()) {
        status_  = std::move(s);
        return false;
    }
    if (!data_iter_->Valid() && !txn_iter_->Valid()) {
        over_ = true;
        return false;
    }
    return true;
}

Status TxnRangeKvFetcher::Next(KvRecord& rec) {
    rec.Clear();

    if (!valid()) {
        return status_;
    }

    bool has_data = false, has_intent = false;
    std::string data_key, intent_key;
    auto both_valid = data_iter_->Valid() && txn_iter_->Valid();
    if (both_valid) {
        data_key = data_iter_->Key();
        intent_key = txn_iter_->Key();
        if (data_key == intent_key) {
            has_data = true;
            has_intent = true;
        } else if (data_key < intent_key) {
            has_data = true;
        } else {
            has_intent = true;
        }
    } else if (data_iter_->Valid()) {
        data_key = data_iter_->Key();
        has_data = true;
    } else { // only intent valid
        intent_key = txn_iter_->Key();
        has_intent = true;
    }

    // read data iter
    if (has_data) {
        rec.key = std::move(data_key);
        rec.value = data_iter_->Value();
        rec.MarkHasValue();
        data_iter_->Next();
    }

    // read txn iter
    if (has_intent) {
        rec.key = std::move(intent_key);
        rec.intent = txn_iter_->Value();
        rec.MarkHasIntent();
        txn_iter_->Next();
    }

    return Status::OK();
}

RangeKvSampleFetcher::RangeKvSampleFetcher(Store& s, const std::string& start,
        const std::string& limit, const uint64_t num) {

    if (start > limit) {
        status_ = Status(Status::kOutOfBound, "range error. ", "[" + start + "," + limit + ")");
    } else {
        iter_ = s.NewIterator(start, limit);
        if (iter_ == nullptr) {
            status_ = Status(Status::kIOError, "create iterator failed", "null");
        }
    }

    is_sample_over_ = false;
    count_ = 0;
    max_sample_num_ = num;
    if (max_sample_num_ == 0) {
        status_ = Status(Status::kInvalidArgument, "max sample num is 0 ", "");
    }
}

Status RangeKvSampleFetcher::Sample(){

    if (!status_.ok()) {
        return status_;
    }

    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<unsigned long long> rng (0, UINT32_MAX);

    KvRecord kv;
    while (iter_->Valid()) {

        if( pool_sample_.size() <= max_sample_num_-1) {
            kv.key = iter_->Key();
            kv.value = iter_->Value();
            kv.MarkHasValue();
            pool_sample_.push_back(kv);
        } else {
            if (rng(gen, std::uniform_int_distribution<unsigned long long>::param_type(0, count_)) < max_sample_num_ ) {
                auto pos = rng(gen, std::uniform_int_distribution<unsigned long long>::param_type(0, max_sample_num_-1));
                pool_sample_[pos].key = iter_->Key();
                pool_sample_[pos].value = iter_->Value();
            }
        }
        count_++;
        iter_->Next();
    }

    status_ = iter_->status();
    return status_;
}

Status RangeKvSampleFetcher::Next(KvRecord& rec) {
    rec.Clear();

    if (!status_.ok()) {
        return status_;
    }

    if (!is_sample_over_) {

        status_ = Sample();
        if (!status_.ok()) {
            return status_;
        }
        is_sample_over_ = true;
        pool_iter_ = pool_sample_.begin();
    }

    if (pool_iter_ == pool_sample_.end()) {
        status_ = Status( Status::kNoMoreData, "RangeKvSampleFetcher pool_iter_ reached the end", "");
    } else {
        rec.key = pool_iter_->key;
        rec.value = pool_iter_->value;
        rec.MarkHasValue();
        ++pool_iter_;
    }

    return status_;
}


} // namespace storage
} // namespace ds
} // namespace jim
