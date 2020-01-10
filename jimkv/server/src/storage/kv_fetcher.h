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

#include "store.h"
#include "db/db.h"
#include <vector>

namespace jimkv {
namespace ds {
namespace storage {

struct KvRecord {
    enum Flags: uint32_t {
        kHasValue = 1U << 0,
        kHasIntent = 1U << 1,
    };

    std::string key;
    std::string value;
    std::string intent;
    uint32_t flag = 0;

    void MarkHasValue() { flag |= Flags::kHasValue; }
    bool HasValue() const { return (flag & Flags::kHasValue) != 0; }

    void MarkHasIntent() { flag |= Flags::kHasIntent; }
    bool HasIntent() const { return (flag & Flags::kHasIntent) != 0; }

    void Clear() { flag = 0; }
    bool Valid() const { return HasValue() || HasIntent(); }

    uint64_t Size() const {
        if (!Valid()) return 0;
        uint64_t size = key.size();
        if (HasValue()) {
            size += value.size();
        }
        if (HasIntent()) {
            size += intent.size();
        }
        return size;
    }
};


class KvFetcher {
public:
    KvFetcher() = default;
    virtual ~KvFetcher() = default;

    virtual Status Next(KvRecord& rec) = 0;

    static std::unique_ptr<KvFetcher> Create(Store& store, const dspb::SelectRequest& req);
    static std::unique_ptr<KvFetcher> Create(Store& store, const dspb::ScanRequest& req);
    static std::unique_ptr<KvFetcher> Create(
            Store& store,
            const dspb::TableRead& req,
            const std::string & start_key,
            const std::string & end_key
    );
    static std::unique_ptr<KvFetcher> Create(
            Store& store,
            const dspb::IndexRead& req,
            const std::string & start_key,
            const std::string & end_key
    );
    static std::unique_ptr<KvFetcher> Create(
            Store& store,
            const dspb::DataSample & req,
            const std::string & start_key,
            const std::string & end_key
    );

    static std::unique_ptr<KvFetcher> Create(Store& sotre, const std::string & start_key, const std::string & end_key);
};


class PointerKvFetcher : public KvFetcher {
public:
    PointerKvFetcher(Store& s, const std::string& key, bool fetch_intent);

    Status Next(KvRecord& rec) override;

private:
    Store& store_;
    const std::string& key_;
    bool fetch_intent_ = false;
    bool fetched_ = false;
};

class PointersKvFetcher: public KvFetcher {
public:
    PointersKvFetcher(Store& s, const std::vector<std::string> & keys, bool fetch_intent);
    Status Next(KvRecord& rec) override;

private:
    Store& store_;
    const std::vector<std::string> keys_;
    std::vector<std::string>::const_iterator iter_;
    bool fetch_intent_;
};

class RangeKvFetcher : public KvFetcher {
public:
    RangeKvFetcher(Store& s, const std::string& start, const std::string& limit);

    Status Next(KvRecord& rec) override;

private:
    db::IteratorPtr iter_ = nullptr;
    Status status_;
};


class TxnRangeKvFetcher : public KvFetcher {
public:
    TxnRangeKvFetcher(Store& s, const std::string& start, const std::string& limit);

    Status Next(KvRecord& rec) override;

private:
    bool valid();

private:
    db::IteratorPtr data_iter_;
    db::IteratorPtr txn_iter_;
    bool over_ = false;
    Status status_;
};

class RangeKvSampleFetcher : public KvFetcher {
public:
    RangeKvSampleFetcher(Store& s, const std::string& start, const std::string& limit, const uint64_t num);

    Status Next(KvRecord& rec) override;
private:
    Status Sample();
private:
    db::IteratorPtr iter_ = nullptr;
    Status status_;
    uint64_t max_sample_num_;
    uint64_t count_;
    std::vector<KvRecord> pool_sample_;
    std::vector<KvRecord>::iterator pool_iter_;
    bool is_sample_over_;

};

} // namespace storage
} // namespace ds
} // namespace jimkv
