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

#include "snapshot.h"

namespace jimkv {
namespace ds {
namespace storage {

Snapshot::Snapshot(uint64_t applied, std::string&& context,
        IterPtr data_iter, IterPtr txn_iter) :
        applied_(applied),
        context_(std::move(context)),
        data_iter_(std::move(data_iter)),
        txn_iter_(std::move(txn_iter)) {
}

Snapshot::~Snapshot() {
    Close();
}

Status Snapshot::Context(std::string* context) {
    *context = context_;
    return Status::OK();
}

Status Snapshot::Next(std::string* data, bool* over) {
    if (!txn_over_) {
        auto s = next(txn_iter_.get(), dspb::CF_TXN, data, &txn_over_);
        if (!s.ok()) {
            return s;
        }
        if (!txn_over_) { // get data in txn cf
            return s;
        }
    }
    // txn_over == true
    return next(data_iter_.get(), dspb::CF_DEFAULT, data, over);
}


Status Snapshot::next(Iter* iter, dspb::CFType cf_type, std::string* data, bool *over) {
    if (iter->Valid()) {
        dspb::SnapshotKVPair p;
        p.set_cf_type(cf_type);
        p.set_key(iter->Key());
        p.set_value(iter->Value());
        if (!p.SerializeToString(data)) {
            return Status(Status::kCorruption, "serialize snapshot data", "pb return false");
        }
        *over = false;
        iter->Next();
    } else {
        *over = true;
    }
    return iter->status();
}

void Snapshot::Close() {
    data_iter_.reset();
    txn_iter_.reset();
}


} /* namespace storage */
} /* namespace ds */
} /* namespace jimkv */
