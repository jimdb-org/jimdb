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

#include "iterator_impl.h"

namespace jim {
namespace ds {
namespace db {

Status RocksIterator::Create(rocksdb::DB *db,
                     const std::vector<rocksdb::ColumnFamilyHandle*>& cf,
                     const std::string& start,
                     const std::string& limit,
                     const rocksdb::ReadOptions& options,
                     std::vector<IteratorPtr>& iterators) {
    // make iterator ReadOptions
    rocksdb::ReadOptions opt = options;
    auto upper_bound = std::make_shared<rocksdb::PinnableSlice>();
    upper_bound->PinSelf(limit);
    opt.iterate_upper_bound = upper_bound.get();

    if (cf.empty()) {
        return Status(Status::kInvalidArgument, "cf size", "0");
    } else if (cf.size() == 1) {
        auto db_iter = db->NewIterator(opt, cf[0]);
        if (db_iter == nullptr) {
            return Status(Status::kIOError, "create iterator", "nullptr");
        }
        IteratorPtr iter(new RocksIterator(db_iter, start, upper_bound));
        iterators.push_back(std::move(iter));
    } else {
        std::vector<rocksdb::Iterator*> db_iters;
        auto s = db->NewIterators(opt, cf, &db_iters);
        if (!s.ok()) {
            return Status(Status::kIOError, "create iterators", s.ToString());
        }
        for (auto db_iter : db_iters) {
            IteratorPtr iter(new RocksIterator(db_iter, start, upper_bound));
            iterators.push_back(std::move(iter));
        }
    }
    assert(iterators.size() == cf.size());
    return Status::OK();
}

RocksIterator::RocksIterator(rocksdb::Iterator* it, const std::string& start,
        const std::shared_ptr<rocksdb::Slice>& limit) :
    rit_(it),
    upper_bound_(limit) {
    assert(!start.empty());
    assert(limit);
    rit_->Seek(start);
}

RocksIterator::~RocksIterator() {
    delete rit_;
}

bool RocksIterator::Valid() {
    assert( !rit_->Valid() || (rit_->key().compare(*upper_bound_) < 0));
    return rit_->Valid();
}

void RocksIterator::Next() {
    rit_->Next();
}

Status RocksIterator::status() {
    if (!rit_->status().ok()) {
        return Status(Status::kIOError, rit_->status().ToString(), "");
    }
    return Status::OK();
}

std::string RocksIterator::Key() {
    return rit_->key().ToString();
}

std::string RocksIterator::Value() {
    return rit_->value().ToString();
}

uint64_t RocksIterator::KeySize() {
    return rit_->key().size();
}

uint64_t RocksIterator::ValueSize() {
    return rit_->value().size();
}

} // namespace db
} // namespace ds
} // namespace jim
