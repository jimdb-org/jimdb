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

#include "mass_tree_wrapper.h"

namespace jim {
namespace ds {
namespace db {

MassIterator::MassIterator(MasstreeWrapper* tree, const std::string& vbegin,
        const std::string& vend, size_t max_per_scan) :
    batch_max_size_(max_per_scan),
    end_key_(vend),
    tree_(tree),
    next_scan_key_(vbegin) {
    batch_size_ = 2;
    scan_buffer_.reserve(batch_size_);
}

bool MassIterator::visit_value(Masstree::Str key, std::string* value, threadinfo &) {
    auto k = std::string(key.data(), key.length());

    if (!end_key_.empty() && k >= end_key_) {
        return false;
    }

    if (scan_buffer_.size() < batch_size_) {
        scan_buffer_.push_back({});
        scan_buffer_.back().first = std::move(k);
        if (value != nullptr) {
            scan_buffer_.back().second = *value;
        }
        return true;
    } else {
        next_scan_key_ = std::move(k);
        scannable_ = end_key_.empty() ? true : next_scan_key_ < end_key_;
        return false;
    }
}

bool MassIterator::Valid() {
    if (iter_cursor_ < scan_buffer_.size()) {
        return true;
    }

    if (!scannable_) {
        return false;
    } else {
        doScan();
        return !scan_buffer_.empty();
    }
}

Status MassIterator::status() {
    return Status::OK();
}

void MassIterator::Next() {
    ++iter_cursor_;
    assert(iter_cursor_ <= scan_buffer_.size());
}

std::string MassIterator::Key() {
    assert(iter_cursor_ < scan_buffer_.size());
    return scan_buffer_[iter_cursor_].first;
}

uint64_t MassIterator::KeySize() {
    assert(iter_cursor_ < scan_buffer_.size());
    return scan_buffer_[iter_cursor_].first.size();
}

std::string MassIterator::Value() {
    assert(iter_cursor_ < scan_buffer_.size());
    return scan_buffer_[iter_cursor_].second;
}

uint64_t MassIterator::ValueSize() {
    assert(iter_cursor_ < scan_buffer_.size());
    return scan_buffer_[iter_cursor_].second.size();
}

void MassIterator::doScan() {
    assert(scannable_);

    iter_cursor_ = 0;
    scan_buffer_.clear();
    scannable_ = false;
    batch_size_=batch_size_*2;
    batch_size_= (batch_size_>batch_max_size_)?batch_max_size_:batch_size_;
    scan_buffer_.reserve( batch_size_);

    tree_->Scan(next_scan_key_, *this);
}

} // namespace db
} // namespace ds
} // namespace jim
