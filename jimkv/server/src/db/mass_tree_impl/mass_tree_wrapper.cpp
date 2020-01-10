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

#include "mass_tree_wrapper.h"

#include <atomic>
#include <mutex>
#include <sstream>

#include "masstree-beta/masstree_tcursor.hh"
#include "masstree-beta/masstree_stats.hh"
#include "masstree-beta/masstree_insert.hh"
#include "masstree-beta/masstree_remove.hh"
#include "masstree-beta/masstree_print.hh"

#include "base/util.h"
#include "common/logger.h"
#include "common/masstree_env.h"
#include "iterator_impl.h"

namespace jimkv {
namespace ds {
namespace db {

std::string MasseCounter::Collect() const {
    std::ostringstream ss;
    ss << "{ get: " << op_get << ", get_miss: " << op_get_miss;
    ss << ", put: " << op_put << ", replace: " << op_replace;
    ss << ", del: " << op_delete << ", del_miss: " << op_delete_miss;
    ss << ", scan_keys: " << op_scan_keys;
    ss << ", rcu_queue: " << rcu_pendings;
    ss << ", keys_count: " << keys_count << ", keys_bytes: " << keys_bytes;
    ss << ", values_bytes: " << values_bytes;
    ss << " }";
    return ss.str();
}

// clear op counters
void MasseCounter::Clear() {
    op_get.store(0, std::memory_order_relaxed);
    op_get_miss.store(0, std::memory_order_relaxed);
    op_put.store(0, std::memory_order_relaxed);
    op_replace.store(0, std::memory_order_relaxed);
    op_delete.store(0, std::memory_order_relaxed);
    op_delete_miss.store(0, std::memory_order_relaxed);
    op_scan_keys.store(0, std::memory_order_relaxed);
}

class ValueDeferFreeCallback: public mrcu_callback {
public:
    ValueDeferFreeCallback(MasseCounter *counter, std::string* value_ptr_arg) :
            counter_(counter),
            value_ptr_(value_ptr_arg) {
        ++counter_->rcu_pendings;
    }

    virtual ~ValueDeferFreeCallback() = default;

    void operator()(threadinfo& ti) override {
        counter_->values_bytes -= value_ptr_->size();
        delete value_ptr_;
        delete this;
        --counter_->rcu_pendings;
    }

private:
    MasseCounter *counter_ = nullptr;
    std::string* value_ptr_ = nullptr;
};


MasstreeWrapper::MasstreeWrapper() : tree_(new TreeType) {
    tree_->initialize(GetThreadInfo());
}

MasstreeWrapper::~MasstreeWrapper() {
    if (tree_) {
        tree_->destroy(GetThreadInfo());
        delete tree_;
    }
}

Status MasstreeWrapper::Put(const std::string& key, const std::string& value) {
    if (key.size() > MASSTREE_MAXKEYLEN) {
        return Status(Status::kInvalidArgument, "key size too large", std::to_string(key.size()));
    }

    Masstree::Str tree_key(key);

    RCUGuard guard;
    TreeType::cursor_type lp(*tree_, tree_key);
    if (lp.find_insert(GetThreadInfo())) {
        auto defer_free_cb = new ValueDeferFreeCallback(&counter_, lp.value());
        RegisterRCU(defer_free_cb);
        ++counter_.op_replace;
    } else {
        counter_.keys_count += 1;
        counter_.keys_bytes += key.size();
    }
    lp.value() = new std::string(value);
    lp.finish(1, GetThreadInfo());

    counter_.values_bytes += value.size();
    ++counter_.op_put;

    return Status::OK();
}

Status MasstreeWrapper::Get(const std::string& key, std::string& value) {
    Masstree::Str tree_key(key);
    std::string *tree_value = nullptr;

    RCUGuard guard;
    if (tree_->get(tree_key, tree_value, GetThreadInfo())) {
        if (tree_value != nullptr) {
            value.assign(*tree_value);
        }
        ++counter_.op_get;
        return Status::OK();
    } else {
        ++counter_.op_get;
        ++counter_.op_get_miss;
        return Status(Status::kNotFound);
    }
}

Status MasstreeWrapper::Delete(const std::string& key) {
    Masstree::Str tree_key(key);

    RCUGuard guard;
    TreeType::cursor_type lp(*tree_, tree_key);
    if (lp.find_locked(GetThreadInfo())) {
        counter_.keys_count -= 1;
        counter_.keys_bytes -= key.size();
        auto defer_free_cb = new ValueDeferFreeCallback(&counter_, lp.value());
        RegisterRCU(defer_free_cb);
    } else {
        ++counter_.op_delete_miss;
    }
    lp.finish(-1, GetThreadInfo());
    ++counter_.op_delete;

    return Status::OK();
}

template int MasstreeWrapper::Scan(const std::string&, MassIterator&);

template <typename F>
int MasstreeWrapper::Scan(const std::string& begin, F& scanner) {
    RCUGuard guard;
    auto count = tree_->scan(begin, true, scanner, GetThreadInfo());
    counter_.op_scan_keys += count;
    return count;
}

std::unique_ptr<Iterator> MasstreeWrapper::NewIterator(const std::string& start, const std::string& limit, size_t max_per_scan) {
    std::unique_ptr<Iterator> ptr(new MassIterator(this, start, limit, max_per_scan));
    return ptr;
}

std::string MasstreeWrapper::CollectTreeCouter() {
    auto ret = counter_.Collect();
    counter_.Clear();
    return ret;
}

std::string MasstreeWrapper::TreeStat() {
    auto j = Masstree::json_stats(*tree_, GetThreadInfo());
    if (j) {
        return j.unparse(lcdf::Json::indent_depth(1).tab_width(2).newline_terminator(true));
    } else {
        return "";
    }
}

Status MasstreeWrapper::Dump(const std::string& file) {
    FILE *f = ::fopen(file.c_str(), "w");
    if (f == NULL) {
        return Status(Status::kIOError, "open dump file " + file, strErrno(errno));
    }
    tree_->print(f);
    ::fclose(f);
    return Status::OK();
}

} // namespace db
} // namespace ds
} // namespace jimkv
