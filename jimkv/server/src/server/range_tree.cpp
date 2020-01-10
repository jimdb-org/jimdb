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

#include "range_tree.h"

#include "masstree-beta/masstree_tcursor.hh"
#include "masstree-beta/masstree_get.hh"
#include "masstree-beta/masstree_insert.hh"
#include "masstree-beta/masstree_remove.hh"
#include "masstree-beta/masstree_scan.hh"

#include "common/masstree_env.h"

namespace jimkv {
namespace ds {
namespace server {

RangeTree::RangeTree() :
    ranges_(new TreeType) {
    ranges_->initialize(GetThreadInfo());
}

RangeTree::~RangeTree() {
    if (ranges_) {
        ranges_->destroy(GetThreadInfo());
        delete ranges_;
    }
}

// NOTE: key is a ref of id
static Masstree::Str makeKey(uint64_t& id) {
    Masstree::Str key((const char *)&id, sizeof(id));
    return key;
}

RangePtr RangeTree::Find(uint64_t id) const {
    auto key = makeKey(id);
    std::shared_ptr<range::Range> *tree_val = nullptr;

    RCUGuard guard;
    if (ranges_->get(key, tree_val, GetThreadInfo())) {
        return *tree_val;
    } else {
        return nullptr;
    }
}

class RangeScanner {
public:
    template<typename SS, typename K>
    void visit_leaf(const SS &, const K &, threadinfo &) {}

    bool visit_value(Masstree::Str key, RangePtr* value, threadinfo &) {
        result_.push_back(*value);
        return true;
    }

    std::vector<RangePtr> GetResult() {
        return std::move(result_);
    }

private:
    std::vector<RangePtr> result_;
};

std::vector<RangePtr> RangeTree::GetAll() const {
    Masstree::Str start;
    RangeScanner scanner;
    {
        RCUGuard guard;
        ranges_->scan(start, true, scanner, GetThreadInfo());
    }
    return scanner.GetResult();
}

class RangeDeferFreeCallback: public mrcu_callback {
public:
    explicit RangeDeferFreeCallback(RangePtr* ptr) : ptr_(ptr) {}
    virtual ~RangeDeferFreeCallback() = default;
    void operator()(threadinfo& ti) override { delete ptr_; delete this; }
private:
    RangePtr *ptr_ = nullptr;
};

Status RangeTree::RemoveIf(uint64_t id, const Predicate& pred, RangePtr& removed) {
    auto key = makeKey(id);
    Status ret;
    int decision = 0;

    RCUGuard guard;
    TreeType::cursor_type lp(*ranges_, key);
    if (lp.find_locked(GetThreadInfo())) {
        assert(lp.value() != nullptr);
        ret = pred(*lp.value());
        if (ret.ok()) {
            removed = *lp.value();
            auto defer_free_cb = new RangeDeferFreeCallback(lp.value());
            RegisterRCU(defer_free_cb);
            decision = -1;
            --size_;
        } else {
            decision = 0; // do not delete
        }
    } else {
        decision = -1;
        ret = Status(Status::kNotFound);
    }
    lp.finish(decision, GetThreadInfo());

    return ret;
}

Status RangeTree::Insert(uint64_t id, const CreateFunc& create_func, RangePtr& existed) {
    auto key = makeKey(id);
    Status ret;
    int decision = 0;

    RCUGuard guard;
    TreeType::cursor_type lp(*ranges_, key);
    if (lp.find_insert(GetThreadInfo())) {
        existed = *lp.value();
        ret = Status(Status::kExisted);
    } else {
        RangePtr new_rng;
        ret = create_func(new_rng);
        if (ret.ok()) {
            lp.value() = new RangePtr(new_rng);
            ++size_;
            decision = 1; // insert
        }
    }
    lp.finish(decision, GetThreadInfo());

    return ret;
}

}  // namespace server
}  // namespace ds
}  // namespace jimkv

