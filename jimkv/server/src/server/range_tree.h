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

#include "masstree-beta/config.h"
#include "masstree-beta/masstree.hh"
#include "masstree-beta/timestamp.hh"
#include "masstree-beta/kvthread.hh"

#include "range/range.h"

namespace jimkv {
namespace ds {
namespace server {

using RangePtr = std::shared_ptr<range::Range>;

// RangeTree Use masstree to hold ranges
class RangeTree {
public:
    using Predicate = std::function<Status(const RangePtr&)>;
    using CreateFunc = std::function<Status(RangePtr&)>;

public:
    RangeTree();
    ~RangeTree();

    RangeTree(const RangeTree&) = delete;
    RangeTree& operator=(const RangeTree&) = delete;

    RangePtr Find(uint64_t id) const;

    std::vector<RangePtr> GetAll() const;

    // if not exist, return Status::kNotFound
    // if exist and meet the predicate condition, will be removed; otherwise keep untouched
    Status RemoveIf(uint64_t id, const Predicate& pred, RangePtr& removed);

    // if not exist, call create_func and insert
    // if exist, return Status::kExisted, and previous one will be returned by existed
    Status Insert(uint64_t id, const CreateFunc& create_func, RangePtr& existed);

    uint64_t Size() const { return size_.load(); }
    bool Empty() const { return Size() == 0; }

private:
    class RangeValuePrinter {
    public:
        static void print(std::shared_ptr<range::Range> *value, FILE* f, const char* prefix,
                int indent, Masstree::Str key, kvtimestamp_t, char* suffix) {
            fprintf(f, "%s%*s%.*s = %s%s\n", prefix, indent, "", key.len, key.s, "", suffix);
        }
    };

    struct RangeTreeParams : public Masstree::nodeparams<15, 15> {
        typedef std::shared_ptr<range::Range>* value_type;
        typedef RangeValuePrinter value_print_type;
        typedef ::threadinfo threadinfo_type;
    };

    using TreeType = Masstree::basic_table<RangeTreeParams>;

private:
    TreeType* ranges_ = nullptr;
    std::atomic<uint64_t> size_ = {0};
};

} /* namespace server */
} /* namespace ds */
} /* namespace jimkv */
