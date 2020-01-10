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

#include <atomic>

#include "masstree-beta/config.h"
#include "masstree-beta/kvthread.hh"
#include "masstree-beta/masstree.hh"
#include "masstree-beta/timestamp.hh"

#include "base/util.h"
#include "base/status.h"
#include "iterator_impl.h"

namespace jimkv {
namespace ds {
namespace db {

struct MasseCounter {
    std::atomic<uint64_t> op_get = {0};
    std::atomic<uint64_t> op_get_miss = {0};
    std::atomic<uint64_t> op_put = {0};
    std::atomic<uint64_t> op_replace = {0};
    std::atomic<uint64_t> op_delete = {0};
    std::atomic<uint64_t> op_delete_miss = {0};
    std::atomic<uint64_t> op_scan_keys = {0};

    std::atomic<uint64_t> rcu_pendings = {0};
    std::atomic<uint64_t> keys_count = {0};
    std::atomic<uint64_t> keys_bytes = {0};
    std::atomic<uint64_t> values_bytes = {0};

    std::string Collect() const;
    void Clear();
};

class MasstreeWrapper {
public:
    MasstreeWrapper();
    ~MasstreeWrapper();

    Status Put(const std::string& key, const std::string& value);
    Status Get(const std::string& key, std::string& value);
    Status Delete(const std::string& key);

    template <typename F>
    int Scan(const std::string& begin, F& scanner);

    std::unique_ptr<Iterator> NewIterator(const std::string& start, const std::string& limit, size_t max_per_scan = 128);

    std::string CollectTreeCouter();
    std::string TreeStat();
    Status Dump(const std::string& file);

private:
    class StringValuePrinter {
    public:
        static void print(std::string *value, FILE* f, const char* prefix,
                          int indent, Masstree::Str key, kvtimestamp_t,
                          char* suffix) {
            fprintf(f, "%s%*s %s = v@%lu\n", 
                    prefix, indent, "  ", EncodeToHex(std::string(key.s, key.len)).c_str(), 
                    (value ? value->size() : 0) + (suffix == NULL ? 0 : strlen(suffix)));
        }
    };

    struct default_query_table_params : public Masstree::nodeparams<15, 15> {
        typedef std::string* value_type;
        typedef StringValuePrinter value_print_type;
        typedef ::threadinfo threadinfo_type;
    };

    using TreeType = Masstree::basic_table<default_query_table_params>;

private:
    TreeType* tree_ = nullptr;
    MasseCounter counter_;
};

} // namespace db
} // namespace ds
} // namespace jimkv
