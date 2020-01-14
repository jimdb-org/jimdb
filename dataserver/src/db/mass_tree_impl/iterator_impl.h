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

_Pragma("once");

#include <string>
#include <vector>

#include "masstree-beta/config.h"
#include "masstree-beta/str.hh"
#include "masstree-beta/masstree_scan.hh"

#include "db/db.h"

namespace jim {
namespace ds {
namespace db {

class MasstreeWrapper;

class MassIterator : public Iterator {
public:
    MassIterator(MasstreeWrapper* tree, const std::string& vbegin, const std::string& vend, size_t max_per_scan = 128);

    template<typename SS, typename K>
    void visit_leaf(const SS &, const K &, threadinfo &) {}

    bool visit_value(Masstree::Str key, std::string* value, threadinfo &);

    bool Valid() override;
    Status status() override;

    void Next() override;

    std::string Key() override;
    uint64_t KeySize() override;

    std::string Value() override;
    uint64_t ValueSize() override ;

private:
    void doScan();

private:
    using KVPair = std::pair<std::string, std::string>;
    using KVPairVector = std::vector<std::pair<std::string, std::string>>;

    size_t batch_size_ = 2;
    const size_t batch_max_size_ = 128;
    const std::string end_key_;

    MasstreeWrapper* tree_ = nullptr;
    bool scannable_ = true;
    std::string next_scan_key_;
    KVPairVector scan_buffer_;
    size_t iter_cursor_ = 0;
};

} // namespace db
} // namespace ds
} // namespace jim
