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

#include <map>
#include <string>
#include <vector>

#include <rocksdb/db.h>

#include "base/status.h"
#include "basepb/basepb.pb.h"

namespace jimkv {
namespace ds {
namespace storage {

static const std::string kRangeMetaPrefix = "\x02";
static const std::string kRangeApplyPrefix = "\x03";
static const std::string kNodeIDKey = "\x04NodeID";
static const std::string kRangeVersionPrefix = "\x05";
static const std::string kRangeStatsPrefix = "\x06";

class MetaStore {
public:
    explicit MetaStore(const std::string& path);
    ~MetaStore();

    Status Open(bool read_only = false);

    MetaStore(const MetaStore&) = delete;
    MetaStore& operator=(const MetaStore&) = delete;
    MetaStore& operator=(const MetaStore&) volatile = delete;

    Status SaveNodeID(uint64_t node_id);
    Status GetNodeID(uint64_t* node_id);

    Status SaveVersionID(const uint64_t &range_id, int64_t ver_id);
    Status GetVersionID(const uint64_t &range_id, int64_t* ver_id);

    Status GetAllRange(std::vector<basepb::Range>* range_metas);
    Status GetRange(uint64_t range_id, basepb::Range* meta);
    Status AddRange(const basepb::Range& meta);
    Status BatchAddRange(const std::vector<basepb::Range>& range_metas);
    Status DelRange(uint64_t range_id);

    Status SaveApplyIndex(uint64_t range_id, uint64_t apply_index);
    Status LoadApplyIndex(uint64_t range_id, uint64_t* apply_index);
    Status DeleteApplyIndex(uint64_t range_id);

    Status SaveRangeStats(const uint64_t& range_id, const std::string& str_stats);
    Status LoadRangeStats(const uint64_t& range_id, std::string& str_stats);
    Status DelRangeStats(const uint64_t& range_id);

private:
    const std::string path_;
    rocksdb::WriteOptions write_options_;
    rocksdb::DB* db_ = nullptr;
};

}  // namespace storage
}  // namespace ds
}  // namespace jimkv
