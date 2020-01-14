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

#include "basepb/basepb.pb.h"
#include "basepb/metapb.pb.h"
#include "dspb/expr.pb.h"
#include "table.h"
#include "db/db_manager.h"

namespace jim {
namespace test {
namespace helper {

// peer_id = node_id + 100
uint64_t GetPeerID(uint64_t node_id);

basepb::Range MakeRangeMeta(Table *t, size_t peers_num = 1);
basepb::Range MakeRangeMeta(Table *t, size_t peers_num, uint32_t rid);

// append table_id/index_id
void EncodeKeyPrefix(std::string* buf, uint32_t id);

// append encoded key schema column value to kv_key
void EncodeKvKey(std::string* kv_key, const basepb::ColumnInfo& col, const std::string& val);

// append encoded non-key column value to kv-value
void EncodeKvValue(std::string* kv_value, const basepb::ColumnInfo& col, const std::string& val);

void DecodeValueColumn(const std::string& buf, size_t& offset, const dspb::ColumnInfo& col, std::string *val);

// init log, set to different level by env {LOG_LEVEL}
void InitLog();

// open different db env {DB}
Status NewDBManager(const std::string& path, std::unique_ptr<ds::db::DBManager>& db_manager);

} /* namespace helper */
} /* namespace test */
} /* namespace jim */
