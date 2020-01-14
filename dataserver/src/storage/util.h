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

#include "base/status.h"
#include "basepb/basepb.pb.h"
#include "basepb/metapb.pb.h"
#include "dspb/txn.pb.h"

#include "field_value.h"
#include "row_decoder.h"
#include "internal_tag.pb.h"
#include "common/ds_encoding.h"

namespace jim {
namespace ds {
namespace storage {

uint64_t calExpireAt(uint64_t ttl);
bool isExpired(uint64_t expired_at);

Status decodeKeyColumn(const std::string& key, size_t& offset,
        const dspb::ColumnInfo& col, std::unique_ptr<FieldValue>* field,
        bool& hav_null);

Status decodeKeyColumn(const std::string& key, size_t& offset,
       const basepb::KeySchema_ColumnInfo& col, std::unique_ptr<FieldValue>* field,
       bool& hav_null);

Status decodeValueColumn(const std::string& buf, size_t& offset,
        const dspb::ColumnInfo& col, std::unique_ptr<FieldValue>& field);

Status decodeValueColumn(const std::string& buf, size_t& offset,
        const dspb::ColumnInfo& col, std::unique_ptr<FieldValue>& field, const jim::EncodeType & type);

Status decodeDefaultValueColumn(const dspb::ColumnInfo& col, std::unique_ptr<FieldValue>* field);

void fillColumnInfo(const basepb::ColumnInfo& col, dspb::ColumnInfo* info);
void makeColumnExpr(const basepb::ColumnInfo& col, dspb::Expr* expr);
void makeConstValExpr(const basepb::ColumnInfo& col, const std::string& value, dspb::Expr* expr);

Status filterExpr(const RowResult& row, const dspb::Expr& expr, bool& matched);

Status assembleValue(std::string& db_value, const std::string& user_value,
        uint64_t version, const pb::InternalTag* tag = nullptr);

Status disassembleValue(const std::string& db_value, size_t& user_value_size,
        uint64_t& version, pb::InternalTag* tag = nullptr);

} /* namespace storage */
} /* namespace ds */
} /* namespace jim */
