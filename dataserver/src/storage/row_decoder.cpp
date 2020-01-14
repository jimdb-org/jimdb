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

#include "row_decoder.h"

#include <algorithm>
#include <sstream>

#include "common/ds_encoding.h"
#include "field_value.h"
#include "store.h"
#include "util.h"

namespace jim {
namespace ds {
namespace storage {

RowResult::RowResult() {}

RowResult::RowResult(const RowResult &other) {
    CopyFrom(other);
}

RowResult& RowResult::operator=(const RowResult &other) {
    if (this == &other) return *this;

    CopyFrom(other);

    return *this;
}

void RowResult::CopyFrom(const RowResult &other) {
    fields_agg_ = other.fields_agg_;
    key_ = other.key_;
    value_ = other.value_;
    version_ = other.version_;
    col_order_by_infos_ = other.col_order_by_infos_;
    for (const auto &it : other.fields_) {
        fields_[it.first] = CopyValue(*it.second);
    }
    pks_flag_ = other.pks_flag_;
    pks_info_ = other.pks_info_;
}

RowResult::~RowResult() {
    Reset();
}

bool RowResult::AddField(uint64_t col, std::unique_ptr<FieldValue>& field) {
    auto ret = fields_.emplace(col, field.get()).second;
    if (ret) {
        auto p = field.release();
        auto q = fields_[col];
        ret = fcompare(*p, *q, CompareOp::kEqual);
        (void)p;
    }
    return ret;
}

FieldValue* RowResult::GetField(uint64_t col) const {
    auto it = fields_.find(col);
    if (it != fields_.cend()) {
        return it->second;
    } else {
        return nullptr;
    }
}

void RowResult::Reset() {
    key_.clear();
    value_.clear();
    version_ = 0;
    std::for_each(fields_.begin(), fields_.end(),
                  [](std::map<uint64_t, FieldValue*>::value_type& p) { delete p.second; });
    fields_.clear();
    pks_flag_ = false;
}

void RowResult::EncodeTo(const dspb::SelectRequest& req, dspb::RowValue* to) {
    std::string buf;
    for (const auto& field: req.field_list()) {
        if (field.has_column()) {
            auto fv = GetField(field.column().id());
            EncodeFieldValue(&buf, fv);
        }
    }
    to->set_fields(std::move(buf));
    to->set_version(version_);
}

void RowResult::EncodeTo(const std::vector<int> &off_sets, const std::vector<uint64_t> &col_ids, dspb::RowValue* to)
{
    std::string buf;

    if (fields_agg_.empty()) {
        for (const auto &i : off_sets) {
            auto col_id = col_ids.at(i);
            auto fv = GetField(col_id);
            EncodeFieldValue(&buf, fv);
        }

    } else {
        // for aggregation
        buf += fields_agg_;
    }

    to->set_fields(std::move(buf));
    to->set_version(version_);
}

std::unique_ptr<Decoder> Decoder::CreateDecoder( const basepb::KeySchema& key_schema,
        const dspb::SelectRequest& req)
{
    return std::unique_ptr<Decoder>(dynamic_cast<Decoder*> (
            new RowDecoder( key_schema, req )
            ));
}

std::unique_ptr<Decoder> Decoder::CreateDecoder(const basepb::KeySchema& key_schema,
        const dspb::TableRead & req)
{
    return std::unique_ptr<Decoder>(dynamic_cast<Decoder*> (
            new RowDecoder( key_schema, req )
            ));
}

std::unique_ptr<Decoder> Decoder::CreateDecoder(const basepb::KeySchema& key_schema,
        const dspb::IndexRead & req)
{
    return std::unique_ptr<Decoder>(dynamic_cast<Decoder*> (
            new IndexDecoder( key_schema, req )
            ));
}

std::unique_ptr<Decoder> Decoder::CreateDecoder(const basepb::KeySchema& key_schema,
        const dspb::DataSample& req)
{
    return std::unique_ptr<Decoder>(dynamic_cast<Decoder*> (
            new RowDecoder( key_schema, req )
            ));

}

RowDecoder::RowDecoder(const basepb::KeySchema& key_schema, const dspb::SelectRequest& req)
            : key_schema_(key_schema) {

    if (req.has_where_expr()) {
        where_expr_.reset(new dspb::Expr(req.where_expr()));
    }

    for (const auto& field: req.field_list()) {
        if (field.has_column()) {
            cols_.emplace(field.column().id(), field.column());
        }
    }
    if (where_expr_) {
        addExprColumn(*where_expr_);
    }
}

RowDecoder::RowDecoder(const basepb::KeySchema& key_schema, const dspb::TableRead& req)
            : key_schema_(key_schema) {

    for (const auto& col: req.columns()) {
        cols_.emplace(col.id(), col);
    }
}

RowDecoder::RowDecoder(const basepb::KeySchema& key_schema, const dspb::DataSample& req)
            : key_schema_(key_schema) {

    for (const auto& col: req.columns()) {
        cols_.emplace(col.id(), col);
    }
}

void RowDecoder::addExprColumn(const dspb::Expr& expr) {
    if (expr.expr_type() == dspb::Column) {
        cols_.emplace(expr.column().id(), expr.column());
    }
    for (const auto& child: expr.child()) {
        addExprColumn(child);
    }
}

RowDecoder::~RowDecoder() = default;

Status RowDecoder::decodeKeyColumns(const std::string& key, RowResult& result) {
    if (key.size() <= kRowPrefixLength) {
        return Status(Status::kCorruption, "insufficient row key length", EncodeToHexString(key));
    }
    size_t offset = kRowPrefixLength;
    size_t key_size = key.size();

    bool hav_null;
    Status status;
    for (const auto& column: key_schema_.key_cols()) {
        if (read_cols_.empty()) {
            break;
        }

        if ( offset >= key_size) {
            return Status(Status::kCorruption,
                          std::string("decode table key info error offset > key_size. offset:") + std::to_string(offset) + "key_size:" + std::to_string(key_size),
                          "buf:" + EncodeToHexString(key));
        }

        std::unique_ptr<FieldValue> value;
        auto it = read_cols_.find(column.id());
        if (it != read_cols_.end()) {
            status = decodeKeyColumn(key, offset, it->second, &value, hav_null);
        } else {
            status = decodeKeyColumn(key, offset, column, nullptr, hav_null);
        }
        if (!status.ok()) {
            return status;
        }
        if (value != nullptr) {
            if (!result.AddField(column.id(), value)) {
                return Status(Status::kDuplicate, "repeated field on column", std::to_string(column.id()));
            }
        }
        read_cols_.erase(column.id());
    }
    return Status::OK();
}

Status RowDecoder::decodeValueColumns(const std::string& buf, RowResult& result) {

    size_t user_value_size = 0;
    uint64_t version = 0;

    Status ret;
    ret = disassembleValue(buf, user_value_size, version, nullptr);
    if (!ret.ok()) {
        return ret;
    }
    result.SetVersion(static_cast<uint64_t>(version));

    uint32_t col_id = 0;
    EncodeType enc_type;
    for (size_t offset = 0; offset < user_value_size;) {
        if (read_cols_.empty()) {
            break;
        }
        if (!DecodeValueTag(buf, offset, &col_id, &enc_type)) {
            return Status(Status::kCorruption,
                    std::string("decode row value tag failed at offset ") + std::to_string(offset),
                    EncodeToHexString(buf));
        }

        auto it = read_cols_.find(col_id);
        if (it == read_cols_.end()) {
            if (!SkipValueAfterTag(buf, offset, enc_type)) {
               return Status(Status::kCorruption,
                        std::string("decode skip value tag failed at offset ") + std::to_string(offset),
                        EncodeToHexString(buf));
            }
            continue;
        }

        std::unique_ptr<FieldValue> value;
        auto s = decodeValueColumn(buf, offset, it->second, value, enc_type);
        if (!s.ok()) {
            return s;
        }
        if (!result.AddField(it->first, value)) {
            return Status(Status::kDuplicate, "repeated field on column", std::to_string(it->second.id()));
        }
        read_cols_.erase(it->second.id());
    }
    return Status::OK();
}

Status RowDecoder::Decode(const std::string& key, const std::string& buf, RowResult& result) {

    for ( auto & m : cols_) {
        read_cols_.emplace(std::make_pair(m.first, m.second));
    }

    auto s = decodeKeyColumns(key, result);
    if (!s.ok()) {
        return s;
    }

    s = decodeValueColumns(buf, result);
    if (!s.ok()) {
        return s;
    }

    for (auto & m : read_cols_) {
        std::unique_ptr<FieldValue> value;
        auto s = decodeDefaultValueColumn(m.second, &value);
        if (!s.ok()) {
            return s;
        }

        if ( value != nullptr ) {
            if (!result.AddField(m.second.id(), value)) {
                return Status(Status::kDuplicate, "repeated field on column", std::to_string(m.second.id()));
            }
        }
    }

    return Status::OK();
}

Status RowDecoder::DecodeAndFilter(const std::string& key, const std::string& buf,
        RowResult& result, bool& matched) {

    auto s = Decode(key, buf, result);
    if (!s.ok()) {
        return s;
    }

    matched = true;
    if (where_expr_) {
        return filterExpr(result, *where_expr_, matched);
    } else {
        return Status::OK();
    }
}

std::string RowDecoder::DebugString() const {
    std::ostringstream ss;
    ss << "filters: " << (where_expr_ ? where_expr_->ShortDebugString() : "");
    return ss.str();
}

IndexDecoder::IndexDecoder(const basepb::KeySchema& key_schema, const dspb::IndexRead & req)
        : key_schema_(key_schema) {

    for ( const auto &col: req.columns()) {
        cols_.emplace(col.id(), col);
    }
    pks_flag_ = req.pks_flag();
}

void IndexDecoder::addExprColumn(const dspb::Expr& expr) {

}

IndexDecoder::~IndexDecoder() = default;

Status IndexDecoder::decodeKeyColumns(const std::string& key, RowResult& result) {
    if (key.size() <= kRowPrefixLength) {
        return Status(Status::kCorruption, "insufficient index key length", EncodeToHexString(key));
    }

    size_t offset = kRowPrefixLength;
    size_t key_size = key.size();

    Status s;
    bool hav_null = false;

    for (const auto & column: key_schema_.key_cols()) {
        if (read_cols_.empty()) {
            break;
        }

        if ( offset >= key_size) {
            return Status(Status::kCorruption,
                          std::string("decode index key info error offset > key_size. offset:") + std::to_string(offset) + "key_size:" + std::to_string(key_size),
                          "buf:" + EncodeToHexString(key));
        }

        std::unique_ptr<FieldValue> value;
        auto it = read_cols_.find(column.id());
        if ( it != read_cols_.end() ) {
            s = decodeKeyColumn(key, offset, it->second, &value, hav_null);
        } else {
            s = decodeKeyColumn(key, offset, column, nullptr, hav_null);
        }
        if (!s.ok()) {
            return s;
        }

        if ( value != nullptr ) {
            if (!result.AddField(column.id(), value)) {
                return Status(Status::kDuplicate, "repeated field on column", std::to_string(column.id()));
            }
        }
        read_cols_.erase(column.id());
    }

    if ( (hav_null || !key_schema_.unique_index()) && s.ok()) {

        if (!key_schema_.unique_index() && pks_flag_) {
            std::string s_tmp = key.substr(offset);
            result.SetPksInfo(std::move(s_tmp));
            result.SetPksFlag(pks_flag_);
        }

        for (const auto& column: key_schema_.extra_cols()) {
            if (read_cols_.empty()) {
                break;
            }

            if ( offset >= key_size) {
                return Status(Status::kCorruption,
                              std::string("decode index extra info error offset > key_size. offset:") + std::to_string(offset) + "key_size:" + std::to_string(key_size),
                              "buf:" + EncodeToHexString(key));
            }

            std::unique_ptr<FieldValue> value;
            auto it = read_cols_.find(column.id());
            if (it != read_cols_.end()) {
                s = decodeKeyColumn(key, offset, it->second, &value, hav_null);
            } else {
                s = decodeKeyColumn(key, offset, column, nullptr, hav_null);
            }
            if (!s.ok()) {
                return s;
            }
            if (value != nullptr) {
                if (!result.AddField(column.id(), value)) {
                    return Status(Status::kDuplicate, "repeated field on column", std::to_string(column.id()));
                }
            }
            read_cols_.erase(column.id());
        }
    }

    return s;
}

Status IndexDecoder::decodeValueColumns(const std::string& buf, RowResult& result) {
    Status s;
    size_t user_value_size = 0;
    uint64_t version = 0;

    s = disassembleValue(buf, user_value_size, version, nullptr);
    if ( !s.ok()) {
        return s;
    }
    result.SetVersion(static_cast<uint64_t>(version));

    if ( key_schema_.unique_index() && pks_flag_) {
        std::string s_tmp = buf.substr(0, user_value_size);
        result.SetPksInfo(std::move(s_tmp));
        result.SetPksFlag(pks_flag_);
    }

    if (!key_schema_.unique_index()) {
        return Status::OK();
    }

    bool hav_null = false;
    size_t offset = 0;
    for ( const auto & column: key_schema_.extra_cols()) {
        if (read_cols_.empty()) {
            break;
        }

        if ( offset >= user_value_size) {
            return Status(Status::kCorruption,
                    std::string("decode index value info error offset < user_value_size. offset:") + std::to_string(offset) + "user_value_size:" + std::to_string(user_value_size),
                    "buf:" + EncodeToHexString(buf));
        }

        std::unique_ptr<FieldValue> value;
        auto it = read_cols_.find(column.id());
        if (it != read_cols_.end()) {
            s = decodeKeyColumn(buf, offset, it->second, &value, hav_null);
        } else {
            s = decodeKeyColumn(buf, offset, column, nullptr, hav_null);
        }
        if (!s.ok()) {
            return s;
        }

        if (value != nullptr) {
            if (!result.AddField(column.id(), value)) {
                return Status(Status::kDuplicate, "repeated field on index value column", std::to_string(column.id()));
            }
        }
        read_cols_.erase(column.id());
    }

    return s;
}

Status IndexDecoder::Decode(const std::string& key, const std::string& buf, RowResult& result) {

    for ( auto & m : cols_) {
        read_cols_.emplace(std::make_pair(m.first, m.second));
    }

    auto s = decodeKeyColumns(key, result);
    //非唯一索引bug为空不需要解析
    if (!s.ok() || buf.empty()) {
        return s;
    }

    s = decodeValueColumns(buf, result);
    if (!s.ok()) {
        return s;
    }

    for (auto & m : read_cols_) {
        std::unique_ptr<FieldValue> value;
        auto s = decodeDefaultValueColumn(m.second, &value);
        if (!s.ok()) {
            return s;
        }

        if ( value != nullptr ) {
            if (!result.AddField(m.second.id(), value)) {
                return Status(Status::kDuplicate, "repeated field on default value column", std::to_string(m.second.id()));
            }
        }
    }

    return Status::OK();
}

Status IndexDecoder::DecodeAndFilter(const std::string& key, const std::string& buf,
                                   RowResult& result, bool& matched) {

    Status s;
    matched = true;
    s = Decode(key, buf, result);
    return s;
}

std::string IndexDecoder::DebugString() const {
    return "";
}

} /* namespace storage */
} /* namespace ds */
} /* namespace jim */
