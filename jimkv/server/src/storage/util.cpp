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

#include "util.h"

#include <chrono>

#include "base/byte_order.h"
#include "common/ds_encoding.h"

namespace jimkv {
namespace ds {
namespace storage {

using namespace std::chrono;

uint64_t calExpireAt(uint64_t ttl) {
    auto epoch = system_clock::now().time_since_epoch();
    return ttl + duration_cast<milliseconds>(epoch).count();
}

bool isExpired(uint64_t expired_at) {
    auto epoch = system_clock::now().time_since_epoch();
    auto now = duration_cast<milliseconds>(epoch).count();
    return static_cast<uint64_t>(now) > expired_at;
}

static const uint8_t kEncodeNull= 0x00;
Status decodeKeyColumn(const std::string& key, size_t& offset,
        const dspb::ColumnInfo& col, std::unique_ptr<FieldValue>* field,
        bool& hav_null) {

    Status s;
    if ( offset > key.size()) {
        s = Status(Status::kCorruption,
                std::string("decode key failed at offset") + std::to_string(offset),
                EncodeToHexString(key) );
    }

    if ( key.at(offset) == kEncodeNull) {
        offset++;
        hav_null = true;
        field = nullptr;
        return s;
    }

    switch (col.typ()) {
        case basepb::TinyInt:
        case basepb::SmallInt:
        case basepb::MediumInt:
        case basepb::Int:
        case basepb::BigInt:
        case basepb::Year: {
            if (col.unsigned_()) {
                uint64_t i = 0;
                if (!DecodeUvarintAscending(key, offset, &i)) {
                    return Status( Status::kCorruption,
                            std::string("decode row unsigned int key failed at offset ") + std::to_string(offset),
                            EncodeToHexString(key));
                }
                if (field != nullptr) field->reset(new FieldValue(i));
            } else {
                int64_t i = 0;
                if (!DecodeVarintAscending(key, offset, &i)) {
                    return Status( Status::kCorruption,
                            std::string("decode row int key failed at offset ") + std::to_string(offset),
                            EncodeToHexString(key));
                }
                if (field != nullptr) {
                    field->reset(new FieldValue(i));
                }
            }
            return Status::OK();
        }

        case basepb::Float:
        case basepb::Double: {
            double d = 0;
            if (!DecodeFloatAscending(key, offset, &d)) {
                return Status(Status::kCorruption,
                        std::string("decode row float key failed at offset ") + std::to_string(offset),
                        EncodeToHexString(key));
            }
            if (field != nullptr) {
                field->reset(new FieldValue(d));
            }
            return Status::OK();
        }

        case basepb::Varchar:
        case basepb::Binary:
        case basepb::Char:
        case basepb::NChar:
        case basepb::Text:
        case basepb::VarBinary:
        case basepb::Json: {
            std::string s;
            if (!DecodeBytesAscending(key, offset, &s)) {
                return Status(Status::kCorruption,
                        std::string("decode row string key failed at offset ") + std::to_string(offset),
                        EncodeToHexString(key));
            }
            if (field != nullptr) {
                field->reset(new FieldValue(std::move(s)));
            }
            return Status::OK();
        }

        case basepb::Decimal: {
            datatype::MyDecimal d;
            if (!DecodeDecimalAscending(key, offset, &d)) {
                return Status(Status::kCorruption,
                        std::string("decode row decimal key failed at offset ") + std::to_string(offset),
                        EncodeToHexString(key));
            }
            if (field != nullptr) {
                field->reset(new FieldValue(d));
            }
            return Status::OK();
        }
        case basepb::Date:
        case basepb::DateTime:
        case basepb::TimeStamp: {
            datatype::MyDateTime dt;
            if ( !DecodeDateAscending(key, offset, &dt)) {
                return Status(Status::kCorruption,
                        std::string("decode row datetime key failed at offset ") + std::to_string(offset),
                        EncodeToHexString(key));
            }
            if (field != nullptr) {
                field->reset(new FieldValue(dt));
            }
            return Status::OK();
        }
        case basepb::Time: {
            datatype::MyTime t;
            if ( !DecodeTimeAscending(key, offset, &t)) {
                return Status(Status::kCorruption,
                        std::string("decode row time key failed at offset ") + std::to_string(offset),
                        EncodeToHexString(key));
            }
            if (field != nullptr) {
                field->reset(new FieldValue(t));
            }
            return Status::OK();
        }

        default:
            return Status(Status::kNotSupported, "unknown decode field type", std::to_string(col.id()));
    }
}

Status decodeKeyColumn(const std::string& key, size_t& offset,
        const basepb::KeySchema_ColumnInfo& col, std::unique_ptr<FieldValue>* field,
        bool& hav_null) {

    Status s;
    if ( offset > key.size()) {
        return Status(Status::kCorruption,
                   std::string("decode key failed at offset") + std::to_string(offset),
                   EncodeToHexString(key) );
    }

    if ( key.at(offset) == kEncodeNull) {
        offset++;
        hav_null = true;
        field = nullptr;
        return s;
    }

    switch (col.type()) {
        case basepb::TinyInt:
        case basepb::SmallInt:
        case basepb::MediumInt:
        case basepb::Int:
        case basepb::BigInt:
        case basepb::Year: {
            if (col.unsigned_()) {
                uint64_t i = 0;
                if (!DecodeUvarintAscending(key, offset, &i)) {
                    return Status( Status::kCorruption,
                                   std::string("decode row unsigned int key failed at offset ") + std::to_string(offset),
                                   EncodeToHexString(key));
                }
                if (field != nullptr) field->reset(new FieldValue(i));
            } else {
                int64_t i = 0;
                if (!DecodeVarintAscending(key, offset, &i)) {
                    return Status( Status::kCorruption,
                                   std::string("decode row int key failed at offset ") + std::to_string(offset),
                                   EncodeToHexString(key));
                }
                if (field != nullptr) {
                    field->reset(new FieldValue(i));
                }
            }
            return Status::OK();
        }

        case basepb::Float:
        case basepb::Double: {
            double d = 0;
            if (!DecodeFloatAscending(key, offset, &d)) {
                return Status(Status::kCorruption,
                              std::string("decode row float key failed at offset ") + std::to_string(offset),
                              EncodeToHexString(key));
            }
            if (field != nullptr) {
                field->reset(new FieldValue(d));
            }
            return Status::OK();
        }

        case basepb::Varchar:
        case basepb::Binary:
        case basepb::Char:
        case basepb::NChar:
        case basepb::Text:
        case basepb::VarBinary:
        case basepb::Json: {
            std::string s;
            if (!DecodeBytesAscending(key, offset, &s)) {
                return Status(Status::kCorruption,
                              std::string("decode row string key failed at offset ") + std::to_string(offset),
                              EncodeToHexString(key));
            }
            if (field != nullptr) {
                field->reset(new FieldValue(std::move(s)));
            }
            return Status::OK();
        }

        case basepb::Decimal: {
            datatype::MyDecimal d;
            if (!DecodeDecimalAscending(key, offset, &d)) {
                return Status(Status::kCorruption,
                              std::string("decode row decimal key failed at offset ") + std::to_string(offset),
                              EncodeToHexString(key));
            }
            if (field != nullptr) {
                field->reset(new FieldValue(d));
            }
            return Status::OK();
        }
        case basepb::Date:
        case basepb::DateTime:
        case basepb::TimeStamp: {
            datatype::MyDateTime dt;
            if ( !DecodeDateAscending(key, offset, &dt)) {
                return Status(Status::kCorruption,
                              std::string("decode row datetime key failed at offset ") + std::to_string(offset),
                              EncodeToHexString(key));
            }
            if (field != nullptr) {
                field->reset(new FieldValue(dt));
            }
            return Status::OK();
        }
        case basepb::Time: {
            datatype::MyTime t;
            if ( !DecodeTimeAscending(key, offset, &t)) {
                return Status(Status::kCorruption,
                              std::string("decode row time key failed at offset ") + std::to_string(offset),
                              EncodeToHexString(key));
            }
            if (field != nullptr) {
                field->reset(new FieldValue(t));
            }
            return Status::OK();
        }

        default:
            return Status(Status::kNotSupported, "unknown decode field type", std::to_string(col.id()));
    }
}

Status decodeValueColumn(const std::string& buf, size_t& offset,
                         const dspb::ColumnInfo& col, std::unique_ptr<FieldValue>& field) {

    switch (col.typ()) {
        case basepb::TinyInt:
        case basepb::SmallInt:
        case basepb::MediumInt:
        case basepb::Int:
        case basepb::BigInt:
        case basepb::Year: {
            int64_t i = 0;
            if (!DecodeIntValue(buf, offset, &i)) {
                return Status( Status::kCorruption,
                               std::string("decode row int value failed at offset ") + std::to_string(offset),
                               EncodeToHexString(buf));
            }
            if (col.unsigned_()) {
                field.reset(new FieldValue(static_cast<uint64_t>(i)));
            } else {
                field.reset(new FieldValue(i));
            }
            return Status::OK();
        }
        case basepb::Float:
        case basepb::Double: {
            double d = 0;
            if (!DecodeFloatValue(buf, offset, &d)) {
                return Status(Status::kCorruption,
                              std::string("decode row float value failed at offset ") + std::to_string(offset),
                              EncodeToHexString(buf));
            }
            field.reset(new FieldValue(d));
            return Status::OK();
        }
        case basepb::Varchar:
        case basepb::Binary:
        case basepb::Char:
        case basepb::NChar:
        case basepb::Text:
        case basepb::VarBinary:
        case basepb::Json: {
            std::string s;
            if (!DecodeBytesValue(buf, offset, &s)) {
                return Status(Status::kCorruption,
                              std::string("decode row string value failed at offset ") + std::to_string(offset),
                              EncodeToHexString(buf));
            }
            field.reset(new FieldValue(std::move(s)));
            return Status::OK();
        }
        case basepb::Decimal: {
            datatype::MyDecimal d;
            if (!DecodeDecimalValue(buf, offset, &d)) {
                return Status(Status::kCorruption,
                              std::string("decode row decimal value failed at offset ") + std::to_string(offset),
                              EncodeToHexString(buf));
            }
            field.reset(new FieldValue(d));
            return Status::OK();
        }
        case basepb::Date:
        case basepb::DateTime:
        case basepb::TimeStamp: {
            datatype::MyDateTime dt;
            if (!DecodeDateValue(buf, offset, &dt)) {
                return Status(Status::kCorruption,
                              std::string("decode row datetime value failed at offset ") + std::to_string(offset),
                              EncodeToHexString(buf));
            }
            field.reset(new FieldValue(dt));
            return Status::OK();
        }
        case basepb::Time: {
            datatype::MyTime t;
            if (!DecodeTimeValue(buf, offset, &t)) {
                return Status(Status::kCorruption,
                              std::string("decode row time value failed at offset ") + std::to_string(offset),
                              EncodeToHexString(buf));
            }

            field.reset(new FieldValue(t));
            return Status::OK();
        }

        default:
            return Status(Status::kNotSupported, "unknown decode field type", std::to_string(col.typ()));
    }
}

Status decodeValueColumn(const std::string& buf, size_t& offset,
        const dspb::ColumnInfo& col, std::unique_ptr<FieldValue>& field, const jimkv::EncodeType & type) {

    switch (col.typ()) {
        case basepb::TinyInt:
        case basepb::SmallInt:
        case basepb::MediumInt:
        case basepb::Int:
        case basepb::BigInt:
        case basepb::Year: {

            if (EncodeType::Int != type) {
                return Status(
                    Status::kCorruption,
                    std::string("decode row int type failed at offset ") + std::to_string(offset),
                    EncodeToHexString(buf));
            }

            int64_t i = 0;
            if (!DecodeIntValueAfterTag(buf, offset, &i)) {
                return Status( Status::kCorruption,
                        std::string("decode row int value failed at offset ") + std::to_string(offset),
                        EncodeToHexString(buf));
            }
            if (col.unsigned_()) {
                field.reset(new FieldValue(static_cast<uint64_t>(i)));
            } else {
                field.reset(new FieldValue(i));
            }
            return Status::OK();
        }
        case basepb::Float:
        case basepb::Double: {

            if ( EncodeType::Float != type) {
                return Status(
                        Status::kCorruption,
                        std::string("decode row float type failed at offset ") + std::to_string(offset),
                        EncodeToHexString(buf));
            }

            double d = 0;
            if (!DecodeFloatValueAfterTag(buf, offset, &d)) {
                return Status(Status::kCorruption,
                        std::string("decode row float value failed at offset ") + std::to_string(offset),
                        EncodeToHexString(buf));
            }
            field.reset(new FieldValue(d));
            return Status::OK();
        }
        case basepb::Varchar:
        case basepb::Binary:
        case basepb::Char:
        case basepb::NChar:
        case basepb::Text:
        case basepb::VarBinary:
        case basepb::Json: {

            if ( EncodeType::Bytes != type) {
                return Status(
                        Status::kCorruption,
                        std::string("decode row string type failed at offset ") + std::to_string(offset),
                        EncodeToHexString(buf));
            }

            std::string s;
            if (!DecodeBytesValueAfterTag(buf, offset, &s)) {
                return Status(Status::kCorruption,
                        std::string("decode row string value failed at offset ") + std::to_string(offset),
                        EncodeToHexString(buf));
            }
            field.reset(new FieldValue(std::move(s)));
            return Status::OK();
        }
        case basepb::Decimal: {

            if ( EncodeType::Decimal != type) {
                return Status(
                        Status::kCorruption,
                        std::string("decode row decimal type failed at offset ") + std::to_string(offset),
                        EncodeToHexString(buf));
            }

            datatype::MyDecimal d;
            if (!DecodeDecimalValueAfterTag(buf, offset, &d)) {
                return Status(Status::kCorruption,
                        std::string("decode row decimal value failed at offset ") + std::to_string(offset),
                        EncodeToHexString(buf));
            }
            field.reset(new FieldValue(d));
            return Status::OK();
        }
        case basepb::Date:
        case basepb::DateTime:
        case basepb::TimeStamp: {

            if ( EncodeType::Date != type) {
                return Status(
                        Status::kCorruption,
                        std::string("decode row date type failed at offset ") + std::to_string(offset),
                        EncodeToHexString(buf));
            }

            datatype::MyDateTime dt;
            if (!DecodeDateValueAfterTag(buf, offset, &dt)) {
                return Status(Status::kCorruption,
                        std::string("decode row datetime value failed at offset ") + std::to_string(offset),
                        EncodeToHexString(buf));
            }
            field.reset(new FieldValue(dt));
            return Status::OK();
        }
        case basepb::Time: {

            if ( EncodeType::Time != type) {
                return Status(
                        Status::kCorruption,
                        std::string("decode row time type failed at offset ") + std::to_string(offset),
                        EncodeToHexString(buf));
            }

            datatype::MyTime t;
            if (!DecodeTimeValueAfterTag(buf, offset, &t)) {
                return Status(Status::kCorruption,
                        std::string("decode row time value failed at offset ") + std::to_string(offset),
                        EncodeToHexString(buf));
            }

            field.reset(new FieldValue(t));
            return Status::OK();
        }

        default:
            return Status(Status::kNotSupported, "unknown decode field type", std::to_string(col.typ()));
    }
}

Status decodeDefaultValueColumn(const dspb::ColumnInfo& col, std::unique_ptr<FieldValue>* field) {

    std::string reorg_value = col.reorg_value();
    if (reorg_value.empty()) {
        field = nullptr;
        return Status::OK();
    }

    size_t offset = 0;
    auto s = decodeValueColumn(reorg_value, offset, col, *field);
    return s;
}

void fillColumnInfo(const basepb::ColumnInfo& col, dspb::ColumnInfo* info) {
    info->set_id(col.id());
    info->set_typ(col.sql_type().type());
    info->set_unsigned_(col.sql_type().unsigned_());
}

void makeColumnExpr(const basepb::ColumnInfo& col, dspb::Expr* expr) {
    assert(expr != nullptr);
    expr->set_expr_type(dspb::Column);
    auto column_info = expr->mutable_column();
    fillColumnInfo(col, column_info);
}

void makeConstValExpr(const basepb::ColumnInfo& col, const std::string& value, dspb::Expr* expr) {
    assert(expr != nullptr);
    expr->set_value(value);
    switch (col.sql_type().type()) {
        case basepb::TinyInt:
        case basepb::SmallInt:
        case basepb::MediumInt:
        case basepb::Int:
        case basepb::BigInt:
        case basepb::Year:
            expr->set_expr_type(col.sql_type().unsigned_() ? dspb::Const_UInt : dspb::Const_Int);
            break;
        case basepb::Float:
        case basepb::Double:
            expr->set_expr_type(dspb::Const_Double);
            break;
        case basepb::Varchar:
        case basepb::Binary:
        case basepb::Char:
        case basepb::NChar:
        case basepb::Text:
        case basepb::VarBinary:
        case basepb::Json:
            expr->set_expr_type(dspb::Const_Bytes);
            break;
        case basepb::Decimal:
            expr->set_expr_type(dspb::Const_Decimal);
            break;
        case basepb::Date:
        case basepb::DateTime:
        case basepb::TimeStamp:
            expr->set_expr_type(dspb::Const_Date);
            break;
        case basepb::Time:
            expr->set_expr_type(dspb::Const_Time);
            break;
        default:
            expr->set_expr_type(dspb::Invalid_Expr);
    }
}

struct ScopedFV {
    FieldValue *val = nullptr;
    bool is_ref = false;

    ScopedFV(FieldValue *val_arg, bool is_ref_arg) : 
        val(val_arg), is_ref(is_ref_arg) {}

    ~ScopedFV() { if (!is_ref) { delete val;} }

    ScopedFV(const ScopedFV&) = delete;
    ScopedFV& operator=(const ScopedFV&) = delete;
};

using ScopedFVPtr = std::unique_ptr<ScopedFV>;

Status evaluateExpr(const RowResult& row, const dspb::Expr& expr, ScopedFVPtr& val) {
    switch (expr.expr_type()) {
    case dspb::Column: {
        auto fv = row.GetField(expr.column().id());
        if (fv != nullptr) {
            val.reset(new ScopedFV(fv, true));
        }
        break;
    }
    case dspb::Const_Int: {
        int64_t i = strtoll(expr.value().c_str(), NULL, 10);
        auto fv = new FieldValue(i);
        val.reset(new ScopedFV(fv, false));
        break;
    }
    case dspb::Const_UInt: {
        uint64_t i = strtoull(expr.value().c_str(), NULL, 10);
        auto fv = new FieldValue(i);
        val.reset(new ScopedFV(fv, false));
        break;
    }
    case dspb::Const_Double: {
        double d = strtod(expr.value().c_str(), NULL);
        auto fv = new FieldValue(d);
        val.reset(new ScopedFV(fv, false));
        break;
    }
    case dspb::Const_Bytes: {
        auto s = new std::string(expr.value());
        auto fv = new FieldValue(s);
        val.reset(new ScopedFV(fv, false));
        break;
    }
    case dspb::Const_Decimal: {
        datatype::MyDecimal d;
        std::string str(expr.value());
        int32_t error = d.FromString(str);
        if (error != datatype::E_DEC_OK) {
            val.reset(nullptr);
        } else {
            auto fv = new FieldValue(d);
            val.reset( new ScopedFV(fv, false));
        }
        break;
    }
    case dspb::Const_Date: {
        datatype::MyDateTime dt;
        datatype::MyDateTime::StWarn st;

        std::string str_tmp(expr.value());
        bool ret_b = dt.FromString( str_tmp, st);
        if ( !ret_b ) {
            dt.SetZero();
        }

        auto fv = new FieldValue(dt);
        val.reset(new ScopedFV(fv, false));
        break;
    }
    case dspb::Const_Time: {
        datatype::MyTime t;
        datatype::MyTime::StWarn st;

        std::string str_tmp(expr.value());
        bool ret_b = t.FromString( str_tmp, st);
        if (!ret_b) {
            t.SetZero();
        }

        auto fv = new FieldValue(t);
        val.reset(new ScopedFV(fv, false));
        break;
    }
    case dspb::Plus:
    case dspb::PlusReal:
    case dspb::Minus:
    case dspb::MinusReal:
    case dspb::Mult:
    case dspb::MultReal:
    case dspb::Mod:
    case dspb::ModReal:
    case dspb::Div:
    case dspb::DivReal: {
        if (expr.child_size() != 2) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc, rc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }
        s = evaluateExpr(row, expr.child(1), rc);
        if (!s.ok()) {
            return s;
        }
        if (lc == nullptr || rc == nullptr) {
            return Status::OK();
        }
        auto result = arithCalc(lc->val, rc->val, expr.expr_type());
        if (result) {
            val.reset(new ScopedFV(result.release(), false));
        }
        break;
    }

    case dspb::PlusInt:
    case dspb::MinusInt:
    case dspb::MultInt:
    case dspb::MultIntUnsigned:
    case dspb::IntDivInt:
    case dspb::ModInt: {

        if (expr.child_size() != 2) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc, rc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        s = evaluateExpr(row, expr.child(1), rc);
        if (!s.ok()) {
            return s;
        }

        if (lc == nullptr || rc == nullptr) {
            return Status::OK();
        }

        if (
                !(((lc->val->Type() == FieldType::kInt) && (rc->val->Type() == FieldType::kInt)) ||
                  ((lc->val->Type() == FieldType::kUInt) && (rc->val->Type() == FieldType::kUInt)))
                ) {

            return Status(Status::kTypeConflict, dspb::ExprType_Name(expr.expr_type()) + "Int arithmetic operation type Confflict ",
                          "lc type:[" + lc->val->TypeString() +"], rc type:[" + rc->val->TypeString() + "]" );
        }

        auto result = arithCalc(lc->val, rc->val, expr.expr_type());

        if (result) {
            val.reset(new ScopedFV(result.release(), false));
        } else {
            return Status(Status::kTypeConflict,
                    "arithCalc Failed.",
                    lc->val->ToString() + " " + dspb::ExprType_Name(expr.expr_type()) + " " + rc->val->ToString()
                    );
        }

        break;
    }

    case dspb::PlusDecimal:
    case dspb::MinusDecimal:
    case dspb::MultDecimal:
    case dspb::DivDecimal:
    case dspb::ModDecimal: {

        if (expr.child_size() != 2) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc, rc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        s = evaluateExpr(row, expr.child(1), rc);
        if (!s.ok()) {
            return s;
        }

        if (lc == nullptr || rc == nullptr) {
            return Status::OK();
        }
        if ( lc->val->Type() != FieldType::kDecimal ||
             rc->val->Type() != FieldType::kDecimal ) {
            return Status(Status::kTypeConflict,
                          "Decimal arithmetic operation type Confflict ",
                          "lc type:[" + lc->val->TypeString() +"], rc type:[" + rc->val->TypeString() + "]"
            );
        }

        auto result = arithCalc(lc->val, rc->val, expr.expr_type());

        if (result) {
            val.reset(new ScopedFV(result.release(), false));
        } else {
            return Status(Status::kTypeConflict,
                          "arithCalc Failed.",
                          lc->val->ToString() + " " + dspb::ExprType_Name(expr.expr_type()) + " " + rc->val->ToString()
            );
        }

        break;
    }

    case dspb::CastIntToInt: {
        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if (
                !((lc->val->Type() == FieldType::kInt) || (lc->val->Type() == FieldType::kUInt))
                ) {

            return Status(Status::kTypeConflict, dspb::ExprType_Name(expr.expr_type()) + "CastIntToInt type Confflict ",
                    lc->val->TypeString() );
        }

        val.reset(new ScopedFV( new FieldValue(lc->val->Int()), false));
        break;
    }

    case dspb::CastIntToReal: {
        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if (
                !((lc->val->Type() == FieldType::kInt) || (lc->val->Type() == FieldType::kUInt))
                ) {

            return Status(Status::kTypeConflict, dspb::ExprType_Name(expr.expr_type()) + "CastIntToReal type Confflict ",
                    lc->val->TypeString() );
        }

        val.reset(new ScopedFV( new FieldValue(double(lc->val->Int())), false));
        break;
    }

    case dspb::CastIntToString: {
        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if (
                !((lc->val->Type() == FieldType::kInt) || (lc->val->Type() == FieldType::kUInt))
                ) {

            return Status(Status::kTypeConflict, dspb::ExprType_Name(expr.expr_type()) + "CastIntToString type Confflict ",
                    lc->val->TypeString() );
        }

        val.reset(new ScopedFV( new FieldValue(lc->val->ToString()) , false));
        break;
    }

    case dspb::CastIntToDecimal: {
        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if (
                !((lc->val->Type() == FieldType::kInt) || (lc->val->Type() == FieldType::kUInt))
                ) {

            return Status(Status::kTypeConflict, dspb::ExprType_Name(expr.expr_type()) + "CastIntToDecimal type Confflict ",
                          lc->val->TypeString() );
        }

        datatype::MyDecimal d;
        d.FromInt(lc->val->Int());

        val.reset(new ScopedFV( new FieldValue(d) , false));
        break;
    }

    case dspb::CastIntToDate: {
        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if (
                !((lc->val->Type() == FieldType::kInt) || (lc->val->Type() == FieldType::kUInt))
                ) {

            return Status(Status::kTypeConflict, dspb::ExprType_Name(expr.expr_type()) + "CastIntToDate type Conflict ",
                          lc->val->TypeString() );
        }

        datatype::MyDateTime dt;
        datatype::MyDateTime::StWarn st;
        bool ret_b = dt.FromNumberUint64(lc->val->Int(), st);
        if (!ret_b) {
            dt.SetZero();
        }

        val.reset(new ScopedFV( new FieldValue(dt) , false));
        break;
    }

    case dspb::CastIntToTime: {

        if ( expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if (
                !((lc->val->Type() == FieldType::kInt) || (lc->val->Type() == FieldType::kUInt))
                ) {

             return Status(Status::kTypeConflict, dspb::ExprType_Name(expr.expr_type()) + "CastIntToTime type Conflict",
                     lc->val->TypeString());
        }

        datatype::MyTime t;
        datatype::MyTime::StWarn st;

        bool ret_b = t.FromNumberInt64(lc->val->Int(), st);
        if (!ret_b) {
            t.SetZero();
        }

        val.reset(new ScopedFV( new FieldValue(t), false));
        break;
    }

    case dspb::CastRealToInt: {
        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kDouble ) {
            return Status(Status::kTypeConflict,
                    "CastRealToInt type Confflict ",
                    lc->val->TypeString()
                    );
        }

        val.reset(new ScopedFV( new FieldValue(int64_t(lc->val->Double())) , false));
        break;
    }

    case dspb::CastRealToReal: {
        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kDouble ) {
            return Status(Status::kTypeConflict,
                    "CastRealToReal type Confflict ",
                    lc->val->TypeString()
                    );
        }

        val.reset(new ScopedFV( new FieldValue(lc->val->Double()) , false));
        break;
    }

    case dspb::CastRealToString: {
        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kDouble ) {
            return Status(Status::kTypeConflict,
                    "CastRealToString type Confflict ",
                    lc->val->TypeString()
                    );
        }

        val.reset(new ScopedFV( new FieldValue(lc->val->ToString()) , false));
        break;
    }

    case dspb::CastRealToDecimal: {
        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kDouble ) {
            return Status(Status::kTypeConflict,
                          "CastRealToDecimal type Confflict ",
                          lc->val->TypeString()
            );
        }

        double dval = lc->val->Double();
        datatype::MyDecimal dec;
        dec.FromFloat64(dval);

        val.reset(new ScopedFV( new FieldValue(dec), false));
        break;
    }

    case dspb::CastRealToDate: {

        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kDouble ) {
            return Status(Status::kTypeConflict,
                          "CastRealToDate type Confflict ",
                          lc->val->TypeString()
            );
        }

        double dval = lc->val->Double();
        datatype::MyDateTime dt;
        datatype::MyDateTime::StWarn st;
        bool ret_b = dt.FromNumberFloat64(dval, st);
        if (!ret_b) {
            dt.SetZero();
        }

        val.reset(new ScopedFV( new FieldValue(dt), false));
        break;
    }

    case dspb::CastRealToTime: {

        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kDouble ) {
            return Status(Status::kTypeConflict,
                          "CastRealToTime type Confflict ",
                          lc->val->TypeString()
            );
        }

        double dval = lc->val->Double();
        datatype::MyTime t;
        datatype::MyTime::StWarn st;
        bool ret_b = t.FromNumberFloat64(dval, st);
        if (!ret_b) {
            t.SetZero();
        }

        val.reset(new ScopedFV( new FieldValue(t), false));
        break;
    }

    case dspb::CastStringToInt: {

        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kBytes) {
            return Status(Status::kTypeConflict,
                    "CastStringToInt type Confflict ",
                    lc->val->TypeString()
                    );
        }

        auto str_tmp = lc->val->Bytes();
        auto long_tmp = std::stol(str_tmp);

        val.reset(new ScopedFV(new FieldValue(static_cast<int64_t>(long_tmp)), false));
        break;
    }
    case dspb::CastStringToReal: {

        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kBytes) {
            return Status(Status::kTypeConflict,
                    "CastStringToReal type Confflict ",
                    lc->val->TypeString()
                    );
        }

        auto str_tmp = lc->val->Bytes();
        auto long_tmp = strtod(str_tmp.c_str(), NULL);

        val.reset(new ScopedFV(new FieldValue(long_tmp), false ));
        break;
    }

    case dspb::CastStringToString: {

        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kBytes) {
            return Status(Status::kTypeConflict,
                    "CastStringToString type Confflict ",
                    lc->val->TypeString()
                    );
        }

        val.reset(new ScopedFV( new FieldValue(lc->val->ToString()), false));
        break;
    }
    case dspb::CastStringToDecimal: {

            if (expr.child_size() != 1) {
                return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
            }

            ScopedFVPtr lc;
            auto s = evaluateExpr(row, expr.child(0), lc);
            if (!s.ok()) {
                return s;
            }

            if ( lc->val->Type() != FieldType::kBytes) {
                return Status(Status::kTypeConflict,
                              "CastStringToDecimal type Confflict ",
                              lc->val->TypeString()
                );
            }

            std::string str(lc->val->Bytes());
            datatype::MyDecimal d;
            int32_t error = d.FromString(str);
            if ( error != datatype::E_DEC_OK ) {
                val.reset(new ScopedFV(nullptr, false));
            } else {
                val.reset(new ScopedFV( new FieldValue(d), false ));
            }

           break;
        }

    case dspb::CastStringToDate: {

        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kBytes) {
            return Status(Status::kTypeConflict,
                          "CastStringToDate type Confflict ",
                          lc->val->TypeString()
            );
        }

        std::string str(lc->val->Bytes());
        datatype::MyDateTime dt;
        datatype::MyDateTime::StWarn st;
        bool ret = dt.FromString(str, st);
        if ( !ret ) {
            val.reset(new ScopedFV(nullptr, false));
        } else {
            val.reset(new ScopedFV( new FieldValue(dt), false ));
        }

        break;
    }

    case dspb::CastStringToTime: {

        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kBytes) {
            return Status(Status::kTypeConflict,
                          "CastStringToTime type Confflict ",
                          lc->val->TypeString()
            );
        }

        std::string str(lc->val->Bytes());
        datatype::MyTime t;
        datatype::MyTime::StWarn st;
        bool ret = t.FromString(str, st);
        if ( !ret ) {
            val.reset(new ScopedFV(nullptr, false));
        } else {
            val.reset(new ScopedFV( new FieldValue(t), false ));
        }

        break;
    }

    case dspb::CastDecimalToInt: {

        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kDecimal) {
            return Status(Status::kTypeConflict,
                          "CastDecimalToInt type Confflict ",
                          lc->val->TypeString()
            );
        }

        int32_t error = 0;
        int64_t v = 0;
        lc->val->Decimal().ToInt(v, error);

        val.reset(new ScopedFV( new FieldValue(v), false ));
        break;
    }

    case dspb::CastDecimalToReal: {

        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kDecimal) {
            return Status(Status::kTypeConflict,
                          "CastDecimalToReal type Confflict ",
                          lc->val->TypeString()
            );
        }

        double dval = 0.0;
        int32_t tmp_err = 0;
        lc->val->Decimal().ToFloat64(dval, tmp_err);

        val.reset(new ScopedFV( new FieldValue(dval), false ));
        break;
    }

    case dspb::CastDecimalToDecimal: {

        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kDecimal) {
            return Status(Status::kTypeConflict,
                          "CastDecimalToDecimal type Confflict ",
                          lc->val->TypeString()
            );
        }

        val.reset(new ScopedFV( new FieldValue(lc->val->Decimal()), false ));
        break;
    }

    case dspb::CastDecimalToString: {

        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kDecimal) {
            return Status(Status::kTypeConflict,
                          "CastDecimalToString type Confflict ",
                          lc->val->TypeString()
            );
        }

        val.reset(new ScopedFV( new FieldValue(lc->val->Decimal().ToString()), false ));
        break;
    }

    case dspb::CastDecimalToDate: {
        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kDecimal) {
            return Status(Status::kTypeConflict,
                          "CastDecimalToDate type Conflict ",
                          lc->val->TypeString()
            );
        }

        auto dec_tmp = lc->val->Decimal();
        auto str_tmp = dec_tmp.ToString();
        datatype::MyDateTime dt;
        datatype::MyDateTime::StWarn st;

        bool ret_tmp = dt.FromString(str_tmp, st);
        if (!ret_tmp) {
            dt.SetZero();
        }

        val.reset(new ScopedFV( new FieldValue(dt), false));
        break;
    }

    case dspb::CastDecimalToTime: {

        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kDecimal) {
            return Status(Status::kTypeConflict,
                          "CastDecimalToDate type Conflict ",
                          lc->val->TypeString()
            );
        }

        auto dec_tmp = lc->val->Decimal();
        auto str_tmp = dec_tmp.ToString();
        datatype::MyTime t;
        datatype::MyTime::StWarn st;

        bool ret_tmp = t.FromString(str_tmp, st);
        if (!ret_tmp) {
            t.SetZero();
        }

        val.reset(new ScopedFV( new FieldValue(t), false));
        break;
    }

    case dspb::CastDateToInt: {

        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kDate) {
            return Status(Status::kTypeConflict,
                          "CastDecimalToDate type Conflict ",
                          lc->val->TypeString()
            );
        }

        auto dt_tmp = lc->val->Date();
        auto int64_tmp = static_cast<int64_t>(dt_tmp.ToNumberUint64());
        val.reset(new ScopedFV( new FieldValue(int64_tmp), false));
        break;
    }

    case dspb::CastDateToReal: {

        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kDate) {
            return Status(Status::kTypeConflict,
                          "CastDecimalToDate type Conflict ",
                          lc->val->TypeString()
            );
        }

        auto dt_tmp = lc->val->Date();
        val.reset(new ScopedFV( new FieldValue( dt_tmp.ToNumberFloat64()), false));
        break;

    }

    case dspb::CastDateToString: {

        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kDate) {
            return Status(Status::kTypeConflict,
                          "CastDateToString type Conflict ",
                          lc->val->TypeString()
            );
        }

        val.reset(new ScopedFV( new FieldValue(lc->val->Date().ToString()), false ));
        break;
    }

    case dspb::CastDateToDecimal: {

        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kDate) {
            return Status(Status::kTypeConflict,
                          "CastDateToDecimal type Conflict ",
                          lc->val->TypeString()
            );
        }

        auto dt_tmp = lc->val->Date();
        datatype::MyDecimal dec;
        int32_t ret_tmp = dec.FromString(dt_tmp.ToNumberString());
        if ( ret_tmp != 0) {
            dec.ResetZero();
        }

        val.reset(new ScopedFV(new FieldValue(dec), false));
        break;
    }

    case dspb::CastDateToDate: {

        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kDate) {
            return Status(Status::kTypeConflict,
                          "CastDateToDecimal type Conflict ",
                          lc->val->TypeString()
            );
        }

        auto dt_tmp = lc->val->Date();
        val.reset(new ScopedFV(new FieldValue(dt_tmp), false));
        break;
    }

    case dspb::CastDateToTime: {

        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kDate) {
            return Status(Status::kTypeConflict,
                          "CastDateToDecimal type Conflict ",
                          lc->val->TypeString()
            );
        }

        auto dt_tmp = lc->val->Date();
        datatype::MyTime t;
        datatype::MyTime::StWarn st;
        bool ret_b = t.FromString(dt_tmp.ToNumberString(), st);
        if (!ret_b) {
            t.SetZero();
        }

        val.reset(new ScopedFV(new FieldValue(t), false));
        break;
    }

    case dspb::CastTimeToInt: {

        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kTime) {
            return Status(Status::kTypeConflict,
                          "CastDateToDecimal type Conflict ",
                          lc->val->TypeString()
            );
        }

        auto t_tmp = lc->val->Time();
        val.reset(new ScopedFV(new FieldValue(t_tmp.ToNumberInt64()), false));
        break;
    }

    case dspb::CastTimeToReal: {

        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kTime) {
            return Status(Status::kTypeConflict,
                          "CastDateToDecimal type Conflict ",
                          lc->val->TypeString()
            );
        }

        auto t_tmp = lc->val->Time();
        val.reset(new ScopedFV(new FieldValue(t_tmp.ToNumberFloat64()), false));
        break;
    }

    case dspb::CastTimeToString: {

        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kTime) {
            return Status(Status::kTypeConflict,
                          "CastTimeToString type Confflict ",
                          lc->val->TypeString()
            );
        }

        val.reset(new ScopedFV( new FieldValue(lc->val->Time().ToString()), false ));
        break;
    }

    case dspb::CastTimeToDecimal: {

        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kTime) {
            return Status(Status::kTypeConflict,
                          "CastTimeToDecimal type Conflict ",
                          lc->val->TypeString()
            );
        }

        auto t_tmp = lc->val->Time();
        datatype::MyDecimal dec;
        int32_t ret_tmp = dec.FromString(t_tmp.ToNumberString());
        if ( ret_tmp != 0) {
            dec.ResetZero();
        }

        val.reset(new ScopedFV(new FieldValue(dec), false));
        break;
    }

    case dspb::CastTimeToDate: {

        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kTime) {
            return Status(Status::kTypeConflict,
                          "CastTimeToDate type Conflict ",
                          lc->val->TypeString()
            );
        }

        auto t_tmp = lc->val->Time();
        auto now = std::chrono::system_clock::now();
        auto in_time_t = std::chrono::system_clock::to_time_t(now);
        char buf[100] = {0};
        std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", std::localtime(&in_time_t));

        datatype::MyDateTime dt;
        datatype::MyDateTime::StWarn st;
        bool ret_b = dt.FromString(std::string(buf), st);
        if (!ret_b) {
            dt.SetZero();
        } else {
            dt.SetHour(t_tmp.GetHour());
            dt.SetMinute(t_tmp.GetMinute());
            dt.SetSecond(t_tmp.GetSecond());
            dt.SetMicroSecond(t_tmp.GetMicroSecond());
        }

        val.reset(new ScopedFV(new FieldValue(dt), false));
        break;
    }

    case dspb::CastTimeToTime: {

        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kTime) {
            return Status(Status::kTypeConflict,
                          "CastTimeToTime type Conflict ",
                          lc->val->TypeString()
            );
        }

        auto t_tmp = lc->val->Time();
        val.reset(new ScopedFV(new FieldValue(t_tmp), false));
        break;
    }

    default:
        return Status(Status::kInvalidArgument, "expr type could not evaluate", std::to_string(expr.expr_type()));
    }

    return Status::OK();
}

Status compareExpr(FieldValue* left, FieldValue* right, dspb::ExprType cmp_type, bool& matched) {
    if (left == nullptr || right == nullptr) {
        matched = false;
        return Status::OK();
    }

    switch (cmp_type) {
    case dspb::Equal:
    case dspb::EqualInt:
    case dspb::EqualReal:
    case dspb::EqualDecimal:
    case dspb::EqualString:
    case dspb::EqualDate:
    case dspb::EqualTime:
        matched = fcompare(*left, *right, CompareOp::kEqual);
        break;
    case dspb::NotEqual:
    case dspb::NotEqualInt:
    case dspb::NotEqualDecimal:
    case dspb::NotEqualReal:
    case dspb::NotEqualString:
    case dspb::NotEqualDate:
    case dspb::NotEqualTime:
        matched = !fcompare(*left, *right, CompareOp::kEqual);
        break;
    case dspb::Less:
    case dspb::LessInt:
    case dspb::LessDecimal:
    case dspb::LessReal:
    case dspb::LessString:
    case dspb::LessDate:
    case dspb::LessTime:
        matched = fcompare(*left, *right, CompareOp::kLess);
        break;
    case dspb::LessOrEqual:
    case dspb::LessOrEqualInt:
    case dspb::LessOrEqualDecimal:
    case dspb::LessOrEqualReal:
    case dspb::LessOrEqualString:
    case dspb::LessOrEqualDate:
    case dspb::LessOrEqualTime:
        matched = fcompare(*left, *right, CompareOp::kLess) ||
                  fcompare(*left, *right, CompareOp::kEqual);
        break;
    case dspb::Larger:
    case dspb::GreaterInt:
    case dspb::GreaterDecimal:
    case dspb::GreaterReal:
    case dspb::GreaterString:
    case dspb::GreaterDate:
    case dspb::GreaterTime:
        matched = fcompare(*left, *right, CompareOp::kGreater);
        break;
    case dspb::LargerOrEqual:
    case dspb::GreaterOrEqualInt:
    case dspb::GreaterOrEqualDecimal:
    case dspb::GreaterOrEqualReal:
    case dspb::GreaterOrEqualString:
    case dspb::GreaterOrEqualDate:
    case dspb::GreaterOrEqualTime:
        matched = fcompare(*left, *right, CompareOp::kGreater) ||
                  fcompare(*left, *right, CompareOp::kEqual);
        break;
    default:
        return Status(Status::kInvalidArgument, "expr type could not compare", std::to_string(cmp_type));
    }
    
    return Status::OK();
}

Status filterExpr(const RowResult& row, const dspb::Expr& expr, bool& matched) {
    
    switch (expr.expr_type()) {
    case ::dspb::LogicAnd:
        if (expr.child_size() < 2) {
            return Status(Status::kInvalidArgument, "and expr child size", std::to_string(expr.child_size()));
        }
        for (const auto& child_expr: expr.child()) {
            auto s = filterExpr(row, child_expr, matched);
            if (!s.ok() || !matched) {
                return s;
            }
        }
        return Status::OK();

    case ::dspb::LogicOr:
        if (expr.child_size() < 2) {
            return Status(Status::kInvalidArgument, "or expr child size", std::to_string(expr.child_size()));
        }
        for (const auto& child_expr: expr.child()) {
            auto s = filterExpr(row, child_expr, matched);
            if (!s.ok() || matched) {
                return s;
            }
        }
        return Status::OK();

    case ::dspb::LogicNot: {
        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "not expr child size", std::to_string(expr.child_size()));
        }
        auto s = filterExpr(row, expr.child(0), matched);
        matched = !matched;
        return s;
    }

    case ::dspb::Equal:
    case ::dspb::EqualReal:
    case ::dspb::NotEqual:
    case ::dspb::NotEqualReal:
    case ::dspb::Less:
    case ::dspb::LessReal:
    case ::dspb::LessOrEqual:
    case ::dspb::LessOrEqualReal:
    case ::dspb::Larger:
    case ::dspb::LargerOrEqual:
    case ::dspb::GreaterReal:
    case ::dspb::GreaterOrEqualReal: {
        if (expr.child_size() != 2) {
            return Status(Status::kInvalidArgument, "compare expr child size", std::to_string(expr.child_size()));
        }
        ScopedFVPtr lc, rc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }
        s = evaluateExpr(row, expr.child(1), rc);
        if (!s.ok()) {
            return s;
        }
        if (lc == nullptr || rc == nullptr) {
            matched = false;
            return Status::OK();
        }
        return compareExpr(lc->val, rc->val, expr.expr_type(), matched);
    }

    case ::dspb::EqualInt:
    case ::dspb::NotEqualInt:
    case ::dspb::LessInt:
    case ::dspb::LessOrEqualInt:
    case ::dspb::GreaterInt:
    case ::dspb::GreaterOrEqualInt: {
        if (expr.child_size() != 2) {
            return Status(Status::kInvalidArgument, "compare expr child size", std::to_string(expr.child_size()));
        }
        ScopedFVPtr lc, rc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }
        s = evaluateExpr(row, expr.child(1), rc);
        if (!s.ok()) {
            return s;
        }
        if (lc == nullptr || rc == nullptr) {
            matched = false;
            return Status::OK();
        }

        if (
                !(((lc->val->Type() == FieldType::kInt) && (rc->val->Type() == FieldType::kInt)) ||
                ((lc->val->Type() == FieldType::kUInt ) && (rc->val->Type() == FieldType::kUInt)))
        ) {
            matched = false;
            return Status( Status::kTypeConflict, dspb::ExprType_Name(expr.expr_type()) + " compare type conflict ",
                    "lc type:[" + lc->val->TypeString() +"], rc type:[" + rc->val->TypeString() + "]" );
        }


        return compareExpr(lc->val, rc->val, expr.expr_type(), matched);

    }

    case ::dspb::EqualString:
    case ::dspb::NotEqualString:
    case ::dspb::LessString:
    case ::dspb::LessOrEqualString:
    case ::dspb::GreaterString:
    case ::dspb::GreaterOrEqualString: {
        if (expr.child_size() != 2) {
            return Status(Status::kInvalidArgument, "compare expr child size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc, rc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }
        s = evaluateExpr(row, expr.child(1), rc);
        if (!s.ok()) {
            return s;
        }

        if (lc == nullptr || rc == nullptr) {
            matched = false;
            return Status::OK();
        }

        if ((lc->val->Type() != FieldType::kBytes) ||
                (rc->val->Type() != FieldType::kBytes)
        ) {
            matched = false;
            return Status( Status::kTypeConflict,
                    " compare type conflict ",
                    "lc type:[" + lc->val->TypeString() +"], rc type:[" + rc->val->TypeString() + "]"
                    );
        }

        return compareExpr(lc->val, rc->val, expr.expr_type(), matched);
    }

    case ::dspb::EqualDecimal:
    case ::dspb::NotEqualDecimal:
    case ::dspb::LessDecimal:
    case ::dspb::LessOrEqualDecimal:
    case ::dspb::GreaterDecimal:
    case ::dspb::GreaterOrEqualDecimal: {
        if (expr.child_size() != 2) {
            return Status(Status::kInvalidArgument, "compare expr child size", std::to_string(expr.child_size()));
        }
        ScopedFVPtr lc, rc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }
        s = evaluateExpr(row, expr.child(1), rc);
        if (!s.ok()) {
            return s;
        }
        if (lc == nullptr || rc == nullptr) {
            matched = false;
            return Status::OK();
        }

        if ((lc->val->Type() != FieldType::kDecimal) ||
            (rc->val->Type() != FieldType::kDecimal)
                ) {
            matched = false;
            return Status( Status::kTypeConflict,
                           " compare type conflict ",
                           "lc type:[" + lc->val->TypeString() +"], rc type:[" + rc->val->TypeString() + "]"
            );
        }


        return compareExpr(lc->val, rc->val, expr.expr_type(), matched);
    }

    case ::dspb::EqualDate:
    case ::dspb::NotEqualDate:
    case ::dspb::LessDate:
    case ::dspb::LessOrEqualDate:
    case ::dspb::GreaterDate:
    case ::dspb::GreaterOrEqualDate: {

        if (expr.child_size() != 2) {
            return Status(Status::kInvalidArgument, "compare expr child size", std::to_string(expr.child_size()));
        }
        ScopedFVPtr lc, rc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }
        s = evaluateExpr(row, expr.child(1), rc);
        if (!s.ok()) {
            return s;
        }
        if (lc == nullptr || rc == nullptr) {
            matched = false;
            return Status::OK();
        }

        if ((lc->val->Type() != FieldType::kDate) ||
            (rc->val->Type() != FieldType::kDate)
                ) {
            matched = false;
            return Status( Status::kTypeConflict,
                           " compare type conflict ",
                           "lc type:[" + lc->val->TypeString() +"], rc type:[" + rc->val->TypeString() + "]"
            );
        }


        return compareExpr(lc->val, rc->val, expr.expr_type(), matched);
    }

    case ::dspb::EqualTime:
    case ::dspb::NotEqualTime:
    case ::dspb::LessTime:
    case ::dspb::LessOrEqualTime:
    case ::dspb::GreaterTime:
    case ::dspb::GreaterOrEqualTime: {

        if (expr.child_size() != 2) {
            return Status(Status::kInvalidArgument, "compare expr child size", std::to_string(expr.child_size()));
        }
        ScopedFVPtr lc, rc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }
        s = evaluateExpr(row, expr.child(1), rc);
        if (!s.ok()) {
            return s;
        }
        if (lc == nullptr || rc == nullptr) {
            matched = false;
            return Status::OK();
        }

        if ((lc->val->Type() != FieldType::kTime) ||
            (rc->val->Type() != FieldType::kTime)
                ) {
            matched = false;
            return Status( Status::kTypeConflict,
                           " compare type conflict ",
                           "lc type:[" + lc->val->TypeString() +"], rc type:[" + rc->val->TypeString() + "]"
            );
        }


        return compareExpr(lc->val, rc->val, expr.expr_type(), matched);
    }

    default:
        return Status(Status::kInvalidArgument, "not a boolean expr type", std::to_string(expr.expr_type()));
    }
}

// db value:
// | user_value | tag_value | tag_size(2 byte) | version(8 byte) |
//
Status assembleValue(std::string& db_value, const std::string& user_value,
                     uint64_t version, const pb::InternalTag* tag) {
    std::string tag_value;
    uint16_t tag_size = 0;
    if (tag != nullptr) {
        if (!tag->SerializeToString(&tag_value)) {
            return Status(Status::kCorruption, "serialize internal tag", "");
        }
        if (tag_value.size() > static_cast<size_t>(std::numeric_limits<uint16_t>::max())) {
            return Status(Status::kInvalidArgument, "tag too large", std::to_string(tag_value.size()));
        }
        tag_size = static_cast<uint16_t>(tag_value.size());
    }

    db_value.resize(user_value.size() + tag_size + sizeof(tag_size) + sizeof(version));
    auto p = const_cast<char*>(db_value.data());
    // append user value
    if (!user_value.empty()) {
        memcpy(p, user_value.c_str(), user_value.size());
        p += user_value.size();
    }
    // append tag value
    if (!tag_value.empty()) {
        memcpy(p, tag_value.c_str(), tag_size);
        p += tag_size;
    }
    // append tag length
    *((uint16_t *)p) = htobe16(tag_size);;
    p += sizeof(tag_size);
    // append version
    *((uint64_t *)p) = htobe64(version);;

    return Status(Status::OK());
}

Status disassembleValue(const std::string& db_value, size_t& user_value_size,
                        uint64_t& version, pb::InternalTag* tag) {
    uint16_t tag_size = 0;
    if (db_value.size() < sizeof(tag_size) + sizeof(version)) {
        return Status(Status::kInvalidArgument, "insufficient length", std::to_string(db_value.size()));
    }
    auto p = db_value.c_str() + db_value.size() - sizeof(version);
    version = be64toh(*((uint64_t *)p));
    p -= sizeof(tag_size);
    tag_size = be16toh(*((uint16_t *)p));

    user_value_size = db_value.size() - sizeof(version)- sizeof(tag_size) - tag_size;
    if (tag_size > 0 && tag != nullptr) {
        if (!tag->ParseFromArray(db_value.c_str() + user_value_size, tag_size)) {
            return Status(Status::kCorruption, "parse internal tag", EncodeToHexString(db_value));
        }
    }
    return Status::OK();
}

} /* namespace storage */
} /* namespace ds */
} /* namespace jimkv */
