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

#include "field_value.h"

#include <assert.h>
#include <math.h>
#include "common/ds_encoding.h"

namespace jimkv {
namespace ds {
namespace storage {

const std::string FieldValue::kDefaultBytes;
const datatype::MyDecimal FieldValue::kDefaultDecimal;
const datatype::MyDateTime FieldValue::kDefaultDateTime;
const datatype::MyTime FieldValue::kDefaultTime;

bool compareInt(const FieldValue& lh, const FieldValue& rh, CompareOp op) {
    switch (op) {
        case CompareOp::kLess:
            return lh.Int() < rh.Int();
        case CompareOp::kEqual:
            return lh.Int() == rh.Int();
        case CompareOp::kGreater:
            return lh.Int() > rh.Int();
        default:
            return false;
    }
}

bool compareUInt(const FieldValue& lh, const FieldValue& rh, CompareOp op) {
    switch (op) {
        case CompareOp::kLess:
            return lh.UInt() < rh.UInt();
        case CompareOp::kEqual:
            return lh.UInt() == rh.UInt();
        case CompareOp::kGreater:
            return lh.UInt() > rh.UInt();
        default:
            return false;
    }
}

bool compareDouble(const FieldValue& lh, const FieldValue& rh, CompareOp op) {
    switch (op) {
        case CompareOp::kLess:
            return lh.Double() < rh.Double();
        case CompareOp::kEqual:
            return lh.Double() == rh.Double();
        case CompareOp::kGreater:
            return lh.Double() > rh.Double();
        default:
            return false;
    }
}

bool compareBytes(const FieldValue& lh, const FieldValue& rh, CompareOp op) {
    switch (op) {
        case CompareOp::kLess:
            return lh.Bytes() < rh.Bytes();
        case CompareOp::kEqual:
            return lh.Bytes() == rh.Bytes();
        case CompareOp::kGreater:
            return lh.Bytes() > rh.Bytes();
        default:
            return false;
    }
}

bool compareDecimal(const FieldValue& lh, const FieldValue& rh, CompareOp op) {
    switch (op) {
        case CompareOp::kLess: {
            int32_t cmp = lh.Decimal().Compare( rh.Decimal());
            return cmp == -1;
        }
        case CompareOp::kEqual: {
            int32_t cmp = lh.Decimal().Compare( rh.Decimal());
            return cmp == 0;
        }
        case CompareOp::kGreater: {
            int32_t cmp = lh.Decimal().Compare( rh.Decimal());
            return cmp == 1;
        }
        default:
            return false;
    }
}

bool compareDate(const FieldValue& lh, const FieldValue& rh, CompareOp op) {
    switch (op) {
        case CompareOp::kLess:
            return lh.Date() < rh.Date();
        case CompareOp::kEqual:
            return lh.Date() == rh.Date();
        case CompareOp::kGreater:
            return lh.Date() > rh.Date();
        default:
            return false;
    }
}

bool compareTime(const FieldValue& lh, const FieldValue& rh, CompareOp op) {
    switch (op) {
        case CompareOp::kLess:
            return lh.Time() < rh.Time();
        case CompareOp::kEqual:
            return lh.Time() == rh.Time();
        case CompareOp::kGreater:
            return lh.Time() > rh.Time();
        default:
            return false;
    }
}

bool fcompare(const FieldValue& lh, const FieldValue& rh, CompareOp op) {
    if (lh.Type() != rh.Type()) {
        return false;
    }

    switch (lh.Type()) {
        case FieldType::kInt:
            return compareInt(lh, rh, op);
        case FieldType::kUInt:
            return compareUInt(lh, rh, op);
        case FieldType::kDouble:
            return compareDouble(lh, rh, op);
        case FieldType::kBytes:
            return compareBytes(lh, rh, op);
        case FieldType::kDecimal:
            return compareDecimal(lh, rh, op);
        case FieldType::kDate:
            return compareDate(lh, rh, op);
        case FieldType::kTime:
            return compareTime(lh, rh, op);
        default:
            return false;
    }
}

FieldValue* CopyValue(const FieldValue& v) {
    switch (v.Type()) {
        case FieldType::kInt:
            return new FieldValue(v.Int());
        case FieldType::kUInt:
            return new FieldValue(v.UInt());
        case FieldType::kDouble:
            return new FieldValue(v.Double());
        case FieldType::kBytes:
            return new FieldValue(v.Bytes());
        case FieldType::kDecimal:
            return new FieldValue(v.Decimal());
        case FieldType::kDate:
            return new FieldValue(v.Date());
        case FieldType::kTime:
            return new FieldValue(v.Time());
    }
    return nullptr;
}

void EncodeFieldValue(std::string* buf, FieldValue* v) {
    if (v == nullptr) {
        EncodeNullValue(buf, kNoColumnID);
        return;
    }

    switch (v->Type()) {
        case FieldType::kInt:
            EncodeIntValue(buf, kNoColumnID, v->Int());
            return;
        case FieldType::kUInt:
            EncodeIntValue(buf, kNoColumnID, static_cast<int64_t>(v->UInt()));
            break;
        case FieldType::kDouble:
            EncodeFloatValue(buf, kNoColumnID, v->Double());
            break;
        case FieldType::kBytes:
            EncodeBytesValue(buf, kNoColumnID, v->Bytes().c_str(), v->Bytes().size());
            break;
        case FieldType::kDecimal:
            EncodeDecimalValue(buf, kNoColumnID, &(v->Decimal()));
            break;
        case FieldType::kDate:
            EncodeDateValue(buf, kNoColumnID, &(v->Date()));
            break;
        case FieldType::kTime:
            EncodeTimeValue(buf, kNoColumnID, &(v->Time()));
            break;
    }
}

void EncodeFieldValue(std::string* buf, FieldValue* v, uint32_t col_id) {
    if (v == nullptr) {
        EncodeNullValue(buf, col_id);
        return;
    }

    switch (v->Type()) {
        case FieldType::kInt:
            EncodeIntValue(buf, col_id, v->Int());
            return;
        case FieldType::kUInt:
            EncodeIntValue(buf, col_id, static_cast<int64_t>(v->UInt()));
            break;
        case FieldType::kDouble:
            EncodeFloatValue(buf, col_id, v->Double());
            break;
        case FieldType::kBytes:
            EncodeBytesValue(buf, col_id, v->Bytes().c_str(), v->Bytes().size());
            break;
        case FieldType::kDecimal:
            EncodeDecimalValue(buf, col_id, &(v->Decimal()));
            break;
        case FieldType::kDate:
            EncodeDateValue(buf, col_id, &(v->Date()));
            break;
        case FieldType::kTime:
            EncodeTimeValue(buf, col_id, &(v->Time()));
            break;
    }
}

std::unique_ptr<FieldValue> arithCalc(const FieldValue* l, const FieldValue* r, dspb::ExprType type) {
    if (l == nullptr || r == nullptr || l->Type() != r->Type()) {
        return nullptr;
    }

    std::unique_ptr<FieldValue> result;
    switch (type) {
    case dspb::Plus:
    case dspb::PlusInt:
    case dspb::PlusReal:
    case dspb::PlusDecimal:
        switch (l->Type()) {
        case FieldType::kInt:
            result.reset(new FieldValue(l->Int() + r->Int()));
            break;
        case FieldType::kUInt:
            result.reset(new FieldValue(l->UInt() + r->UInt()));
            break;
        case FieldType::kDouble:
            if (type == dspb::PlusInt) {
                result.reset(new FieldValue(int64_t(l->Double() + r->Double())));
            } else {
                result.reset(new FieldValue(l->Double() + r->Double()));
            }
            break;
        case FieldType::kDecimal:{

            datatype::MyDecimal ret;
            int32_t error = 0;

            error = datatype::DecimalAdd(&ret, &(l->Decimal()), &(r->Decimal()));
            if ( error == datatype::E_DEC_OK || error == datatype::E_DEC_OVERFLOW) {
                result.reset( new FieldValue(ret));
            } else {
                result.reset(nullptr);
            }

            break;
        }
        default:
            break;
        }
        break;
    case dspb::Minus:
    case dspb::MinusInt:
    case dspb::MinusReal:
    case dspb::MinusDecimal:
        switch (l->Type()) {
        case FieldType::kInt:
            result.reset(new FieldValue(l->Int() - r->Int()));
            break;
        case FieldType::kUInt:
            result.reset(new FieldValue(l->UInt() - r->UInt()));
            break;
        case FieldType::kDouble:
            if (type == dspb::MinusInt) {
                result.reset(new FieldValue(int64_t(l->Double() - r->Double())));
            } else {
                result.reset(new FieldValue(l->Double() - r->Double()));
            }
            break;
        case FieldType::kDecimal:{

            datatype::MyDecimal ret;
            int32_t error = 0;

            error = datatype::DecimalSub(&ret, &(l->Decimal()), &(r->Decimal()));

            if ( error == datatype::E_DEC_OK || error == datatype::E_DEC_OVERFLOW) {
                result.reset( new FieldValue(ret));
            } else {
                result.reset(nullptr);
            }

            break;
        }
        default:
            break;
        }
        break;
    case dspb::Mult:
    case dspb::MultInt:
    case dspb::MultReal:
    case dspb::MultDecimal:
        switch (l->Type()) {
        case FieldType::kInt:
            result.reset(new FieldValue(l->Int() * r->Int()));
            break;
        case FieldType::kUInt:
            result.reset(new FieldValue(l->UInt() * r->UInt()));
            break;
        case FieldType::kDouble:
            if (type == dspb::MultInt) {
                result.reset(new FieldValue(int64_t(l->Double() * r->Double())));
            } else {
                result.reset(new FieldValue(l->Double() * r->Double()));
            }
            break;
        case FieldType::kDecimal: {
            datatype::MyDecimal ret;
            int32_t error = 0;

            error = datatype::DecimalMul(&ret, &(l->Decimal()), &(r->Decimal()));

            if ( error == datatype::E_DEC_OK || error == datatype::E_DEC_OVERFLOW) {
                result.reset( new FieldValue(ret));
            } else {
                result.reset(nullptr);
            }

            break;
        }
        default:
            break;
        }
        break;

    case dspb::Div:
    case dspb::DivReal:
    case dspb::IntDivInt:
    case dspb::DivDecimal:
        switch (l->Type()) {
        case FieldType::kInt:
            if (r->Int() != 0) {
                result.reset(new FieldValue(l->Int() / r->Int()));
            } else {
                result.reset(nullptr);
            }
            break;
        case FieldType::kUInt:
            if (r->UInt() != 0) {
                result.reset(new FieldValue(l->UInt() / r->UInt()));
            } else {
                result.reset(nullptr);
            }
            break;
        case FieldType::kDouble:
            if (r->Double() != 0) {
                if (type == dspb::IntDivInt) {
                    result.reset(new FieldValue(int64_t(l->Double() / r->Double())));
                } else {
                    result.reset(new FieldValue(l->Double() / r->Double()));
                }
            } else {
                result.reset(nullptr);
            }
            break;
        case FieldType::kDecimal: {
            datatype::MyDecimal ret;
            int32_t error = 0;

            error = datatype::DecimalDiv(&ret, &(l->Decimal()), &(r->Decimal()), 5);

            if ( error == datatype::E_DEC_OK || error == datatype::E_DEC_OVERFLOW) {
                result.reset( new FieldValue(ret));
            } else {
                result.reset(nullptr);
            }

            break;
        }
        default:
            break;
        }
        break;
    case dspb::Mod:
    case dspb::ModInt:
    case dspb::ModReal:
    case dspb::ModDecimal:
        switch (l->Type()) {
        case FieldType::kInt:
            result.reset(new FieldValue(l->Int() % r->Int()));
            break;
        case FieldType::kUInt:
            result.reset(new FieldValue(l->UInt() % r->UInt()));
            break;
        case FieldType::kDouble:
            result.reset(new FieldValue(fmod(l->Double(), r->Double())));
            break;
        case FieldType::kDecimal: {
            datatype::MyDecimal ret;
            int32_t error = 0;

            error = datatype::DecimalMod(&ret, &(l->Decimal()), &(r->Decimal()));

            if ( error == datatype::E_DEC_OK || error == datatype::E_DEC_OVERFLOW) {
                result.reset( new FieldValue(ret));
            } else {
                result.reset(nullptr);
            }

            break;
        }
        default:
            break;
        }
        break;
    default:
        return result;
    }
    return result;
}

} /* namespace storage */
} /* namespace ds */
} /* namespace jimkv */
