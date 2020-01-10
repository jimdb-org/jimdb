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

#include <stdint.h>
#include <string>
#include <vector>
#include <common/data_type/my_decimal.h>
#include <common/data_type/my_timestamp.h>
#include <common/data_type/my_time.h>

#include "dspb/expr.pb.h"

namespace jimkv {
namespace ds {
namespace storage {

enum class FieldType : char {
    kInt,
    kUInt,
    kDouble,
    kBytes,
    kDecimal,
    kDate,
    kTime,
};

class FieldValue {
public:
    explicit FieldValue(int64_t val) : type_(FieldType::kInt) {
        value_.ival = val;
    }
    explicit FieldValue(uint64_t val) : type_(FieldType::kUInt) {
        value_.uval = val;
    }
    explicit FieldValue(double val) : type_(FieldType::kDouble) {
        value_.fval = val;
    }
    explicit FieldValue(std::string* val) : type_(FieldType::kBytes) {
        value_.sval = val;
    }
    explicit FieldValue(const std::string& val) : type_(FieldType::kBytes) {
        value_.sval = new std::string(val);
    }
    explicit FieldValue(std::string&& val) : type_(FieldType::kBytes) {
        value_.sval = new std::string(std::move(val));
    }

    explicit FieldValue( datatype::MyDecimal* dec ) : type_(FieldType::kDecimal) {
        value_.decval = dec;
    }

    explicit FieldValue( const datatype::MyDecimal& dec ) : type_(FieldType::kDecimal) {
        value_.decval = new datatype::MyDecimal(dec);
    }

    explicit FieldValue( datatype::MyDateTime* val ) : type_(FieldType::kDate) {
        value_.dateval= val;
    }

    explicit FieldValue( const datatype::MyDateTime& val) : type_(FieldType::kDate) {
        value_.dateval = new datatype::MyDateTime(val);
    }

    explicit FieldValue( datatype::MyTime* val ) : type_(FieldType::kTime) {
        value_.timeval= val;
    }

    explicit FieldValue( const datatype::MyTime& val) : type_(FieldType::kTime) {
        value_.timeval = new datatype::MyTime(val);
    }

    ~FieldValue() {
        if ( FieldType::kBytes == type_ ) {
            delete value_.sval;
        } else if (FieldType::kDecimal == type_ ) {
            delete value_.decval;
        } else if (FieldType::kDate == type_) {
            delete value_.dateval;
        } else if (FieldType::kTime == type_) {
            delete value_.timeval;
        }
    }

    FieldValue(const FieldValue&) = delete;
    FieldValue& operator=(const FieldValue&) = delete;

    FieldType Type() const { return type_; }

    const std::string TypeString() {
        std::string str;
        switch (type_) {
            case FieldType::kInt: {
                str = "kInt";
                break;
            }
            case FieldType::kUInt: {
                str = "kUInt";
                break;
            }
            case FieldType::kDouble: {
                str = "kDouble";
                break;
            }
            case FieldType::kBytes: {
                str = "kBytes";
                break;
            }
            case FieldType::kDecimal: {
                str = "kDecimal";
                break;
            }
            case FieldType::kDate: {
                str = "kDate";
                break;
            }
            case FieldType::kTime: {
                str = "kTime";
                break;
            }
            default: {
                str = "Type Error.";
            }
        }

        return str;
    }

    int64_t Int() const {
        if (type_ != FieldType::kInt) return 0;
        return value_.ival;
    }

    uint64_t UInt() const {
        if (type_ != FieldType::kUInt) return 0;
        return value_.uval;
    }

    double Double() const {
        if (type_ != FieldType::kDouble) return 0;
        return value_.fval;
    }

    const std::string& Bytes() const {
        if (type_ != FieldType::kBytes || value_.sval == nullptr)
            return kDefaultBytes;
        return *value_.sval;
    }

    const datatype::MyDecimal& Decimal() const {
        if (type_ != FieldType::kDecimal|| value_.decval == nullptr)
            return kDefaultDecimal;
        return *value_.decval;
    }

    const datatype::MyDateTime& Date() const {
        if (type_ != FieldType::kDate || value_.dateval == nullptr)
            return kDefaultDateTime;
        return *value_.dateval;
    }

    const datatype::MyTime& Time() const {
        if (type_ != FieldType::kTime || value_.dateval == nullptr)
            return kDefaultTime;
        return *value_.timeval;
    }

    std::string ToString() const {
        if (type_ == FieldType::kInt) {
            return std::to_string(value_.ival);
        } else if (type_ == FieldType::kUInt) {
            return std::to_string(value_.uval);
        } else if (type_ == FieldType::kDouble) {
            return std::to_string(value_.fval);
        } else if (type_ == FieldType::kBytes) {
            return *value_.sval;
        }else if (type_ == FieldType::kDecimal) {
            return value_.decval->ToString();
        }else if (type_ == FieldType::kDate) {
            return value_.dateval->ToString();
        }else if (type_ == FieldType::kTime) {
            return value_.timeval->ToString();
        } else {
            return "Unknown field value";
        }
    }

private:
    static const std::string kDefaultBytes;
    static const datatype::MyDecimal kDefaultDecimal;
    static const datatype::MyDateTime kDefaultDateTime;
    static const datatype::MyTime kDefaultTime;

    FieldType type_;
    union {
        int64_t ival;
        uint64_t uval;
        double fval;
        std::string* sval;
        datatype::MyDecimal* decval;
        datatype::MyDateTime* dateval;
        datatype::MyTime* timeval;
    } value_;
};

enum class CompareOp : char {
    kLess,
    kGreater,
    kEqual,
};

bool fcompare(const FieldValue& lh, const FieldValue& rh, CompareOp op);

FieldValue* CopyValue(const FieldValue& f);
void EncodeFieldValue(std::string* buf, FieldValue* v);
void EncodeFieldValue(std::string* buf, FieldValue* v, uint32_t col_id);

std::unique_ptr<FieldValue> arithCalc(const FieldValue* lh, const FieldValue* rh, dspb::ExprType type);

} /* namespace storage */
} /* namespace ds */
} /* namespace jimkv */
