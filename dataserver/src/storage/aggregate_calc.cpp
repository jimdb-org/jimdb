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
#include "common/logger.h"
#include "base/status.h"
#include "aggregate_calc.h"
#include "field_value.h"

namespace jim {
namespace ds {
namespace storage {

Status checkFunc(const dspb::Expr &f) {
    Status s;
    if (f.child_size() == 1) {
        if (f.child(0).expr_type() != dspb::Column) {
            s = Status( Status::kNotSupported,
                        "func column type error",
                        "expr_type:" + std::to_string(f.child(0).expr_type()));
        }
    } else {
        s = Status( Status::kNotSupported,
                        "func column is not one",
                        "column size:" + std::to_string(f.child_size()));
    }
    return s;
}

std::unique_ptr<AggreCalculator> AggreCalculator::New(const dspb::Expr &f) {
    if (f.expr_type() == dspb::Count) {
        if (f.child_size() == 1) {
            switch (f.child(0).expr_type())
            {
            case dspb::Column:
                return std::unique_ptr<AggreCalculator>(new CountCalculator(f.child(0).column().id()));
            case dspb::Const_Int:
                return std::unique_ptr<AggreCalculator>(new CountCalculator(AggreCalculator::default_const_col_id));
            default:
                // never go here
                return nullptr;
            }
        } else {
            // count(*) or count(1) ...
            return std::unique_ptr<AggreCalculator>(new CountCalculator(AggreCalculator::default_const_col_id));
        }
    } else {
        Status s = checkFunc(f);
        if (!s.ok()) {
            FLOG_ERROR("AggreCalculator checkFunc: {}", s.ToString());
            return nullptr;
        }
        switch (f.expr_type())
        {
        case dspb::Avg:
            return std::unique_ptr<AggreCalculator>(new AvgCalculator(f.child(0).column().id()));
        case dspb::Max:
            return std::unique_ptr<AggreCalculator>(new MaxCalculator(f.child(0).column().id()));
        case dspb::Min:
            return std::unique_ptr<AggreCalculator>(new MinCalculator(f.child(0).column().id()));
        case dspb::Sum:
                return std::unique_ptr<AggreCalculator>(new SumCalculator(f.child(0).column().id()));
        case dspb::Distinct:
                return std::unique_ptr<AggreCalculator>(new FristCalculator(f.child(0).column().id()));
        default:
            return nullptr;
        }
    }
    
}

CountCalculator::CountCalculator(const u_int64_t id) : AggreCalculator(id) {}
CountCalculator::~CountCalculator() {}

void CountCalculator::Add(const FieldValue* f) {
   if (f != nullptr || col_id_ == AggreCalculator::default_const_col_id) {
        ++count_;
   }
}

std::unique_ptr<FieldValue> CountCalculator::Result() {
    return std::unique_ptr<FieldValue>(new FieldValue(count_));
}

// min
MinCalculator::MinCalculator(const u_int64_t id) : AggreCalculator(id) {}
MinCalculator::~MinCalculator() {}

void MinCalculator::Add(const FieldValue* f) {
    if (f != nullptr) {
        if (min_value_ == nullptr ||
            fcompare(*f, *min_value_.get(), CompareOp::kLess)) {
            min_value_.reset(CopyValue(*f));
        }
    }
}

std::unique_ptr<FieldValue> MinCalculator::Result() {
    return std::move(min_value_);
}

//
// max
MaxCalculator::MaxCalculator(const u_int64_t id) : AggreCalculator(id) {}
MaxCalculator::~MaxCalculator() {}

void MaxCalculator::Add(const FieldValue* f) {
    if (f != nullptr) {
        if (max_value_ == nullptr ||
            fcompare(*f, *max_value_.get(), CompareOp::kGreater)) {
            max_value_.reset(CopyValue(*f));
        }
    }
}

std::unique_ptr<FieldValue> MaxCalculator::Result() {
    return std::move(max_value_);
}

//FristCalculator
FristCalculator::FristCalculator(const u_int64_t id) : AggreCalculator(id) {}
FristCalculator::~FristCalculator() {}

void FristCalculator::Add(const FieldValue* f) {
    if (value_ == nullptr && f != nullptr) {
        value_.reset(CopyValue(*f));
    }
}

std::unique_ptr<FieldValue> FristCalculator::Result() {
    return std::move(value_);
}

// sum

SumCalculator::SumCalculator(const u_int64_t id) : AggreCalculator(id) {}
SumCalculator::~SumCalculator() {}

void SumCalculator::Add(const FieldValue* f) {
    if (f == nullptr) return;
    
    if (count_ == 0) {
        type_ = f->Type();
        switch (type_) {
            case FieldType::kDouble:
                sum_.fval = f->Double();
                break;
            case FieldType::kInt:
                sum_.ival = f->Int();
                break;
            case FieldType::kUInt:
                sum_.uval = f->UInt();
                break;
            default:
                return;
        }
    } else {
        if (type_ != f->Type()) return;
        switch (type_) {
            case FieldType::kDouble:
                sum_.fval += f->Double();
                break;
            case FieldType::kInt:
                sum_.ival += f->Int();
                break;
            case FieldType::kUInt:
                sum_.uval += f->UInt();
                break;
            default:
                return;
        }
    }
    ++count_;
}

std::unique_ptr<FieldValue> SumCalculator::Result() {
    switch (type_) {
        case FieldType::kDouble:
            return std::unique_ptr<FieldValue>(new FieldValue(sum_.fval));
        case FieldType::kInt:
            return std::unique_ptr<FieldValue>(new FieldValue(sum_.ival));
        case FieldType::kUInt:
            return std::unique_ptr<FieldValue>(new FieldValue(sum_.uval));
        default:
            return nullptr;
    }
}

std::unique_ptr<FieldValue> AvgCalculator::Result() {
    if (count_ == 0) return nullptr;

    switch (type_) {
        case FieldType::kDouble:
            return std::unique_ptr<FieldValue>(new FieldValue(sum_.fval));
        case FieldType::kInt:
            return std::unique_ptr<FieldValue>(new FieldValue(sum_.ival));
        case FieldType::kUInt:
            return std::unique_ptr<FieldValue>(new FieldValue(sum_.uval));
        default:
            return nullptr;
    }
}

} /* namespace storage */
} /* namespace ds */
} /* namespace jim */
