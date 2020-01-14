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

#include "field_value.h"
#include "basepb/basepb.pb.h"

namespace jim {
namespace ds {
namespace storage {

class AggreCalculator {
public:
    AggreCalculator(const u_int64_t id) : col_id_(id) {}
    virtual ~AggreCalculator() {}

    virtual void Add(const FieldValue* f) = 0;

    int64_t Count() const {return count_;}

    virtual std::unique_ptr<FieldValue> Result() = 0;

    static std::unique_ptr<AggreCalculator> New(const std::string& name,
                                                const u_int64_t id);
    static std::unique_ptr<AggreCalculator> New(dspb::ExprType type,
                                                const u_int64_t id);

    static std::unique_ptr<AggreCalculator> New(const dspb::Expr &f);
    
    u_int64_t GetColumnId() { return col_id_;}

    bool isAvg() {return bAvg_;}

protected:
    const u_int64_t col_id_;
    int64_t count_ = 0;
    bool bAvg_ = false;

    static const u_int64_t default_const_col_id = 0;
};

class CountCalculator : public AggreCalculator {
public:
    CountCalculator(const u_int64_t id);
    ~CountCalculator();

    void Add(const FieldValue* f) override;
    std::unique_ptr<FieldValue> Result() override;
};

class MinCalculator : public AggreCalculator {
public:
    MinCalculator(const u_int64_t id);
    ~MinCalculator();

    void Add(const FieldValue* f) override;
    std::unique_ptr<FieldValue> Result() override;

private:    
    std::unique_ptr<FieldValue> min_value_;
};

class MaxCalculator : public AggreCalculator {
public:
    MaxCalculator(const u_int64_t id);
    ~MaxCalculator();

    void Add(const FieldValue* f) override;
    std::unique_ptr<FieldValue> Result() override;

private:    
    std::unique_ptr<FieldValue> max_value_;
};

class FristCalculator : public AggreCalculator {
public:
    FristCalculator(const u_int64_t id);
    ~FristCalculator();

    void Add(const FieldValue* f) override;
    std::unique_ptr<FieldValue> Result() override;

private:    
    std::unique_ptr<FieldValue> value_;
};

class SumCalculator : public AggreCalculator {
public:
    SumCalculator(const u_int64_t id);
    ~SumCalculator();

    void Add(const FieldValue* f) override;
    std::unique_ptr<FieldValue> Result() override;

protected:
    union {
        int64_t ival;
        uint64_t uval;
        double fval;
    } sum_;
    FieldType type_ = FieldType::kBytes;
};

class AvgCalculator : public SumCalculator {
public:
    AvgCalculator(const u_int64_t id) : SumCalculator(id){ bAvg_ = true;}

    virtual std::unique_ptr<FieldValue> Result();
};

} /* namespace storage */
} /* namespace ds */
} /* namespace jim */
