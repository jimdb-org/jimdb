// Copyright 2017 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Portions Copyright 2019 The JIMDB Authors.
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
#include <limits>
#include <common/data_type/my_decimal.h>
#include <common/data_type/my_timestamp.h>
#include <common/data_type/my_time.h>

// from https://github.com/cockroachdb/cockroach

namespace jim {

enum class EncodeType : char {
    Unknown = 0,
    Null,
    NotNull,
    Int,
    Float,
    Decimal,
    Bytes,
    BytesDesc,  // Bytes encoded descendingly
    Date,
    Time,

    SentinelType = 15  // Used in the Value encoding.
};

// Reserved column id for internal use
static const uint32_t kNoColumnID = 0;

void EncodeNonSortingUvarint(std::string* buf, uint64_t value);
size_t NonSortingUvarintSize(uint64_t value);
void EncodeNonSortingVarint(std::string* buf, int64_t value);
size_t NonSortingVarintSize(int64_t value);
bool DecodeNonSortingUvarint(const std::string& data, size_t& offset, uint64_t* value);
bool DecodeNonSortingVarint(const std::string& data, size_t& offset, int64_t* value);

void EncodeUint64Ascending(std::string* buf, uint64_t value);
bool DecodeUint64Ascending(const std::string& data, size_t& offset, uint64_t* value);

void EncodeUint32Ascending(std::string* buf, uint32_t value);
bool DecodeUint32Ascending(const std::string& data, size_t& offset, uint32_t* value);

void EncodeIntValue(std::string* buf, uint32_t col_id, int64_t value);
size_t IntValueSize(uint32_t col_id, int64_t value);
void EncodeFloatValue(std::string* buf, uint32_t col_id, double value);
void EncodeBytesValue(std::string* buf, uint32_t col_id, const char* value, size_t value_size);
void EncodeDecimalValue(std::string* buf, uint32_t col_id, const datatype::MyDecimal* d);
void EncodeDateValue(std::string* buf, uint32_t col_id, const datatype::MyDateTime* dt);
void EncodeTimeValue(std::string* buf, uint32_t col_id, const datatype::MyTime* t);
void EncodeNullValue(std::string* buf, uint32_t col_id);

bool DecodeValueTag(const std::string& data, size_t& offset, uint32_t* col_id, EncodeType* type);
bool DecodeIntValueAfterTag(const std::string& data, size_t& offset, int64_t* value);
bool DecodeFloatValueAfterTag(const std::string& data, size_t& offset, double* value);
bool DecodeBytesValueAfterTag(const std::string& data, size_t& offset, std::string* value);
bool DecodeDecimalValueAfterTag(const std::string& data, size_t& offset, datatype::MyDecimal* d);
bool DecodeDateValueAfterTag(const std::string& data, size_t& offset, datatype::MyDateTime* dt);
bool DecodeTimeValueAfterTag(const std::string& data, size_t& offset, datatype::MyTime* t);
bool SkipValueAfterTag(const std::string& data, size_t& offset, const EncodeType &type);

bool DecodeIntValue(const std::string& data, size_t& offset, int64_t* value);
bool DecodeFloatValue(const std::string& data, size_t& offset, double* value);
bool DecodeBytesValue(const std::string& data, size_t& offset, std::string* value);
bool DecodeDecimalValue(const std::string& data, size_t& offset, datatype::MyDecimal* d);
bool DecodeDateValue(const std::string& data, size_t& offset, datatype::MyDateTime* dt);
bool DecodeTimeValue(const std::string& data, size_t& offset, datatype::MyTime* t);
bool SkipValue(const std::string& data, size_t& offset);

void EncodeNullAscending(std::string* buf);
void EncodeUvarintAscending(std::string* buf, uint64_t value);
void EncodeUvarintDescending(std::string* buf, uint64_t value);
void EncodeVarintAscending(std::string* buf, int64_t value);
void EncodeFloatAscending(std::string* buf, double value);
void EncodeBytesAscending(std::string* buf, const char* value, size_t value_size);
void EncodeDecimalAscending(std::string* buf, const datatype::MyDecimal* d);
void EncodeDateAscending(std::string* buf, const datatype::MyDateTime* dt);
void EncodeTimeAscending(std::string* buf, const datatype::MyTime* t);

bool DecodeUvarintAscending(const std::string& buf, size_t& pos, uint64_t* out);
bool DecodeUvarintDescending(const std::string& buf, size_t& pos, uint64_t* out);
bool DecodeVarintAscending(const std::string& buf, size_t& pos, int64_t* out);
bool DecodeFloatAscending(const std::string& buf, size_t& pos, double* out);
bool DecodeBytesAscending(const std::string& buf, size_t& pos, std::string* out);
bool DecodeDecimalAscending(const std::string& buf, size_t& pos, datatype::MyDecimal* d);
bool DecodeDateAscending(const std::string& buf, size_t& pos, datatype::MyDateTime* dt);
bool DecodeTimeAscending(const std::string& buf, size_t& pos, datatype::MyTime* t);

// for tests or debug
std::string EncodeToHexString(const std::string& str);

} /* namespace jim */
