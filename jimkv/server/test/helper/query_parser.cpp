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

#include "query_parser.h"

#include <sstream>
#include <common/ds_encoding.h>
#include "helper_util.h"

namespace jimkv {
namespace test {
namespace helper {

SelectResultParser::SelectResultParser(const dspb::SelectRequest& req,
                                       const dspb::SelectResponse& resp) {
    dspb::ColumnInfo fake_count_col;
    fake_count_col.set_typ(basepb::BigInt);
    dspb::Row tmp1;

    rows_.reserve(resp.rows_size());
    for (const auto& r: resp.rows()) {
        keys_.push_back(r.key());

        std::vector<std::string> values;
        size_t offset = 0;
        for (const auto &f: req.field_list()) {
            std::string val;
            if (f.typ() == dspb::SelectField_Type_Column) {
                DecodeValueColumn(r.value().fields(), offset, f.column(), &val);
            } else {
                if (f.aggre_func() == "count") {
                    DecodeValueColumn(r.value().fields(), offset, fake_count_col, &val);
                } else {
                    DecodeValueColumn(r.value().fields(), offset, f.column(), &val);
                }
            }
            values.push_back(std::move(val));
        }

        rows_.push_back(std::move(values));
    }
}

static std::string ToDebugString(const std::vector<std::vector<std::string>>& rows) {
    std::ostringstream ss;
    ss << "[\n" ;
    for (const auto& row: rows) {
        ss << " { ";
        for (size_t i = 0; i < row.size(); ++i) {
            ss << row[i];
            if (i != row.size() - 1) {
                ss << ", ";
            }
        }
        ss << " }\n";
    }
    ss << "]" ;
    auto s = ss.str();
    return s;
}

Status SelectResultParser::Match(
        const std::vector<std::vector<std::string>>& expected_rows) const {
    bool matched = true;
    do {
        if (expected_rows.size() != rows_.size()) {
            matched = false;
            break;
        }

        for (size_t i = 0; i < rows_.size(); ++i) {
            if (rows_[i] != expected_rows[i]) {
                matched = false;
                break;
            }
        }
    } while(0);

    if (matched) {
        return Status::OK();
    } else {
        std::ostringstream ss;
        ss << "\nexpected: \n";
        ss <<  ToDebugString(expected_rows);
        ss << "\nactual: \n";
        ss << ToDebugString(rows_);
        return Status(Status::kUnknown, "mismatch", ss.str());
    }
}

SelectFlowResultParser::SelectFlowResultParser(const dspb::SelectFlowRequest& req,
                                       const dspb::SelectFlowResponse& resp) {
    dspb::ColumnInfo fake_count_col;
    fake_count_col.set_typ(basepb::BigInt);
    dspb::Row tmp1;

    rows_.reserve(resp.rows_size());
    auto processors = req.processors();
    auto processor_0 = processors[0];
    int index_agg = -1;
    for (int i = 1; i < processors.size(); ++i) {
        if (processors[i].type() == dspb::AGGREGATION_TYPE) {
            index_agg = i;
        }
    }
    for (const auto& r: resp.rows()) {
        keys_.push_back(r.key());
       
        std::vector<std::string> values;
        size_t offset = 0;

        if (processor_0.type() == dspb::TABLE_READ_TYPE) {
            if (index_agg != -1) {
                int64_t value = 0;
                for (auto &func : processors[index_agg].aggregation().func()) {
                    if (func.expr_type() == dspb::Avg) {
                        int64_t sum = 0;
                        int64_t count = 0;
                        DecodeNonSortingVarint(r.value().fields(), ++offset, &sum);
                        DecodeNonSortingVarint(r.value().fields(), ++offset, &count);
                        if (count > 0) {
                            value = sum / count;
                        }
                    } else {
                        DecodeNonSortingVarint(r.value().fields(), ++offset, &value);
                    }
                    values.push_back(std::to_string(value));
                }
            } else {
                for (auto &col : processor_0.table_read().columns()) {
                    std::string val;
                    DecodeValueColumn(r.value().fields(), offset, col, &val);
                    values.push_back(std::move(val));
                }
            }
        }

        rows_.push_back(std::move(values));
    }
}

Status SelectFlowResultParser::Match(
        const std::vector<std::vector<std::string>>& expected_rows) const {
    bool matched = true;
    do {
        if (expected_rows.size() != rows_.size()) {
            matched = false;
            break;
        }

        for (size_t i = 0; i < rows_.size(); ++i) {
            if (rows_[i] != expected_rows[i]) {
                matched = false;
                break;
            }
        }
    } while(0);

    if (matched) {
        return Status::OK();
    } else {
        std::ostringstream ss;
        ss << "\nexpected: \n";
        ss <<  ToDebugString(expected_rows);
        ss << "\nactual: \n";
        ss << ToDebugString(rows_);
        return Status(Status::kUnknown, "mismatch", ss.str());
    }
}

} /* namespace helper */
} /* namespace test */
} /* namespace jimkv */
