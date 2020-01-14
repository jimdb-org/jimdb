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

#include "dspb/txn.pb.h"

#include "table.h"

namespace jim {
namespace test {
namespace helper {

class SelectRequestBuilder {
public:
    explicit SelectRequestBuilder(Table *t);

    // select one row
    void SetKey(const std::vector<std::string>& all_pk_values);

    // select multi rows
    void SetScope(const std::vector<std::string>& start_pk_values,
            const std::vector<std::string>& end_pk_values);

    void SetSQL(const std::string& query);

    dspb::SelectRequest Build() { return std::move(req_); }

private:
    Table *table_ = nullptr;
    dspb::SelectRequest req_;
};

class SelectFlowRequestBuilder {
public:
    explicit SelectFlowRequestBuilder(Table *t) : table_(t) {}

    void SetSQL(const std::string& query);

    dspb::SelectFlowRequest Build() { return std::move(req_); }

private:
    Table *table_ = nullptr;
    dspb::SelectFlowRequest req_;
};

class LocalPrepareBuilder {
public:
    explicit LocalPrepareBuilder(Table *t);

    void AddRow(const std::vector<std::string>& values, dspb::OpType type);

    dspb::PrepareRequest Build() { return std::move(req_); }

protected:
    Table *table_ = nullptr;
    std::vector<basepb::ColumnInfo> pk_columns_;
    std::vector<basepb::ColumnInfo> non_pk_columns_;
    dspb::PrepareRequest req_;
};

class DeleteRequestBuilder : public LocalPrepareBuilder {
public:
    explicit DeleteRequestBuilder(Table *t);
    void AddRow(const std::vector<std::string>& values);
    void AddRows(const std::vector<std::vector<std::string>>& rows);
};

class InsertRequestBuilder : public LocalPrepareBuilder {
public:
    explicit InsertRequestBuilder(Table *t);
    void AddRow(const std::vector<std::string>& values);
    void AddRows(const std::vector<std::vector<std::string>>& rows);
    void SetCheckUnique();
};

} /* namespace helper */
} /* namespace test */
} /* namespace jim */
