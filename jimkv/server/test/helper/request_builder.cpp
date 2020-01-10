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

#include "request_builder.h"

#include <sstream>

#include "base/util.h"
#include "helper_util.h"
#include "storage/util.h"

#include "sql_parser.h"
#include "util/sqlhelper.h"

namespace jimkv {
namespace test {
namespace helper {

using jimkv::randomInt;
using namespace jimkv::ds::storage;

static std::string buildKey(Table *table, const std::vector<std::string>& values) {
    auto pks = table->GetPKs();
    if (values.size() != pks.size()) {
        throw std::runtime_error("mismatched table primary keys count and input values count");
    }

    std::string buf;
    EncodeKeyPrefix(&buf, table->GetID());
    for (size_t i = 0; i < pks.size(); ++i) {
        EncodeKvKey(&buf, pks[i], values[i]);
    }
    return buf;
}

static std::pair<std::string, std::string> buildScope(Table *table,
                                                      const std::vector<std::string>& start_values,
                                                      const std::vector<std::string>& end_values) {
    auto pks = table->GetPKs();
    std::string start, end;

    EncodeKeyPrefix(&start, table->GetID());
    auto len = std::min(pks.size(), start_values.size());
    for (size_t i = 0; i < len; ++i) {
        EncodeKvKey(&start, pks[i], start_values[i]);
    }

    if (end_values.empty()) {
        EncodeKeyPrefix(&end, table->GetID() + 1);
    } else {
        EncodeKeyPrefix(&end, table->GetID());
        auto len = std::min(pks.size(), end_values.size());
        for (size_t i = 0; i < len; ++i) {
            EncodeKvKey(&end, pks[i], end_values[i]);
        }
    }

    return std::make_pair(start, end);
}


SelectRequestBuilder::SelectRequestBuilder(Table *t) : table_(t) {
    // default select all scope
    SetScope({}, {});
}

void SelectRequestBuilder::SetKey(const std::vector<std::string>& all_pk_values) {
    req_.set_key(buildKey(table_, all_pk_values));
}

void SelectRequestBuilder::SetScope(const std::vector<std::string>& start_pk_values,
              const std::vector<std::string>& end_pk_values) {
    auto ret = buildScope(table_, start_pk_values, end_pk_values);
    req_.mutable_scope()->mutable_start()->assign(ret.first);
    req_.mutable_scope()->mutable_limit()->assign(ret.second);
}


void SelectRequestBuilder::SetSQL(const std::string& query) {
    hsql::SQLParserResult result;
    auto ret = hsql::SQLParser::parse(query, &result);
    if (!ret || !result.isValid() || result.size() != 1) {
        std::ostringstream ss;
        ss << "parse sql [" << query << "] failed: " << result.errorMsg() << std::endl;
        ss << "column: " << result.errorColumn()  << query[result.errorColumn()]<< std::endl;
        throw std::runtime_error(ss.str());
    }
    auto statement = result.getStatement(0);
    if (!statement->isType(hsql::kStmtSelect)) {
        throw std::runtime_error(std::string("not select sql: ") + query);
    }
    auto select = (const hsql::SelectStatement*)statement;

    try {
        // field list
        parseFieldList(*table_, select, &req_);

        // where
        if (select->whereClause != nullptr) {
            parseExpr(*table_, select->whereClause, req_.mutable_where_expr());
        }
        // limit
        if (select->limit != nullptr) {
            parseLimit(select->limit, req_.mutable_limit());
        }
    } catch (std::exception& e) {
        hsql::printStatementInfo(statement);
        std::ostringstream ss;
        ss << "parse sql [" << query << "] failed: " << e.what() << std::endl;
        throw std::runtime_error(ss.str());
    }
}

void SelectFlowRequestBuilder::SetSQL(const std::string& query) {
    hsql::SQLParserResult result;
    auto ret = hsql::SQLParser::parse(query, &result);
    if (!ret || !result.isValid() || result.size() != 1) {
        std::ostringstream ss;
        ss << "parse sql [" << query << "] failed: " << result.errorMsg() << std::endl;
        ss << "column: " << result.errorColumn()  << query[result.errorColumn()]<< std::endl;
        throw std::runtime_error(ss.str());
    }
    auto statement = result.getStatement(0);
    if (!statement->isType(hsql::kStmtSelect)) {
        throw std::runtime_error(std::string("not select sql: ") + query);
    }
    auto select = (const hsql::SelectStatement*)statement;

    try {
        std::cout << "parseFieldList:" << query << std::endl;
        // field list
        parseFieldList(*table_, select, &req_);

        // where
        if (select->whereClause != nullptr) {
            parseExpr(*table_, select->whereClause, &req_);
        }
        // order by
        if (select->order != nullptr) {
            parseOrder(*table_, select->order, &req_);
        }

        // group by todo
        
        // limit
        if (select->limit != nullptr) {
            parseLimit(select->limit, &req_);
        }
    } catch (std::exception& e) {
        hsql::printStatementInfo(statement);
        std::ostringstream ss;
        ss << "parse sql [" << query << "] failed: " << e.what() << std::endl;
        throw std::runtime_error(ss.str());
    }
}

LocalPrepareBuilder::LocalPrepareBuilder(Table *t) : table_(t) {
    pk_columns_ = t->GetPKs();
    non_pk_columns_ = t->GetNonPkColumns();

    req_.set_txn_id(randomString(20));
    req_.set_local(true);
    req_.set_lock_ttl(1000);
}

void LocalPrepareBuilder::AddRow(const std::vector<std::string>& values, dspb::OpType type) {
    switch (type) {
    case dspb::INSERT:
        if (values.size() != pk_columns_.size() + non_pk_columns_.size()) {
            throw std::runtime_error("mismatched row values size with table columns size");
        }
        break;
    case dspb::DELETE:
        if (values.size() < pk_columns_.size()) {
            throw std::runtime_error("mismatched row values size with table pk columns size");
        }
        break;
    default:
        throw std::runtime_error("invalid intent op type");
    }

    std::string key;
    std::string value;
    size_t index = 0;

    // encode key
    EncodeKeyPrefix(&key, table_->GetID());
    for (const auto &pk : pk_columns_) {
        EncodeKvKey(&key, pk, values[index++]);
    }

    // encode value
    if (type == dspb::INSERT) {
        for (const auto &col : non_pk_columns_) {
            EncodeKvValue(&value, col, values[index++]);
        }
    }

    // fill intent
    bool is_primary = false;
    if (req_.primary_key().empty()) {
        req_.set_primary_key(key);
        is_primary = true;
    }
    auto intent = req_.add_intents();
    intent->set_typ(type);
    intent->set_key(std::move(key));
    if (!value.empty()) {
        intent->set_value(std::move(value));
    }
    intent->set_is_primary(is_primary);
}

DeleteRequestBuilder::DeleteRequestBuilder(Table *t) : LocalPrepareBuilder(t) {
}

void DeleteRequestBuilder::DeleteRequestBuilder::AddRow(const std::vector<std::string>& values) {
    LocalPrepareBuilder::AddRow(values, dspb::DELETE);
}

void DeleteRequestBuilder::AddRows(const std::vector<std::vector<std::string>>& rows) {
    for (const auto& row: rows) {
        AddRow(row);
    }
}

InsertRequestBuilder::InsertRequestBuilder(Table *t) : LocalPrepareBuilder(t) {
}

void InsertRequestBuilder::AddRow(const std::vector<std::string>& values) {
    LocalPrepareBuilder::AddRow(values, dspb::INSERT);
}

void InsertRequestBuilder::AddRows(const std::vector<std::vector<std::string>>& rows) {
    for (const auto& row: rows) {
        AddRow(row);
    }
}

void InsertRequestBuilder::SetCheckUnique() {
    for (auto &intnet : *req_.mutable_intents()) {
        intnet.set_check_unique(true);
    }
}

} /* namespace helper */
} /* namespace test */
} /* namespace jimkv */

