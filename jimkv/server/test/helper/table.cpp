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

#include "table.h"

namespace jimkv {
namespace test {
namespace helper {

Table::Table(const std::string& table_name, uint32_t table_id) {
    meta_.set_db_id(default_db_id);
//    meta_.set_db_name("shark_test_db");
    meta_.set_name(table_name);
    meta_.set_id(table_id);
}

Table::Table(const basepb::TableInfo& meta) : meta_(meta) {
    if (!meta_.columns().empty()) {
        last_col_id_ = meta_.columns(meta_.columns_size() - 1).id();
    }
}

std::vector<basepb::ColumnInfo> Table::GetPKs() const {
    std::vector<basepb::ColumnInfo> cols;
    for (const auto& col : meta_.columns()) {
        if (col.primary() > 0) cols.push_back(col);
    }
    return cols;
}

std::vector<basepb::ColumnInfo> Table::GetNonPkColumns() const {
    std::vector<basepb::ColumnInfo> cols;
    for (const auto& col : meta_.columns()) {
        if (col.primary() == 0) cols.push_back(col);
    }
    return cols;
}

std::vector<basepb::ColumnInfo> Table::GetAllColumns() const {
    std::vector<basepb::ColumnInfo> cols;
    for (const auto& col : meta_.columns()) {
        cols.push_back(col);
    }
    return cols;
}

basepb::ColumnInfo Table::GetColumn(uint64_t id) const {
    auto it = std::find_if(meta_.columns().cbegin(), meta_.columns().cend(),
            [id](const basepb::ColumnInfo& col){ return col.id() == id; });
    if (it != meta_.columns().cend()) {
        return *it;
    } else {
        throw std::runtime_error(std::string("column id not found: ") + std::to_string(id));
    }
}

basepb::ColumnInfo Table::GetColumn(const std::string& name) const {
    auto it = std::find_if(meta_.columns().cbegin(), meta_.columns().cend(),
                           [&name](const basepb::ColumnInfo& col){ return col.name() == name; });
    if (it != meta_.columns().cend()) {
        return *it;
    } else {
        throw std::runtime_error(std::string("column name not found: ") + name);
    }
}

static dspb::ColumnInfo makeColumnInfo(const basepb::ColumnInfo& col) {
    dspb::ColumnInfo info;
    info.set_typ(col.sql_type().type());
    info.set_id(col.id());
    info.set_unsigned_(col.sql_type().unsigned_());
    return info;
}

dspb::ColumnInfo Table::GetColumnInfo(uint64_t id) const {
    return makeColumnInfo(GetColumn(id));
}

dspb::ColumnInfo Table::GetColumnInfo(const std::string& name) const {
    return makeColumnInfo(GetColumn(name));
}

void Table::AddColumn(const std::string& name, basepb::DataType type, bool is_pk) {
    auto col = meta_.add_columns();
    col->set_id(++last_col_id_);
    col->set_name(name);
    col->mutable_sql_type()->set_type(type);
    col->set_primary(is_pk ? 1 : 0);
}

std::unique_ptr<Table> CreateAccountTable(uint32_t tid) {
    std::unique_ptr<Table> t(new Table("account", tid));
    t->AddColumn("id", basepb::BigInt, true);
    t->AddColumn("name", basepb::Varchar);
    t->AddColumn("balance", basepb::BigInt);
    return t;
}

std::unique_ptr<Table> CreatePersonTable(uint32_t tid) {
    std::unique_ptr<Table> t(new Table("person", tid));
    t->AddColumn("id", basepb::BigInt, true);
    t->AddColumn("name", basepb::Varchar);
    t->AddColumn("age", basepb::SmallInt);
    t->AddColumn("height", basepb::Double);
    return t;
}


std::unique_ptr<Table> CreateUserTable(uint32_t tid) {
    std::unique_ptr<Table> t(new Table("user", tid));
    t->AddColumn("user_name", basepb::Varchar, true);
    t->AddColumn("pass_word", basepb::Varchar);
    t->AddColumn("real_name", basepb::Varchar);
    return t;
}

std::unique_ptr<Table> CreateAccountTable() {
    return CreateAccountTable(default_table_id);
}

std::unique_ptr<Table> CreatePersonTable() {
    return CreatePersonTable(default_person_table_id);
}

std::unique_ptr<Table> CreateUserTable() {
    return CreateUserTable(default_user_table_id);
}

std::unique_ptr<Table> CreateHashUserTable(uint32_t tid) {
    std::unique_ptr<Table> t(new Table("user", tid));
    t->AddColumn("h", basepb::Int, true);
    t->AddColumn("user_name", basepb::Varchar, true);
    t->AddColumn("pass_word", basepb::Varchar);
    t->AddColumn("real_name", basepb::Varchar);
    return t;
}

std::unique_ptr<Table> CreateHashUserTable() {
    return CreateHashUserTable(default_hashuser_table_id);
}

} /* namespace helper */
} /* namespace test */
} /* namespace jimkv */
