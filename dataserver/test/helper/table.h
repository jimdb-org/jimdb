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

#include <memory>

#include "dspb/expr.pb.h"
#include "basepb/metapb.pb.h"

namespace jim {
namespace test {
namespace helper {

const static uint32_t default_db_id = 1;

const static uint32_t default_table_id = 10;
const static uint32_t default_account_table_id = 11;
const static uint32_t default_user_table_id = 12;
const static uint32_t default_hashuser_table_id = 13;
const static uint32_t default_person_table_id = 14;

const static uint32_t default_account_index_id1 = 21;
const static uint32_t default_account_index_id2 = 22;

class Table {
public:
    Table(const std::string& table_name, uint32_t table_id = default_table_id);
    explicit Table(const basepb::TableInfo& meta);
    ~Table() = default;

    uint64_t GetID() const { return meta_.id(); }
    std::string GetName() const { return meta_.name(); }

    std::vector<basepb::ColumnInfo> GetPKs() const;
    std::vector<basepb::ColumnInfo> GetAllColumns() const;
    std::vector<basepb::ColumnInfo> GetNonPkColumns() const;
    basepb::ColumnInfo GetColumn(uint64_t id) const;
    basepb::ColumnInfo GetColumn(const std::string& name) const;

    dspb::ColumnInfo GetColumnInfo(uint64_t id) const;
    dspb::ColumnInfo GetColumnInfo(const std::string& name) const;

    const basepb::TableInfo& GetMeta() const { return meta_; }

    void AddColumn(const std::string& name, basepb::DataType type, bool is_pk = false);

private:
    basepb::TableInfo meta_;
    uint64_t last_col_id_ = 0;
};

std::unique_ptr<Table> CreateAccountTable();
std::unique_ptr<Table> CreateUserTable();
std::unique_ptr<Table> CreatePersonTable();
std::unique_ptr<Table> CreateHashUserTable();

std::unique_ptr<Table> CreateAccountTable(uint32_t tid);
std::unique_ptr<Table> CreateUserTable(uint32_t tid);
std::unique_ptr<Table> CreateHashUserTable(uint32_t tid);

} /* namespace helper */
} /* namespace test */
} /* namespace jim */
