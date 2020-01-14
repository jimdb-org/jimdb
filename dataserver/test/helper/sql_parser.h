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

#include "SQLParser.h"

#include "dspb/txn.pb.h"
#include "table.h"

namespace jim {
namespace test {
namespace helper {

void parseExpr(const Table& t, hsql::Expr* from, dspb::Expr* to);
void parseExpr(const Table& t, hsql::Expr* from, dspb::SelectFlowRequest* req);
void parseLimit(hsql::LimitDescription* from, dspb::Limit* to);
void parseLimit(hsql::LimitDescription* from, dspb::SelectFlowRequest* req);
void parseFieldList(const Table& t, const hsql::SelectStatement* sel, dspb::SelectRequest* req);
void parseFieldList(const Table& t, const hsql::SelectStatement* sel, dspb::SelectFlowRequest* req);
void parseOrder(const Table& t, const std::vector<hsql::OrderDescription*>* orders, dspb::SelectFlowRequest* req);

} /* namespace helper */
} /* namespace test */
} /* namespace jim */
