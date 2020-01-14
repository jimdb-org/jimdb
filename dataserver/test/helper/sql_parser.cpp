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

#include "sql_parser.h"

#include "storage/util.h"
#include "helper_util.h"

namespace jim {
namespace test {
namespace helper {

using namespace jim::ds::storage;

static dspb::ExprType parseOpType(hsql::OperatorType type) {
    switch (type) {
    case hsql::kOpAnd:
        return dspb::LogicAnd;
    case hsql::kOpOr:
        return dspb::LogicOr;
    case hsql::kOpNot:
        return dspb::LogicNot;
    case hsql::kOpEquals:
        return dspb::Equal;
    case hsql::kOpNotEquals:
        return dspb::NotEqual;
    case hsql::kOpLess:
        return dspb::Less;
    case hsql::kOpLessEq:
        return dspb::LessOrEqual;
    case hsql::kOpGreater:
        return dspb::Larger;
    case hsql::kOpGreaterEq:
        return dspb::LargerOrEqual;
    case hsql::kOpPlus:
        return dspb::Plus;
    case hsql::kOpMinus:
        return dspb::Minus;
    case hsql::kOpAsterisk:
        return dspb::Mult;
    case hsql::kOpSlash:
        return dspb::Div;
    default:
        throw std::runtime_error(std::string("unsupported op type: ") + std::to_string(type));
    }
}

void parseExpr(const Table& t, hsql::Expr* from, dspb::SelectFlowRequest* req) {
    assert(from != nullptr);
    assert(req != nullptr);

    auto selection_processor = req->add_processors();
    selection_processor->set_type(dspb::SELECTION_TYPE);
    auto selection = selection_processor->mutable_selection();

    dspb::Expr expr;
    parseExpr(t, from, &expr);
    selection->add_filter()->CopyFrom(expr);
}

static void parseOperatorExpr(const Table& t, hsql::Expr* from, dspb::Expr* to) {
    assert(from->type == hsql::kExprOperator);
    to->set_expr_type(parseOpType(from->opType));
    if (from->expr != nullptr) {
        parseExpr(t, from->expr, to->add_child());
    }
    if (from->expr2 != nullptr) {
        parseExpr(t, from->expr2, to->add_child());
    }
}

void parseExpr(const Table& t, hsql::Expr* from, dspb::Expr* to) {
    assert(from != nullptr);
    assert(to != nullptr);

    switch (from->type) {
    case hsql::kExprLiteralFloat:
        to->set_expr_type(dspb::Const_Double);
        to->set_value(std::to_string(from->fval));
        break;
    case hsql::kExprLiteralString:
        to->set_expr_type(dspb::Const_Bytes);
        to->set_value(from->name);
        break;
    case hsql::kExprLiteralInt:
        to->set_expr_type(dspb::Const_Int);
        to->set_value(std::to_string(from->ival));
        break;
    case hsql::kExprColumnRef: {
        to->set_expr_type(dspb::Column);
        auto col = t.GetColumn(from->name);
        fillColumnInfo(col, to->mutable_column());
        break;
    }
    case hsql::kExprOperator:
        parseOperatorExpr(t, from, to);
        break;
    default:
        throw std::runtime_error(std::string("unsupported expr type: ") + std::to_string(from->type));
    }
}

static int64_t parseMustConstInt(hsql::Expr* expr) {
    assert(expr != nullptr);
    if (!expr->isType(hsql::kExprLiteralInt)) {
        throw std::runtime_error(std::string("not literal int expr: ") + std::to_string(expr->type));
    }
    return expr->ival;
}

void parseLimit(hsql::LimitDescription* from, dspb::SelectFlowRequest* req) {
    if (from->limit == nullptr && from->offset == nullptr) {
        std::cout << "from->limit == nullptr && from->offset == nullptr" << std::endl;
        return;
    }
    auto limit_processor = req->add_processors();
    limit_processor->set_type(dspb::LIMIT_TYPE);
    auto limit_read = limit_processor->mutable_limit();

    if (from->limit != nullptr) {
        limit_read->set_count(parseMustConstInt(from->limit));
    }

    if (from->offset != nullptr) {
        limit_read->set_offset(parseMustConstInt(from->offset));
    }
}

void parseLimit(hsql::LimitDescription* from, dspb::Limit* to) {
    if (from->limit != nullptr) {
        to->set_count(parseMustConstInt(from->limit));
    }
    if (from->offset != nullptr) {
        to->set_offset(parseMustConstInt(from->offset));
    }
}

void parseFieldList(const Table& t, const hsql::SelectStatement* sel, dspb::SelectRequest* req) {
    if (sel->selectList == nullptr) {
        throw std::runtime_error("invalid select field list");
    }

    for (auto expr: *sel->selectList) {
        switch (expr->type) {
        case hsql::kExprStar: {
            auto cols = t.GetAllColumns();
            for (const auto& col : cols) {
                auto field = req->add_field_list();
                field->set_typ(dspb::SelectField_Type_Column);
                fillColumnInfo(col, field->mutable_column());
            }
            break;
        }
        case hsql::kExprColumnRef: {
            auto field = req->add_field_list();
            field->set_typ(dspb::SelectField_Type_Column);
            auto col = t.GetColumn(expr->name);
            fillColumnInfo(col, field->mutable_column());
            break;
        }
        case hsql::kExprFunctionRef: {
            auto field = req->add_field_list();
            field->set_typ(dspb::SelectField_Type_AggreFunction);
            field->set_aggre_func(expr->name);
            if (expr->exprList == nullptr || expr->exprList->size() != 1) {
                throw std::runtime_error("expect aggragate parameter size = 1");
            }
            auto para_expr = (*expr->exprList)[0];
            switch (para_expr->type) {
            case hsql::kExprStar:
                break;
            case hsql::kExprColumnRef: {
                auto col = t.GetColumn(para_expr->name);
                fillColumnInfo(col, field->mutable_column());
                break;
            }
            default:
                throw std::runtime_error(std::string("unknown aggre paramenter type: ") + std::to_string(para_expr->type));
            }
            break;
        }
        default:
            throw std::runtime_error(std::string("unknown select field list expr type: ") + std::to_string(expr->type));
        }
    }
}

void parseFieldList(const Table& t, const hsql::SelectStatement* sel, dspb::SelectFlowRequest* select_flow) {
    if (sel->selectList == nullptr) {
        throw std::runtime_error("invalid select field list");
    }

    // add table read processor
    auto table_read_processor = select_flow->add_processors();
    table_read_processor->set_type(dspb::TABLE_READ_TYPE );
    auto table_read = table_read_processor-> mutable_table_read();
    dspb::Processor *agg_processor = nullptr;
    int nOffset = 0;
    // std::cout << "parseFieldList sel->selectList->size:" << sel->selectList->size() << std::endl;
    for (auto expr: *sel->selectList) {
        switch (expr->type) {
        case hsql::kExprStar: {
            auto cols = t.GetAllColumns();
            for (const auto& col : cols) {
                table_read->add_columns()->CopyFrom(t.GetColumnInfo(col.id()));
                select_flow->add_output_offsets(nOffset++);
            }
            break;
        }
        case hsql::kExprColumnRef: {
            auto col = t.GetColumn(expr->name);
            table_read->add_columns()->CopyFrom(t.GetColumnInfo(col.id()));
            select_flow->add_output_offsets(nOffset++);
            break;
        }
        case hsql::kExprFunctionRef: {
            if (agg_processor == nullptr) {
                agg_processor = select_flow->add_processors();
                agg_processor->set_type(dspb::AGGREGATION_TYPE);
            }
            
            if (expr->exprList == nullptr || expr->exprList->size() != 1) {
                throw std::runtime_error("expect aggragate parameter size = 1");
            }
            std::string func = expr->name;
            auto agg = agg_processor->mutable_aggregation();
            auto func_tmp = agg->add_func();
            // std::cout << "agg->add_func()" << std::endl;
            if (func.compare("count") == 0) {
                func_tmp->set_expr_type(dspb::Count);
            } else if (func.compare("avg") == 0) {
                func_tmp->set_expr_type(dspb::Avg);
            } else if (func.compare("sum") == 0) {
                func_tmp->set_expr_type(dspb::Sum);
            } else if (func.compare("min") == 0) {
                func_tmp->set_expr_type(dspb::Min);
            } else if (func.compare("max") == 0) {
                func_tmp->set_expr_type(dspb::Max);
            } else {
                throw std::runtime_error(std::string("unknown aggre func: ") + expr->name);
            }
            // select_flow->add_output_offsets(nOffset++);
            auto para_expr = (*expr->exprList)[0];
            switch (para_expr->type) {
            case hsql::kExprStar:
            case hsql::kExprLiteralInt:
                if (table_read->columns().empty()) {
                    auto cols = t.GetAllColumns();
                    for (const auto& col : cols) {
                        table_read->add_columns()->CopyFrom(t.GetColumnInfo(col.id()));
                        select_flow->add_output_offsets(nOffset++);
                    }
                }
                break;
            case hsql::kExprColumnRef: {
                auto col = t.GetColumn(para_expr->name);
                table_read->add_columns()->CopyFrom(t.GetColumnInfo(col.id()));
//                func_tmp->mutable_column()->CopyFrom(t.GetColumnInfo(col.id()));
                auto child = func_tmp->add_child();
                child->set_expr_type(dspb::ExprType::Column);
                child->mutable_column()->CopyFrom(t.GetColumnInfo(col.id()));
                break;
            }
            default:
                throw std::runtime_error(std::string("unknown aggre paramenter type: ") + std::to_string(para_expr->type));
            }
            break;
        }
        default:
            throw std::runtime_error(std::string("unknown select field list expr type: ") + std::to_string(expr->type));
        }
    }
    std::string start_key;
    std::string end_key;
    EncodeKeyPrefix(&start_key, t.GetID());
    EncodeKeyPrefix(&end_key, t.GetID()+1);
    table_read->set_type(dspb::KEYS_RANGE_TYPE);
    table_read->mutable_range()->set_start_key(start_key);
    table_read->mutable_range()->set_end_key(end_key);
    table_read->set_desc(false);
}

void parseOrder(const Table& t, const std::vector<hsql::OrderDescription*>* orders, dspb::SelectFlowRequest* req) {
    auto order_by_processor = req->add_processors();
    order_by_processor->set_type(dspb::ORDER_BY_TYPE);
    auto ordering = order_by_processor->mutable_ordering();
    for (auto &order : *orders) {
        auto cl = ordering->add_columns();
        cl->mutable_expr()->set_expr_type(dspb::Column);
        auto col = t.GetColumn(order->expr->name);
        cl->mutable_expr()->mutable_column()->set_id(col.id());
        cl->set_asc(true);
        if (order->type == hsql::kOrderDesc) {
            cl->set_asc(false);
        }
    }
    ordering->set_count(1024);
}
} /* namespace helper */
} /* namespace test */
} /* namespace jim */
