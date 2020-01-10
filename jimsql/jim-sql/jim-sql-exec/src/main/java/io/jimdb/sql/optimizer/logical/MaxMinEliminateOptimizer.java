/*
 * Copyright 2019 The JimDB Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package io.jimdb.sql.optimizer.logical;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.jimdb.core.Session;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.ExpressionUtil;
import io.jimdb.core.expression.aggregate.AggregateExpr;
import io.jimdb.core.expression.aggregate.AggregateType;
import io.jimdb.sql.operator.Aggregation;
import io.jimdb.sql.operator.Order;
import io.jimdb.sql.operator.RelOperator;
import io.jimdb.sql.operator.Selection;

import com.alibaba.druid.sql.ast.SQLOrderingSpecification;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Optimizer of eliminateMaxMin for logical optimization.
 */
@SuppressFBWarnings({ "CFS_CONFUSING_FUNCTION_SEMANTICS", "DLS_DEAD_LOCAL_STORE", "UC_USELESS_OBJECT" })
public class MaxMinEliminateOptimizer implements IRuleOptimizer {

  @Override
  public RelOperator optimize(RelOperator logicalOperator, Session session) {
    eliminateMaxMin(logicalOperator);
    return null;
  }

  public void eliminateMaxMin(RelOperator logicalOperator) {
    if (logicalOperator instanceof Aggregation) {
      Aggregation aggregation = (Aggregation) logicalOperator;
      AggregateExpr[] aggFuncs = aggregation.getAggregateExprs();
      ColumnExpr[] groupByColumnExpr = aggregation.getGroupByColumnExprs();
      if (aggFuncs == null || aggFuncs.length != 1 || groupByColumnExpr.length != 0) {
        return;
      }
      AggregateExpr aggFunc = aggFuncs[0];

      if (!aggFunc.getName().equals(AggregateType.MAX.getName()) && aggFunc.getName().equals(AggregateType.MIN
              .getName())) {
        return;
      }

      RelOperator child = logicalOperator.getChildren()[0];
      List<ColumnExpr> resultColumn = new ArrayList<>();
      Expression expression = aggFunc.getArgs()[0];
      ExpressionUtil.extractColumns(resultColumn, expression, null);

      if (!resultColumn.isEmpty()) {
        if (!expression.getResultType().getNotNull()) {

          child = new Selection(Collections.singletonList(expression), logicalOperator.getChildren()[0]);
          //TODO warp not null expression
        }

        SQLOrderingSpecification specification = aggFunc.getName().equals(AggregateType.MAX.getName())
                ? SQLOrderingSpecification.DESC : SQLOrderingSpecification.ASC;

        Order.OrderExpression orderExpression = new Order.OrderExpression(aggFunc.getArgs()[0], specification);
        child = new Order(new Order.OrderExpression[]{ orderExpression }, child);
      }
    }
  }
}
