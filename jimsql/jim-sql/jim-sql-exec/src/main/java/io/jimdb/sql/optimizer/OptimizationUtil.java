/*
 * Copyright 2019 The JIMDB Authors.
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
package io.jimdb.sql.optimizer;

import static io.jimdb.core.expression.ExpressionType.FUNC;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import io.jimdb.core.Session;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.ExpressionType;
import io.jimdb.core.expression.FuncExpr;
import io.jimdb.core.expression.KeyColumn;
import io.jimdb.core.expression.Schema;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.expression.aggregate.AggregateExpr;
import io.jimdb.core.expression.functions.FuncType;
import io.jimdb.core.model.meta.Column;
import io.jimdb.sql.operator.Aggregation;
import io.jimdb.sql.operator.DualTable;
import io.jimdb.sql.operator.Projection;
import io.jimdb.sql.operator.RelOperator;
import io.jimdb.sql.optimizer.statistics.StatsUtils;
import io.jimdb.core.values.LongValue;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * OptimizationUtil for common methods
 */
@SuppressFBWarnings("PSC_PRESIZE_COLLECTIONS")
public class OptimizationUtil {
  public static boolean hasVarSet(Expression expression) {
    if (expression.getExprType() != FUNC) {
      return false;
    }

    if (expression.getExprType() == FUNC) {
      FuncExpr funcExpr = (FuncExpr) expression;

      if (FuncType.SetVariable.name().equals(funcExpr.getFunc().getName())) {
        return true;
      }

      for (Expression expr : funcExpr.getArgs()) {
        if (hasVarSet(expr)) {
          return true;
        }
      }
    }
    return false;
  }

  public static boolean hasVarSetGet(Expression expression) {
    if (expression.getExprType() != FUNC) {
      return false;
    }

    if (expression.getExprType() == FUNC) {
      FuncExpr funcExpr = (FuncExpr) expression;

      String name = funcExpr.getFunc().getName();
      // todo: the FuncType.GetVariable needs to be added
      if (FuncType.SetVariable.name().equals(name) || FuncType.GetVariable.name().equals(name)) {
        return true;
      }

      for (Expression expr : funcExpr.getArgs()) {
        if (hasVarSetGet(expr)) {
          return true;
        }
      }
    }
    return false;
  }

  public static Projection removeAggregation(Aggregation aggregation) {
    List<ColumnExpr> groupByColumns = Arrays.asList(aggregation.getGroupByColumnExprs());
    Schema groupBySchema = new Schema(groupByColumns);

    List<KeyColumn> keyColumns = aggregation.getChildren()[0].getSchema().getKeyColumns();
    boolean foundKeyColumns = false;
    for (KeyColumn keyColumn : keyColumns) {
      List<Integer> posList = groupBySchema.indexPosInSchema(keyColumn.getColumnExprs());
      if (posList != null) {
        foundKeyColumns = true;
        break;
      }
    }

    if (foundKeyColumns) {
      // we have found key columns in group columns, so this aggregation operator can be removed
      Projection projection = convertAgg2Projection(aggregation);
      projection.setChildren(aggregation.getChildren()[0]);
      return projection;
    }
    return null;
  }

  public static Projection convertAgg2Projection(Aggregation aggregation) {

    AggregateExpr[] aggregateExprs = aggregation.getAggregateExprs();
    Expression[] projectExprs = new Expression[aggregateExprs.length];
    int i = 0;
    for (AggregateExpr aggregateExpr : aggregateExprs) {
      Expression expression = OptimizationUtil.convertAggFuncExpr(aggregateExpr);
      projectExprs[i++] = expression;
    }

    return new Projection(projectExprs, aggregation.getSchema().clone());
  }

  @SuppressFBWarnings("UP_UNUSED_PARAMETER")
  public static Expression convertAggFuncExpr(AggregateExpr aggregateExpr) {
    // todo:convert aggregate function/expression to common expression
//    System.out.println(JSON.toJSONString(aggregateExpr));
    return new ColumnExpr(0L);
  }

  public static List<Expression> propagateConstants(List<Expression> expressions, Session session) {
    ConstantPropagator constantPropagator = new ConstantPropagator(expressions, session);
    return constantPropagator.propagateConstants();
  }

  public static DualTable exprs2DualTable(Session session, RelOperator operator, List<Expression> expressions) {
    if (expressions == null || expressions.size() != 1) {
      return null;
    }
    Expression expression = expressions.get(0);

    if (expression.getExprType() == ExpressionType.CONST) {
      LongValue longValue = expression.execLong(session, ValueAccessor.EMPTY);
      long value = longValue.getValue();
      if (value == 0) {
        DualTable dualTable = new DualTable(1);
        dualTable.setSchema(operator.getSchema());
        return dualTable;
      }
    }

    return null;
  }

  public static double getSortCost(double count) {
    return count * (StatsUtils.CPU_FACTOR + StatsUtils.MEMORY_FACTOR);
  }

//  public static boolean columnsCoveredByIndex(Column[] pkCols, List<ColumnExpr> columnExprs, List<ColumnExpr> indexCols) {
//    if (columnExprs == null || indexCols == null) {
//      return false;
//    }
//
//    Set<Integer> indexColumnIds = indexCols.stream().map(ColumnExpr::getId).collect(Collectors.toSet());
//
//    for (ColumnExpr columnExpr : columnExprs) {
//
//      if (!indexColumnIds.contains(columnExpr.getId())) {
//        return false;
//      }
//    }
//
//    return true;
//  }

  public static boolean columnsCoveredByIndex(Column[] pkCols, List<ColumnExpr> columnExprs, Column[] indexColumns) {
    if (columnExprs == null) {
      return false;
    }

    Set<Integer> indexColumnIds = Arrays.stream(indexColumns).map(Column::getId).collect(Collectors.toSet());

    for (Column pkCol : pkCols) {
      indexColumnIds.add(pkCol.getId());
    }

    for (ColumnExpr columnExpr : columnExprs) {
      if (!indexColumnIds.contains(columnExpr.getId())) {
        return false;
      }
    }

    return true;
  }
}
