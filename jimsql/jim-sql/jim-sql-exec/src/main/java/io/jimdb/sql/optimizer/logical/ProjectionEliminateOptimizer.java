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
package io.jimdb.sql.optimizer.logical;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.jimdb.core.Session;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.ExpressionType;
import io.jimdb.core.expression.FuncExpr;
import io.jimdb.core.expression.Schema;
import io.jimdb.core.expression.aggregate.AggregateExpr;
import io.jimdb.pb.Metapb;
import io.jimdb.sql.operator.Aggregation;
import io.jimdb.sql.operator.Order;
import io.jimdb.sql.operator.Projection;
import io.jimdb.sql.operator.RelOperator;
import io.jimdb.sql.operator.Selection;
import io.jimdb.sql.optimizer.OptimizationUtil;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings({ "CFS_CONFUSING_FUNCTION_SEMANTICS", "CE_CLASS_ENVY" })
public class ProjectionEliminateOptimizer implements IRuleOptimizer {

  private LogicalOptimizationVisitor eliminatedVisitor;

  @Override
  public RelOperator optimize(RelOperator logicalOperator, Session session) {
    Map<Long, ColumnExpr> replaceMap = new HashMap<>();
    eliminatedVisitor = new ProjectionEliminateVisitor(replaceMap);
    return eliminate(logicalOperator, replaceMap, false);
  }

  private RelOperator eliminate(RelOperator logicalOperator, Map<Long, ColumnExpr> replace, boolean canEliminate) {
    boolean isProjection = false;
    boolean isChild = canEliminate;
    Projection projection = null;
    if (logicalOperator instanceof Projection) {
      projection = (Projection) logicalOperator;
      isProjection = true;
    }

    if (logicalOperator instanceof Aggregation || isProjection) {
      isChild = true;
    }

    if (logicalOperator.hasChildren()) {
      for (int i = 0; i < logicalOperator.getChildren().length; i++) {
        logicalOperator.getChildren()[i] = eliminate(logicalOperator.getChildren()[i], replace, isChild);
      }
    }


//    switch (logicalOperator.getRelOperatorType()) {
//      case JOIN:
      //TODO JOIN
//      case APPLY:
      //TODO APPLY
//      default:
//        logicalOperator.getSchema().getColumns().forEach(e -> {
//          resolveColumnAndReplace(e, replace);
//        });
//        break;
//    }

    logicalOperator.getSchema().getColumns().forEach(e -> {
      resolveColumnAndReplace(e, replace);
    });

    logicalOperator.acceptVisitor(eliminatedVisitor);

    if (isProjection) {
      if (logicalOperator.hasChildren()) {
        if (logicalOperator.getChildren()[0] instanceof Projection) {
          Projection child = (Projection) logicalOperator.getChildren()[0];
          if (hasVarSet(child.getExpressions())) {
            for (int i = 0; i < projection.getExpressions().length; i++) {
              projection.getExpressions()[i] = resolveExprAndReplaceInProj(projection.getExpressions()[i], child);
            }
            logicalOperator.getChildren()[0] = child.getChildren()[0];
          }
        }
      }
    }

    if (!(isProjection && canEliminate && canBeEliminated(projection))) {
      return logicalOperator;
    }

    Expression[] expressions = projection.getExpressions();
    List<ColumnExpr> schemaColumns = projection.getSchema().getColumns();
    for (int i = 0; i < schemaColumns.size(); i++) {
      if (expressions[i].getExprType() == ExpressionType.COLUMN) {
        ColumnExpr columnExpr = (ColumnExpr) expressions[i];
        replace.put(schemaColumns.get(i).getUid(), columnExpr);
      }
    }

    return logicalOperator.getChildren()[0];
  }

  private boolean hasVarSet(Expression[] expressions) {
    for (Expression expression : expressions) {
      if (OptimizationUtil.hasVarSet(expression)) {
        return true;
      }
    }

    return false;
  }

  private static void resolveExprAndReplace(Expression expression, Map<Long, ColumnExpr> replace) {
    switch (expression.getExprType()) {
      case COLUMN:
        resolveColumnAndReplace((ColumnExpr) expression, replace);
        break;
      case FUNC:
        FuncExpr func = (FuncExpr) expression;
        for (Expression funArg : func.getArgs()) {
          resolveExprAndReplace(funArg, replace);
        }
        break;
      default:
        break;
    }
  }

  private Expression resolveExprAndReplaceInProj(Expression expression, Projection replace) {
    switch (expression.getExprType()) {
      case COLUMN:
        int columnIndex = replace.getSchema().getColumnIndex((ColumnExpr) expression);
        if (columnIndex != -1 && columnIndex < replace.getExpressions().length) {
          return replace.getExpressions()[columnIndex];
        }
        break;
      case FUNC:
        FuncExpr func = (FuncExpr) expression;
        Expression[] args = func.getArgs();
        for (int i = 0; i < args.length; i++) {
          Expression funArg = args[i];
          args[i] = resolveExprAndReplaceInProj(funArg, replace);
        }
        break;
      default:
        break;
    }

    return expression;
  }

  private static void resolveColumnAndReplace(ColumnExpr orgColumnExpr, Map<Long, ColumnExpr> replace) {
    ColumnExpr columnExpr = replace.get(orgColumnExpr.getUid());
    if (columnExpr != null) {
      replaceColumn(orgColumnExpr, columnExpr);
    }
  }

  private static void replaceColumn(ColumnExpr orgColumnExpr, ColumnExpr replaceColumn) {
    boolean isInSubQuery = orgColumnExpr.isInSubQuery();
    String oriColName = orgColumnExpr.getOriCol();
    Metapb.SQLType resultType = orgColumnExpr.getResultType();
    orgColumnExpr.setOther(replaceColumn);
    orgColumnExpr.setInSubQuery(isInSubQuery);
    orgColumnExpr.setResultType(resultType);
    orgColumnExpr.setOriCol(oriColName);
  }

  private boolean canBeEliminated(Projection projection) {
    if (projection == null) {
      return false;
    }
    for (Expression expression : projection.getExpressions()) {
      if (expression.getExprType() != ExpressionType.COLUMN) {
        return false;
      }
    }
    return true;
  }

  private static RelOperator doPhysicalProjectionElimination(RelOperator plan) {
    if (!plan.hasChildren()) {
      return plan;
    }

    for (int i = 0; i < plan.getChildren().length; i++) {
      plan.getChildren()[i] = doPhysicalProjectionElimination(plan.getChildren()[i]);
    }

    if (!(plan instanceof Projection) || !canProjectionBeEliminatedStrict((Projection) plan)) {
      return plan;
    }
    return plan.getChildren()[0];
  }

  public static RelOperator eliminatePhysicalProjection(RelOperator plan) {
    Schema oldSchema = plan.getSchema();
    RelOperator newPlan = doPhysicalProjectionElimination(plan);
    List<ColumnExpr> newColumns = newPlan.getSchema().getColumns();
    for (int i = 0; i < oldSchema.getColumns().size(); i++) {
      ColumnExpr oldColumn = oldSchema.getColumns().get(i);
      oldColumn.setOffset(newColumns.get(i).getOffset());
      newColumns.set(i, oldColumn);
    }
    return newPlan;
  }

  private static boolean canProjectionBeEliminatedStrict(Projection projection) {

    if (projection.getSchema().isEmpty()) {
      return true;
    }

    if (projection.getSchema().size() != projection.getChildren()[0].getSchema().size()) {
      return false;
    }

    Expression[] expressions = projection.getExpressions();
    Schema schema = projection.getChildren()[0].getSchema();
    for (int i = 0; i < expressions.length; i++) {
      if (expressions[i].getExprType() == ExpressionType.COLUMN) {
        if (!expressions[i].equals(schema.getColumn(i))) {
          return false;
        }
      } else {
        return false;
      }
    }

    return true;
  }

  /**
   * @version V1.0
   */
  public static class ProjectionEliminateVisitor extends LogicalOptimizationVisitor {

    private Map<Long, ColumnExpr> replace;

    public ProjectionEliminateVisitor(Map<Long, ColumnExpr> replace) {
      this.replace = replace;
    }

    @Override
    public RelOperator visitOperator(Projection operator) {
      for (Expression expression : operator.getExpressions()) {
        resolveExprAndReplace(expression, replace);
      }
      return null;
    }

    @Override
    public RelOperator visitOperator(Aggregation operator) {
      for (AggregateExpr aggregateExpr : operator.getAggregateExprs()) {
        for (Expression expression : aggregateExpr.getArgs()) {
          resolveExprAndReplace(expression, replace);
        }
      }

      for (Expression expression : operator.getGroupByExprs()) {
        resolveExprAndReplace(expression, replace);
      }
      operator.collectGroupByColumns();
      return null;
    }

    @Override
    public RelOperator visitOperator(Selection operator) {
      for (Expression expression : operator.getConditions()) {
        resolveExprAndReplace(expression, replace);
      }
      return null;
    }

    @Override
    public RelOperator visitOperator(Order operator) {
      for (Order.OrderExpression orderExpression : operator.getOrderExpressions()) {
        resolveExprAndReplace(orderExpression.getExpression(), replace);
      }

      return null;
    }
  }
}
