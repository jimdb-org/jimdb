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
import io.jimdb.core.expression.ExpressionType;
import io.jimdb.core.expression.ExpressionUtil;
import io.jimdb.sql.operator.Aggregation;
import io.jimdb.sql.operator.DualTable;
import io.jimdb.sql.operator.Projection;
import io.jimdb.sql.operator.RelOperator;
import io.jimdb.sql.operator.Selection;
import io.jimdb.sql.operator.TableSource;
import io.jimdb.sql.optimizer.OptimizationUtil;
import io.jimdb.sql.optimizer.ParameterizedOperatorVisitor;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

/**
 * Predicates push down optimizer extracts all the predicates that can be pushed down
 * to data server.
 */
@SuppressFBWarnings()
public class PredicatesPushDownOptimizer implements IRuleOptimizer {

  private static PredicatesPushDownVisitor visitor = new PredicatesPushDownVisitor();

  @SuppressFBWarnings({ "DLS_DEAD_LOCAL_STORE" })
  @Override
  public RelOperator optimize(RelOperator relOperator, Session session) {
    return relOperator.acceptVisitor(session, visitor, new ArrayList<>()).getT1();
  }

  /**
   * Implementation class for predicate push down visitor.
   */
  private static class PredicatesPushDownVisitor extends ParameterizedOperatorVisitor<List<Expression>, Tuple2<RelOperator, List<Expression>>> {

    private void extractPushDownPredicates(List<Expression> preds, List<Expression> predsPushDown,
                                           List<Expression> predsRetain) {
      for (Expression expression : preds) {
        if (OptimizationUtil.hasVarSetGet(expression)) {
          predsRetain.add(expression);
        } else {
          predsPushDown.add(expression);
        }
      }
    }

    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE")
    @Override
    public Tuple2<RelOperator, List<Expression>> visitOperator(Session session, Aggregation aggregation, List<Expression> predicates) {
      Expression[] originalColumnExpressions = new Expression[aggregation.getAggregateExprs().length];

      for (int i = 0; i < aggregation.getAggregateExprs().length; i++) {
        originalColumnExpressions[i] = aggregation.getAggregateExprs()[i].getArgs()[0];
      }

      List<Expression> result = new ArrayList<>();
      List<Expression> pushDownExpressions = new ArrayList<>(predicates.size());

      for (Expression predicate : predicates) {
        if (predicate.getExprType() == ExpressionType.CONST) {
          pushDownExpressions.add(predicate);
          result.add(predicate);
        } else if (predicate.getExprType() == ExpressionType.FUNC) {
          List<ColumnExpr> cols = new ArrayList<>();
          ExpressionUtil.extractColumns(cols, predicate, null);
          boolean match = true;
          for (ColumnExpr col : cols) {
            if (!ExpressionUtil.containsExpr(aggregation.getGroupByExprs(), col)) {
              match = false;
              break;
            }
          }
          if (match) {
            Expression newExpr = ExpressionUtil.substituteColumn(session, predicate, aggregation.getSchema(), originalColumnExpressions);
            pushDownExpressions.add(newExpr);
          } else {
            result.add(predicate);
          }
        } else {
          result.add(predicate);
        }
      }

      this.visitOperatorByDefault(session, aggregation, pushDownExpressions);

      return Tuples.of(aggregation, result);
    }

    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE")
    @Override
    public Tuple2<RelOperator, List<Expression>> visitOperator(Session session, Selection selection, List<Expression> predicates) {
      // extract the expression to pushDown list and retain list
      List<Expression> predsPushDown = new ArrayList<>();
      List<Expression> predsRetain = new ArrayList<>();
      extractPushDownPredicates(selection.getConditions(), predsPushDown, predsRetain);

      // visit children operators and pass the pushDown predicates through predicatesPushDown
      List<Expression> newPredicates = putLists(predicates, predsPushDown);
      Tuple2<RelOperator, List<Expression>> tuple2 = selection.getChildren()[0].acceptVisitor(session, this, newPredicates);
      List<Expression> remainPredicates = putLists(tuple2.getT2(), predsRetain);

      // process predicates that cannot be pushed down to DS
      if (!remainPredicates.isEmpty()) {
        List<Expression> exprList = OptimizationUtil.propagateConstants(remainPredicates, session);
        // TODO: remove SuppressFBWarnings(REDUNDANT_NULLCHECK) tag after finishing this method
        DualTable dt = OptimizationUtil.exprs2DualTable(session, selection, exprList);
        selection.setConditions(exprList);
        if (null != dt) {
          return Tuples.of(dt, Collections.emptyList());
        }
        return Tuples.of(selection, Collections.emptyList());
      }
      return Tuples.of(tuple2.getT1(), Collections.emptyList());
    }

    private <T> List<T> putLists(List<T>... lists) {
      List<T> newList = new ArrayList<>();
      for (List<T> list : lists) {
        if (list != null && !list.isEmpty()) {
          newList.addAll(list);
        }
      }
      return newList;
    }

    @Override
    public Tuple2<RelOperator, List<Expression>> visitOperator(Session session, Projection projection, List<Expression> predicates) {
      // extract the expression to pushDown list and retain list
      List<Expression> predsPushDown = new ArrayList<>();
      List<Expression> predsRetain = new ArrayList<>();
      for (Expression expression : predicates) {
        Expression newExpr = ExpressionUtil.substituteColumn(
                session, expression, projection.getSchema(), projection.getExpressions());
        if (OptimizationUtil.hasVarSetGet(newExpr)) {
          predsRetain.add(expression);
        } else {
          predsPushDown.add(newExpr);
        }
      }

      Tuple2<RelOperator, List<Expression>> result = this.visitOperatorByDefault(session, projection, predsPushDown);
      result.getT2().addAll(predsRetain);
      return result;
    }

    @Override
    public Tuple2<RelOperator, List<Expression>> visitOperator(Session session, TableSource tableSource, List<Expression> predicates) {
      tableSource.setAllPredicates(predicates);
      tableSource.setPushDownPredicates(predicates);
      return Tuples.of(tableSource, Collections.emptyList());
    }

    @Override
    public Tuple2<RelOperator, List<Expression>> visitOperator(Session session, DualTable operator, List<Expression> predicates) {
      return Tuples.of(operator, predicates);
    }

    @Override
    public Tuple2<RelOperator, List<Expression>> visitOperatorByDefault(Session session, RelOperator operator, List<Expression> predicates) {
      if (operator.hasChildren()) {
        Tuple2<RelOperator, List<Expression>> result = operator.getChildren()[0].acceptVisitor(session, this, predicates);
        addSelection(session, operator, result.getT1(), result.getT2(), 0);
        return Tuples.of(operator, new ArrayList<>());
      }
      return Tuples.of(operator, predicates);
    }

    private void addSelection(Session session, RelOperator operator, RelOperator child, List<Expression> expressions, int childIndex) {
      if (expressions.isEmpty()) {
        operator.getChildren()[childIndex] = child;
        return;
      }
      expressions = OptimizationUtil.propagateConstants(expressions, session);
      DualTable dt = OptimizationUtil.exprs2DualTable(session, child, expressions);
      if (dt != null) {
        operator.getChildren()[childIndex] = dt;
        return;
      }
      Selection selection = new Selection(expressions, child);
      operator.getChildren()[childIndex] = selection;
    }
  }
}
