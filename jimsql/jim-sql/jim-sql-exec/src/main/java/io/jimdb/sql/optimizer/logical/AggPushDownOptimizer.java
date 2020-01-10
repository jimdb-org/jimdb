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

import io.jimdb.core.Session;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.ExpressionUtil;
import io.jimdb.core.expression.aggregate.AggregateExpr;
import io.jimdb.sql.operator.Aggregation;
import io.jimdb.sql.operator.Projection;
import io.jimdb.sql.operator.RelOperator;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Predicates push down optimizer extracts all the predicates that can be pushed down
 * to data server.
 */
@SuppressFBWarnings("CFS_CONFUSING_FUNCTION_SEMANTICS")
public class AggPushDownOptimizer implements IRuleOptimizer {

  @SuppressFBWarnings({ "DLS_DEAD_LOCAL_STORE" })
  @Override
  public RelOperator optimize(RelOperator relOperator, Session session) {
    return relOperator.acceptVisitor(new AggPushDownVisitor(session));
  }

  /**
   * Implementation class for aggregation push down visitor.
   */
  private static class AggPushDownVisitor extends LogicalOptimizationVisitor {

    private Session session;

    AggPushDownVisitor(Session session) {
      this.session = session;
    }

    // override this method to allow logical optimization when root operator is not Aggregation
    public RelOperator visitOperator(RelOperator operator) {
      RelOperator[] children = operator.getChildren();
      int i = 0;
      if (null != children && children.length > 0) {
        RelOperator[] newChildren = new RelOperator[children.length];
        for (RelOperator childOperator : children) {
          RelOperator newChild = childOperator.acceptVisitor(this);
          children[i++] = newChild;
        }
        operator.setChildren(newChildren);
      }
      return operator;
    }

    @Override
    public RelOperator visitOperator(Aggregation aggregation) {
      Projection projection = AggEliminateOptimizer.AggEliminator.tryToEliminateAgg(session, aggregation);
      if (null == projection) {
        RelOperator[] children = aggregation.getChildren();
        if (null != children && children.length > 0) {
          RelOperator child = children[0];
          if (child instanceof Projection) {
            Projection childProj = (Projection) child;
            Expression[] groupByExprs = aggregation.getGroupByExprs();
            Expression[] newExprs = new Expression[groupByExprs.length];
            int i = 0;
            for (Expression grbyExpr : groupByExprs) {
              Expression newExpr =
                      ExpressionUtil.substituteColumn(
                              this.session, grbyExpr, childProj.getSchema(), childProj.getExpressions());
              newExprs[i++] = newExpr;
            }
            aggregation.setGroupByExprs(newExprs);

            aggregation.collectGroupByColumns();
            AggregateExpr[] aggregateExprs = aggregation.getAggregateExprs();
            for (AggregateExpr aggExpr : aggregateExprs) {
              Expression[] argExprs = aggExpr.getArgs();
              Expression[] newArgExprs = new Expression[argExprs.length];
              int j = 0;
              for (Expression arg : argExprs) {
                Expression newArg = ExpressionUtil.substituteColumn(
                        this.session, arg, childProj.getSchema(), childProj.getExpressions());
                newArgExprs[j++] = newArg;
              }
              aggExpr.setArgs(newArgExprs);
              aggregation.setChildren(child.getChildren()[0]);
            }
          }
        }
      }

      return visitOperator((RelOperator) aggregation);
    }
  }
}
