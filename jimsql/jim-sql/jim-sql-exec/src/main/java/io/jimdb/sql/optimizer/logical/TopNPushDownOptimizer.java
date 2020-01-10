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
import io.jimdb.sql.operator.DualTable;
import io.jimdb.sql.operator.Limit;
import io.jimdb.sql.operator.RelOperator;
import io.jimdb.sql.operator.Order;
import io.jimdb.sql.operator.Projection;
import io.jimdb.sql.operator.TopN;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * TopN push-down optimization pushes the "top n" query close to the data source.
 * A "top n" query usually contains an "order by" clause followed by a "limit" clause.
 */
@SuppressFBWarnings("CFS_CONFUSING_FUNCTION_SEMANTICS")
public class TopNPushDownOptimizer implements IRuleOptimizer {

  @Override
  public RelOperator optimize(RelOperator relOperator, Session session) {
    return relOperator.acceptVisitor(new TopNPushDownVisitor(session));
  }

  /**
   * Visitor for TopN push-down optimization
   */
  private static class TopNPushDownVisitor extends LogicalOptimizationVisitor {
    private TopN topN = null;
    private Session session;

    TopNPushDownVisitor(Session session) {
      this.session = session;
    }

    @Override
    public RelOperator visitChildren(RelOperator operator) {
      TopN oldTopN = this.topN;
      RelOperator[] children = operator.getChildren();
      if (null != children && children.length > 0) {
        for (int i = 0; i < children.length; i++) {
          this.topN = null;
          operator.setChild(i, children[i].acceptVisitor(this));
        }
      }

      if (oldTopN != null) {
        return setChildForTopN(oldTopN, operator);
      }

      return operator;
    }

    @Override
    public RelOperator visitOperator(Limit limit) {
      TopN oldTopN = this.topN;
      this.topN = new TopN(limit.getOffset(), limit.getCount());
      RelOperator child = limit.getChildren()[0].acceptVisitor(this);

      if (oldTopN != null) {
        return setChildForTopN(oldTopN, child);
      }

      return child;
    }

    @Override
    public RelOperator visitOperator(Order order) {
      if (topN == null) {
        return visitChildren(order);

      } else if (topN.isLimit()) {
        topN.setOrderExpression(order.getOrderExpressions());
      }

      return order.getChildren()[0].acceptVisitor(this);
    }

    @Override
    public RelOperator visitOperator(Projection projection) {
      if (topN != null) {

        // substitute columns in TopN's expression columns
        topN.substituteColumns(this.session, projection.getSchema(), projection.getExpressions());

        // remove constant type items in the expressions
        topN.removeConstantExpressions();
      }

      RelOperator newChild = projection.getChildren()[0].acceptVisitor(this);
      projection.setChild(0, newChild);

      return projection;
    }

    private RelOperator setChildForTopN(TopN topN, RelOperator child) {

      // remove this TopN if the child is a DualTable
      if (child instanceof DualTable) {
        DualTable dualTable = (DualTable) child;

        int rowCount = dualTable.getRowCount();

        if (rowCount < topN.getOffset()) {
          dualTable.setRowCount(0);
          return dualTable;
        }

        dualTable.setRowCount((int) Math.min(rowCount - topN.getOffset(), topN.getCount()));

        return dualTable;
      }

      if (topN.isLimit()) {
        Limit limit = new Limit(topN.getOffset(), topN.getCount());
        limit.setChildren(child);
        return limit;
      }

      topN.setChildren(child);
      return topN;
    }
  }
}
