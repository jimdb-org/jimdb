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

package io.jimdb.sql.operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import io.jimdb.core.Session;
import io.jimdb.common.exception.JimException;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.ExpressionType;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.model.result.impl.QueryExecResult;
import io.jimdb.sql.optimizer.OperatorVisitor;
import io.jimdb.sql.optimizer.ParameterizedOperatorVisitor;

import com.alibaba.druid.sql.ast.SQLOrderingSpecification;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.core.publisher.Flux;

/**
 * OrderOperator
 *
 * @since 2019-07-25
 */
@SuppressFBWarnings({ "EI_EXPOSE_REP", "CLI_CONSTANT_LIST_INDEX", "FCCD_FIND_CLASS_CIRCULAR_DEPENDENCY"})
public class Order extends RelOperator {

  private OrderExpression[] orderExpressions;

  public Order(OrderExpression[] orderExpressions, RelOperator... children) {
    this.orderExpressions = orderExpressions;
    this.children = children;
  }

  private Order(Order order) {
    this.orderExpressions = order.orderExpressions;
    this.copyBaseParameters(order);
  }

  public OrderExpression[] getOrderExpressions() {
    return orderExpressions;
  }

  public void setOrderExpressions(OrderExpression[] orderExpressions) {
    this.orderExpressions = orderExpressions;
  }

  @Override
  public Order copy() {
    Order order = new Order(this);
    order.children = this.copyChildren();

    return order;
  }

  /**
   * Order by Multiple
   */
  @Override
  public Flux<ExecResult> execute(Session session) throws JimException {
    if (hasChildren()) {
      return children[0].execute(session).map(execResult -> {
        OperatorUtil.orderBy(session, execResult, orderExpressions);
        return execResult;
      });
    }
    return Flux.just(new QueryExecResult(schema.getColumns().toArray(new ColumnExpr[schema.getColumns().size()]), new ValueAccessor[0]));
  }

  @Override
  public ColumnExpr[][] getCandidatesProperties() {
    ColumnExpr[] columnExprs = getCandidatesPropertiesFromOrderExprs(this.orderExpressions);
    if (columnExprs == null || columnExprs.length == 0) {
      return null;
    }
    return new ColumnExpr[][]{columnExprs};
  }


  public static ColumnExpr[] getCandidatesPropertiesFromOrderExprs(OrderExpression[] orderExprs) {
    List<ColumnExpr> columnExprs = new ArrayList<>(orderExprs.length);
    for (int i = 0; i < orderExprs.length; i++) {
      Expression expr = orderExprs[i].getExpression();
      if (expr.getExprType() == ExpressionType.COLUMN) {
        columnExprs.add((ColumnExpr) expr);
      } else {
        break;
      }
    }
    return columnExprs.toArray(new ColumnExpr[columnExprs.size()]);
  }

  @Override
  public void resolveOffset() {
    if (hasChildren()) {
      Arrays.stream(children).forEach(Operator::resolveOffset);
    }

    for (OrderExpression orderExpression : this.orderExpressions) {
      Expression expression = orderExpression.getExpression();
      orderExpression.setExpression(expression.resolveOffset(children[0].getSchema(), true));
    }
  }

  public <T, R> R acceptVisitor(Session session, ParameterizedOperatorVisitor<T, R> visitor, T t) {
    return visitor.visitOperator(session, this, t);
  }

  @Override
  public <R> R acceptVisitor(OperatorVisitor<R> visitor) {
    return visitor.visitOperator(this);
  }

  @Override
  public String getName() {
    return "Order";
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("Order{info=");
    sb.append(Arrays.toString(orderExpressions)).append('}');
    return sb.toString();
  }

  /**
   * Expression in the order operator
   */
  public static class OrderExpression {
    private Expression expression;
    private SQLOrderingSpecification orderType;

    public OrderExpression(Expression expression, SQLOrderingSpecification orderType) {
      this.expression = expression;
      this.orderType = orderType;
    }

    public Expression getExpression() {
      return expression;
    }

    public void setExpression(Expression expression) {
      this.expression = expression;
    }

    public boolean getOrderType() {
      return orderType != SQLOrderingSpecification.DESC;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append('(').append(expression).append(',').append(orderType == SQLOrderingSpecification.DESC ? "DESC" : "ASC").append(')');
      return sb.toString();
    }
  }
}
