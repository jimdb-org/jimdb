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

import static io.jimdb.core.expression.ExpressionType.CONST;
import static io.jimdb.core.expression.ExpressionUtil.substituteColumn;

import java.util.Arrays;

import io.jimdb.core.Session;
import io.jimdb.common.exception.JimException;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.Schema;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.model.result.impl.QueryExecResult;
import io.jimdb.sql.optimizer.statistics.OperatorStatsInfo;
import io.jimdb.sql.optimizer.OperatorVisitor;
import io.jimdb.sql.optimizer.ParameterizedOperatorVisitor;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.core.publisher.Flux;

/**
 * TopN operator is an internal operator generated during TopN push-down optimization.
 */
@SuppressFBWarnings()
public class TopN extends RelOperator {
  private long offset;
  private long count;
  private Order.OrderExpression[] orderExpressions;

  public TopN(long offset, long count) {
    this.offset = offset;
    this.count = count;
    this.orderExpressions = null;
  }

  public TopN(long offset, long count, Order.OrderExpression[] orderExpressions, OperatorStatsInfo statInfo) {
    this.offset = offset;
    this.count = count;
    this.orderExpressions = orderExpressions;
    this.statInfo = statInfo;
  }

  private TopN(TopN topN) {
    this.offset = topN.offset;
    this.count = topN.count;

    // TODO do we need to have a copy of the orderExpressions?
    this.orderExpressions = topN.orderExpressions;
    this.copyBaseParameters(topN);
  }

  public long getOffset() {
    return offset;
  }

  public long getCount() {
    return count;
  }

  @Override
  public TopN copy() {
    TopN topN = new TopN(this);
    topN.children = this.copyChildren();

    return topN;
  }

  @Override
  public Flux<ExecResult> execute(Session session) throws JimException {
    if (hasChildren()) {
      return children[0].execute(session).flatMap(execResult -> {
        OperatorUtil.orderBy(session, execResult, orderExpressions);
        execResult.truncate((int) offset, (int) count);
        return Flux.just(execResult);
      });
    }
    return Flux.just(new QueryExecResult(schema.getColumns().toArray(new ColumnExpr[schema.getColumns().size()]), new ValueAccessor[0]));
  }

  @Override
  public ColumnExpr[][] getCandidatesProperties() {
    ColumnExpr[] columnExprs = Order.getCandidatesPropertiesFromOrderExprs(this.orderExpressions);
    if (columnExprs == null || columnExprs.length == 0) {
      return null;
    }
    return new ColumnExpr[][]{columnExprs};
  }

  @Override
  public void resolveOffset() {
    if (hasChildren()) {
      Arrays.stream(children).forEach(Operator::resolveOffset);
    }

    if (this.orderExpressions != null) {
      for (Order.OrderExpression orderExpression : this.orderExpressions) {
        Expression expression = orderExpression.getExpression();
        orderExpression.setExpression(expression.resolveOffset(children[0].getSchema(), true));
      }
    }
  }

  public <T, R> R acceptVisitor(Session session, ParameterizedOperatorVisitor<T, R> visitor, T t) {
    return visitor.visitOperator(session, this, t);
  }

  @Override
  public <R> R acceptVisitor(OperatorVisitor<R> visitor) {
    return visitor.visitOperator(this);
  }

  public void setOrderExpression(final Order.OrderExpression[] orderExpressions) {
    this.orderExpressions = Arrays.copyOf(orderExpressions, orderExpressions.length);
  }

  public boolean isLimit() {
    return null == orderExpressions || orderExpressions.length == 0;
  }

  public void removeConstantExpressions() {
    if (orderExpressions == null) {
      return;
    }

    this.orderExpressions = Arrays.stream(orderExpressions)
            .filter(expr -> expr.getExpression().getExprType() != CONST)
            .toArray(Order.OrderExpression[]::new);
  }

  public void substituteColumns(Session session, Schema schema, Expression[] newExpressions) {
    if (this.orderExpressions == null) {
      return;
    }
    for (Order.OrderExpression orderExpression : orderExpressions) {
      Expression expression = orderExpression.getExpression();
      orderExpression.setExpression(substituteColumn(session, expression, schema, newExpressions));
    }
  }

  public Order.OrderExpression[] getOrderExpressions() {
    return orderExpressions;
  }

  @Override
  public String getName() {
    return "TopN";
  }

  @Override
  public String toString() {
    String exprs;
    if (this.orderExpressions != null) {
      if (this.orderExpressions.length > 1) {
        final StringBuilder stringBuilder = new StringBuilder("(");
        Arrays.stream(this.orderExpressions).forEach(stringBuilder::append);
        exprs = stringBuilder.append(')').toString();
      } else {
        exprs = this.orderExpressions[0].toString();
      }
    } else {
      exprs = "NULL";
    }
    return String.format("TopN(Exprs=%s,Offset=%d,Count=%d)", exprs, this.offset, this.count);
  }
}
