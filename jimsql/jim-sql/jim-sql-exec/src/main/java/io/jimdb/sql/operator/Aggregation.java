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
package io.jimdb.sql.operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import io.jimdb.core.Session;
import io.jimdb.common.exception.JimException;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.ExpressionType;
import io.jimdb.core.expression.Schema;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.expression.aggregate.AggregateExpr;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.model.result.impl.QueryExecResult;
import io.jimdb.sql.executor.AggExecutor;
import io.jimdb.sql.executor.AggExecutorBuilder;
import io.jimdb.sql.optimizer.OperatorVisitor;
import io.jimdb.sql.optimizer.ParameterizedOperatorVisitor;
import io.jimdb.sql.optimizer.statistics.OperatorStatsInfo;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.core.publisher.Flux;

/**
 * @version V1.0
 */
@SuppressFBWarnings()
public final class Aggregation extends RelOperator {
  private AggOpType aggOpType;
  private AggregateExpr[] aggregateExprs;
  private Expression[] groupByExprs;
  private ColumnExpr[] groupByColumnExprs;
  private double inputCount;
  private AggExecutor aggExecutor;
  private ColumnExpr[][] properties;
  private Boolean hasDistinct;

  public Aggregation(AggregateExpr[] aggregateExprs, Expression[] groupByExprs, Schema schema,
                     RelOperator... children) {
    this.aggregateExprs = aggregateExprs;
    this.groupByExprs = groupByExprs;
    this.schema = schema;
    this.children = children;
    this.collectGroupByColumns();
  }

  public Aggregation(AggregateExpr[] aggregateExprs, Expression[] groupByExprs, Schema schema, AggOpType aggOpType,
                     OperatorStatsInfo statInfo) {
    this.aggregateExprs = aggregateExprs;
    this.groupByExprs = groupByExprs;
    this.schema = schema;
    this.aggOpType = aggOpType;
    this.statInfo = statInfo;
    this.collectGroupByColumns();
  }

  public Aggregation(Aggregation aggregation) {
    this.aggOpType = aggregation.aggOpType;
    this.aggregateExprs = aggregation.aggregateExprs;
    this.groupByExprs = aggregation.groupByExprs;
    this.groupByColumnExprs = aggregation.groupByColumnExprs;
    this.inputCount = aggregation.inputCount;
    this.copyBaseParameters(aggregation);
  }

  @Override
  public Aggregation copy() {
    Aggregation aggregation = new Aggregation(this);
    aggregation.children = this.copyChildren();

    return aggregation;
  }

  @Override
  public Flux<ExecResult> execute(Session session) throws JimException {
    if (hasChildren()) {
      return children[0].execute(session).flatMap(execResult -> aggExecutor.execute(session, execResult));
    }

    return Flux.just(new QueryExecResult(schema.getColumns().toArray(new ColumnExpr[schema.getColumns().size()]), new ValueAccessor[0]));
  }

  @Override
  public void resolveOffset() {
    if (hasChildren()) {
      Arrays.stream(children).forEach(Operator::resolveOffset);
    }

    if (this.aggregateExprs != null) {
      for (AggregateExpr aggregateExpr : this.aggregateExprs) {
        Expression[] args = aggregateExpr.getArgs();
        if (args != null) {
          for (int i = 0; i < args.length; i++) {
            Expression arg = args[i];
            args[i] = arg.resolveOffset(children[0].getSchema(), true);
          }
        }
      }

      if (this.groupByExprs != null) {
        for (int i = 0; i < this.groupByExprs.length; i++) {
          Expression expr = this.groupByExprs[i];
          groupByExprs[i] = expr.resolveOffset(children[0].getSchema(), true);
        }
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

  public AggregateExpr[] getAggregateExprs() {
    return aggregateExprs;
  }

  public void setAggregateExprs(AggregateExpr[] aggregateExprs) {
    this.aggregateExprs = aggregateExprs;
  }

  public Expression[] getGroupByExprs() {
    return groupByExprs;
  }

  public void setGroupByExprs(Expression[] groupByExprs) {
    this.groupByExprs = groupByExprs;
  }

  public ColumnExpr[] getGroupByColumnExprs() {
    return groupByColumnExprs;
  }

  public void collectGroupByColumns() {
    if (groupByExprs == null) {
      return;
    }
    List<ColumnExpr> columnExprList = new ArrayList<>();
    for (Expression expression : groupByExprs) {
      if (expression.getExprType() == ExpressionType.COLUMN) {
        columnExprList.add((ColumnExpr) expression);
      }
    }
    groupByColumnExprs = columnExprList.toArray(new ColumnExpr[columnExprList.size()]);
  }

  public boolean hasDistinct() {
    if (hasDistinct == null) {
      for (AggregateExpr expr : aggregateExprs) {
        if (expr.isHasDistinct()) {
          hasDistinct = Boolean.TRUE;
          return true;
        }
      }
      hasDistinct = Boolean.FALSE;
    }
    return hasDistinct.booleanValue();
  }

  public AggOpType getAggOpType() {
    return aggOpType;
  }

  public void setAggOpType(AggOpType aggOpType) {
    this.aggOpType = aggOpType;
  }

  public void setInputCount(double inputCount) {
    this.inputCount = inputCount;
  }

  public void buildExecutor(Session session) {
    this.aggExecutor = AggExecutorBuilder.build(session, this);
  }

  @Override
  public ColumnExpr[][] getCandidatesProperties() {
    if (properties != null) {
      return properties;
    }

    if (hasChildren()) {
      ColumnExpr[][] childProperties = this.getChildren()[0].getCandidatesProperties();
      if (groupByExprs == null || groupByExprs.length == 0) {
        this.properties = new ColumnExpr[0][];
      } else {
        this.properties = childProperties;
      }
      return properties;
    }

    return null;
  }

  /**
   * AggOpType Two type of Aggregation RelOperator: stream and hash.
   */
  public enum AggOpType {
    StreamAgg,
    HashAgg
  }

  @Override
  public String getName() {
    return "Aggregation";
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("Aggregation{aggOpType=");
    sb.append(aggOpType);
    sb.append(", aggregateExprs=").append(Arrays.toString(aggregateExprs));
    sb.append(", groupByExprs=").append(Arrays.toString(groupByExprs));
    sb.append(", groupByColumnExprs=").append(Arrays.toString(groupByColumnExprs));
    sb.append(", inputCount=").append(inputCount);
    sb.append('}');
    return sb.toString();
  }
}
