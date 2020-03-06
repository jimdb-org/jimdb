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

import java.util.Arrays;

import io.jimdb.core.Session;
import io.jimdb.common.exception.BaseException;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.RowValueAccessor;
import io.jimdb.core.expression.Schema;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.model.result.impl.QueryExecResult;
import io.jimdb.sql.optimizer.OperatorVisitor;
import io.jimdb.sql.optimizer.ParameterizedOperatorVisitor;
import io.jimdb.core.values.Value;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.core.publisher.Flux;

/**
 * @version V1.0
 */
@SuppressFBWarnings({ "EI_EXPOSE_REP", "FCBL_FIELD_COULD_BE_LOCAL", "UUF_UNUSED_FIELD", "CLI_CONSTANT_LIST_INDEX" })
public final class Projection extends RelOperator {

  private Expression[] expressions;

  public Projection(Expression[] expressions, Schema schema, RelOperator... children) {
    this.expressions = expressions;
    this.schema = schema;
    this.children = children;
  }

  private Projection(Projection projection) {
    this.expressions = projection.expressions;
    this.copyBaseParameters(projection);
  }

  @Override
  public Projection copy() {
    Projection projection = new Projection(this);
    projection.children = this.copyChildren();

    return projection;
  }

  public Expression[] getExpressions() {
    return expressions;
  }

  public void setExpressions(Expression[] expressions) {
    this.expressions = expressions;
  }

  @Override
  public Flux<ExecResult> execute(Session session) throws BaseException {
    int colsSize = schema.getColumns().size();
    if (hasChildren()) {
      return children[0].execute(session).map(r -> {
        //Get all columns
        final int rowColSize = r.getColumns().length;
        int exprsLen = expressions.length;
        int[] index = new int[]{ 0 };
        RowValueAccessor[] rows = new RowValueAccessor[r.size()];
        r.forEach(row -> {
          //Construct the new row according to the projection expression
          int size = row.size() > exprsLen ? exprsLen + 1 : exprsLen;
          Value[] values = new Value[size];
          for (int i = 0; i < exprsLen; i++) {
            values[i] = expressions[i].exec(row);
          }
          //The last field is "version", except aggregation, get row version
          if (size > exprsLen && rowColSize < row.size()) {
            values[exprsLen] = row.get(row.size() - 1);
          }
          rows[index[0]++] = new RowValueAccessor(values);
        });
        return new QueryExecResult(schema.getColumns().toArray(new ColumnExpr[colsSize]), rows);
      });
    }
    return Flux.just(new QueryExecResult(schema.getColumns().toArray(new ColumnExpr[colsSize]), new ValueAccessor[0]));
  }

  @Override
  public void resolveOffset() {
    if (hasChildren()) {
      Arrays.stream(children).forEach(Operator::resolveOffset);
    }

    Expression[] expressions = this.expressions;
    for (int i = 0; i < expressions.length; i++) {
      Expression expression = expressions[i];
      expressions[i] = expression.resolveOffset(children[0].getSchema(), true);
    }
  }

  @Override
  public ColumnExpr[][] getCandidatesProperties() {
//    TODO
    return this.getChildren()[0].getCandidatesProperties();
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
    return "Projection";
  }

  @Override
  @SuppressFBWarnings({ "ITC_INHERITANCE_TYPE_CHECKING", "UCPM_USE_CHARACTER_PARAMETERIZED_METHOD" })
  public String toString() {
    StringBuilder sb = new StringBuilder("Projection={");
    sb.append(export(expressions));
    sb.append('}');
    return sb.toString();
  }
}
