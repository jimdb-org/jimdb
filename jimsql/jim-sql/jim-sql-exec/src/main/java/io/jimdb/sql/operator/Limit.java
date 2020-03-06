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
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.model.result.impl.QueryExecResult;
import io.jimdb.sql.optimizer.OperatorVisitor;
import io.jimdb.sql.optimizer.ParameterizedOperatorVisitor;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.core.publisher.Flux;

/**
 * LimitOperator
 *
 */
@SuppressFBWarnings()
public final class Limit extends RelOperator {
  private long offset;
  private long count;

  public Limit(long offset, long count) {
    this.offset = offset;
    this.count = count;
  }

  private Limit(Limit limit) {
    this.offset = limit.offset;
    this.count = limit.count;
    this.copyBaseParameters(limit);
  }

  public long getOffset() {
    return offset;
  }

  public long getCount() {
    return count;
  }

  @Override
  public Limit copy() {
    Limit limit = new Limit(this);
    limit.children = this.copyChildren();

    return limit;
  }

  @Override
  public void resolveOffset() {
    if (hasChildren()) {
      Arrays.stream(children).forEach(Operator::resolveOffset);
    }
  }

  @Override
  public Flux<ExecResult> execute(Session session) throws BaseException {
    if (hasChildren()) {
      return children[0].execute(session).flatMap(execResult -> {
        execResult.truncate((int) offset, (int) count);
        return Flux.just(execResult);
      });
    }
    return Flux.just(new QueryExecResult(schema.getColumns().toArray(new ColumnExpr[schema.getColumns().size()]), new ValueAccessor[0]));
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
    return "Limit";
  }

  @Override
  public String toString() {
    return String.format("Limit(Offset=%d,Count=%d)", this.offset, this.count);
  }
}
