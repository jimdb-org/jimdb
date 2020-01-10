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
package io.jimdb.core.expression.aggregate;

import java.util.Set;

import io.jimdb.core.Session;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.values.Value;
import io.jimdb.core.expression.aggregate.util.ValueUtil;
import io.jimdb.pb.Basepb;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings()
public class AggregateSum extends AggregateFunc {

  public AggregateSum(AggregateExpr aggregateExpr, int ordinal, boolean isOriginal) {
    super(aggregateExpr.getArgs(), ordinal, aggregateExpr.isHasDistinct(), aggregateExpr.getType(), isOriginal);
  }

  private static AggFuncExec valueOriginalFunction = (session, rowsInGroup, aggregateFunc, partialResult) -> {

    Value value;
    Set distinctSet = partialResult.getDistinctSet();
    Basepb.DataType type = aggregateFunc.getSqlType().getType();
    Expression expression = aggregateFunc.getArgs()[0];
    for (ValueAccessor valueAccessor : rowsInGroup) {

      value = ValueUtil.exec(session, expression, valueAccessor, type);

      if (distinctSet != null && !distinctSet.add(value)) {
        continue;
      }

      partialResult.plus(session, value);
    }

    return partialResult;
  };

  private static AggFuncExec valuePartialFunction = (session, rowsInGroup, aggregateFunc, partialResult) -> {

    Value value;
    Basepb.DataType type = aggregateFunc.getSqlType().getType();
    Expression expression = aggregateFunc.getArgs()[0];
    for (ValueAccessor valueAccessor : rowsInGroup) {

      value = ValueUtil.exec(session, expression, valueAccessor, type);

      partialResult.plus(session, value);
    }

    return partialResult;
  };

  @Override
  protected void initCalculateFunc() {
    if (isOriginal) {
      this.currentFunction = valueOriginalFunction;
    } else {
      this.currentFunction = valuePartialFunction;
    }
  }

  @Override
  public Cell calculatePartialResult(Session session, ValueAccessor[] rowsInGroup, Cell partialResult) {
    return currentFunction.apply(session, rowsInGroup, this, partialResult);
  }

  @Override
  public Cell mergePartialResult(Session session, Cell src, Cell dst) {
    return dst.plus(session, src);
  }

  @Override
  public boolean append2Result(Session session, ValueAccessor finalRow, int index, Cell partialResult) {
    finalRow.set(index, partialResult != null ? partialResult.getValue() : null);
    return true;
  }
}
