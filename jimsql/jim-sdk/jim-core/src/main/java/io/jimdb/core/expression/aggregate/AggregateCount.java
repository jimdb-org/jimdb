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
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.values.LongValue;
import io.jimdb.core.values.Value;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings()
public class AggregateCount extends AggregateFunc {

  private static AggFuncExec valueOriginalFunction = (session, rowsInGroup, aggregateFunc, partialResult) -> {
    long count = 0;
    Expression expression = aggregateFunc.getArgs()[0];
    Set distinctSet = partialResult.getDistinctSet();
    for (ValueAccessor valueAccessor : rowsInGroup) {
      if (valueAccessor.size() == 1 && (valueAccessor.get(0) == null || valueAccessor.get(0).isNull())) {
        continue;
      }
      if (distinctSet != null && !distinctSet.add(exec(expression, valueAccessor))) {
        continue;
      }
      count++;
    }
    return partialResult.plus(session, LongValue.getInstance(count));
  };

  private static AggFuncExec valuePartialFunction = (session, rowsInGroup, aggregateFunc, partialResult) -> {
    long count = 0;
    for (ValueAccessor valueAccessor : rowsInGroup) {
      LongValue value = aggregateFunc.getArgs()[0].execLong(session, valueAccessor);
      if (value == null || value.isNull()) {
        continue;
      }
      count += value.getValue();
    }
    return partialResult.plus(session, LongValue.getInstance(count));
  };

  private static Value exec(Expression expression, ValueAccessor row) {
    if (expression instanceof ColumnExpr) {
      return row.get(((ColumnExpr) expression).getOffset());
    }
    return expression.exec(row);
  }

  public AggregateCount(AggregateExpr aggregateExpr, int ordinal, boolean isOriginal) {
    super(aggregateExpr.getArgs(), ordinal, aggregateExpr.isHasDistinct(), aggregateExpr.getType(), isOriginal);
  }

  @Override
  protected void initCellFunc() {
    curInitCellFunc = AggregateFunc.initIntegerCellFunc;
  }

  @Override
  public void initCalculateFunc() {
    if (isOriginal) {
      currentFunction = valueOriginalFunction;
    } else {
      currentFunction = valuePartialFunction;
    }
  }

  @Override
  public Cell calculatePartialResult(Session session, ValueAccessor[] rowsInGroup, Cell partialResult) {
    return currentFunction.apply(session, rowsInGroup, this, partialResult);
  }

  @Override
  public Cell mergePartialResult(Session session, Cell src, Cell dst) {
    if (src.getDistinctSet() != null && dst.getDistinctSet() != null) {
      dst.getDistinctSet().addAll(src.getDistinctSet());
      dst.setValue(LongValue.getInstance(dst.getDistinctSet().size()));
      return dst;
    }
    return dst.plus(session, src);
  }

  @Override
  public boolean append2Result(Session session, ValueAccessor finalRow, int index, Cell partialResult) {
    finalRow.set(index, partialResult != null ? partialResult.getValue() : null);
    return true;
  }
}

