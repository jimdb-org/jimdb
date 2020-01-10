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

import io.jimdb.core.Session;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.values.Value;
import io.jimdb.core.expression.aggregate.util.ValueUtil;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings()
public class AggregateDistinctValue extends AggregateFunc {

  AggregateDistinctValue(AggregateExpr aggregateExpr, int ordinal, boolean isOrigin) {
    super(aggregateExpr.getArgs(), ordinal, aggregateExpr.isHasDistinct(), aggregateExpr.getType(), isOrigin);
  }

  private static AggFuncExec valueOriginalFunction = (session, rowsInGroup, aggregateFunc, partialResult) -> {
    CommonCell cell = (CommonCell) partialResult;

    if (cell.isFirstValue()) {
      return partialResult;
    }

    for (ValueAccessor valueAccessor : rowsInGroup) {
      if (partialResult.getDistinctSet() != null && !partialResult.getDistinctSet().add(valueAccessor)) {
        continue;
      }

      Value value = ValueUtil.exec(session, aggregateFunc.getArgs()[0], valueAccessor, aggregateFunc.getSqlType().getType());
      cell.setValue(value);
      cell.setFirstValue(true);
    }

    return cell;
  };

  private static AggFuncExec valuePartialFunction = (session, rowsInGroup, aggregateFunc, partialResult) -> {

    CommonCell cell = (CommonCell) partialResult;
    if (cell.isFirstValue()) {
      return partialResult;
    }

    for (ValueAccessor valueAccessor : rowsInGroup) {
      Value value = ValueUtil.exec(session, aggregateFunc.getArgs()[0], valueAccessor, aggregateFunc.getSqlType().getType());
      cell.setValue(value);
      cell.setFirstValue(true);
      break;
    }

    return cell;
  };

  @Override
  public void initCalculateFunc() {
    if (this.isOriginal) {
      this.currentFunction = valueOriginalFunction;
    } else {
      this.currentFunction = valuePartialFunction;
    }
  }

  @Override
  public Cell calculatePartialResult(Session session, ValueAccessor[] rowsInGroup, Cell partialResult) {
    Cell apply = currentFunction.apply(session, rowsInGroup, this, partialResult);
    return apply;
  }

  @Override
  public Cell mergePartialResult(Session session, Cell src, Cell dst) {
    if (src == null || src.getValue().isNull()) {
      return dst;
    }
    if (!((CommonCell) dst).isFirstValue()) {
      dst = src;
    }
    return dst;
  }

  @Override
  public boolean append2Result(Session session, ValueAccessor finalRow, int index, Cell partialResult) {
    if (partialResult != null && ((CommonCell) partialResult).isFirstValue()) {
      finalRow.set(index, partialResult.getValue());
      return true;
    }
    return false;
  }
}
