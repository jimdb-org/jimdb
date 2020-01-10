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
package io.jimdb.core.expression.aggregate;

import io.jimdb.core.Session;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.values.NullValue;
import io.jimdb.core.values.Value;
import io.jimdb.core.expression.aggregate.util.ValueUtil;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings()
public class AggregateMaxMin extends AggregateFunc {

  private static AggFuncExec valueFunction = (session, rowsInGroup, aggregateFunc, partialResult) -> {
    boolean isMax = ((AggregateMaxMin) aggregateFunc).isMax;
    Value result = partialResult.getValue();

    for (ValueAccessor valueAccessor : rowsInGroup) {

      Value value = ValueUtil.exec(session, aggregateFunc.getArgs()[0], valueAccessor, aggregateFunc.getSqlType().getType());

      if (result == null || result.isNull()) {
        partialResult.setValue(value);
        result = value;
        continue;
      }

      int b = value.compareTo(session, result);
      if (isMax && b > 0 || !isMax && b < 0) {
        partialResult.setValue(value);
        result = value;
      }
    }

    return partialResult;
  };

  private boolean isMax;

  public AggregateMaxMin(AggregateExpr aggregateExpr, int ordinal, boolean isMax) {
    super(aggregateExpr.getArgs(), ordinal, aggregateExpr.isHasDistinct(), aggregateExpr.getType());
    this.isMax = isMax;
  }

  @Override
  protected void initCalculateFunc() {
    this.currentFunction = valueFunction;
  }

  @Override
  public Cell initCell() {
    return new CommonCell(NullValue.getInstance());
  }

  @Override
  public Cell calculatePartialResult(Session session, ValueAccessor[] rowsInGroup, Cell partialResult) {
    return currentFunction.apply(session, rowsInGroup, this, partialResult);
  }

  @Override
  public Cell mergePartialResult(Session session, Cell src, Cell dst) {
    Value srcValue = src.getValue();
    Value dstValue = dst.getValue();
    if (srcValue.isNull()) {
      return dst;
    }
    if (dstValue.isNull()) {
      return src;
    }
    boolean b = srcValue.compareTo(session, dstValue) > 0;
    if ((isMax && b) || (!isMax && !b)) {
      dst.setValue(srcValue);
    }
    return dst;
  }

  @Override
  public boolean append2Result(Session session, ValueAccessor finalRow, int index, Cell partialResult) {
    finalRow.set(index, partialResult != null ? partialResult.getValue() : null);
    return true;
  }
}
