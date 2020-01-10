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

import java.math.BigDecimal;

import io.jimdb.core.Session;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.values.DecimalValue;
import io.jimdb.core.values.DoubleValue;
import io.jimdb.core.values.LongValue;
import io.jimdb.core.values.Value;
import io.jimdb.core.values.ValueConvertor;
import io.jimdb.core.expression.aggregate.util.ValueUtil;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings({ "CFS_CONFUSING_FUNCTION_SEMANTICS", "CLI_CONSTANT_LIST_INDEX",
        "PRMC_POSSIBLY_REDUNDANT_METHOD_CALLS", "UR_UNINIT_READ_CALLED_FROM_SUPER_CONSTRUCTOR" })
public class AggregateAvg extends AggregateFunc {

  private static AggFuncExec valueOriginalFunction = (session, rowsInGroup, aggregateFunc, partialResult) -> {

    AvgCell cell = (AvgCell) partialResult;
    Value countValue = cell.getCount();
    Value sumValue = cell.getSum();

    for (ValueAccessor valueAccessor : rowsInGroup) {
      Value value = ValueUtil.exec(session, aggregateFunc.getArgs()[0], valueAccessor,
              aggregateFunc.getArgs()[0].getResultType().getType());
      if (value == null || value.isNull()) {
        continue;
      }

      if (partialResult.getDistinctSet() != null && !partialResult.getDistinctSet().add(value)) {
        continue;
      }

      sumValue = sumValue.plus(session, value);
      countValue = countValue.plus(session, LongValue.getInstance(1));

      cell.setSum(sumValue);
      cell.setCount(countValue);
    }

    return cell;
  };

  private static AggFuncExec valuePartialFunction = (session, rowsInGroup, aggregateFunc, partialResult) -> {
    AvgCell cell = (AvgCell) partialResult;
    Value countValue = cell.getCount();
    Value sumValue = cell.getSum();
    for (ValueAccessor row : rowsInGroup) {
      Value inputSum = aggregateFunc.getArgs()[0].exec(row);
      if (inputSum == null || inputSum.isNull()) {
        continue;
      }
      Value inputCount = aggregateFunc.getArgs()[1].exec(row);
      if (inputCount == null || inputCount.isNull()) {
        continue;
      }

      countValue = countValue.plus(session, inputCount);
      sumValue = sumValue.plus(session, inputSum);
    }

    cell.setSum(sumValue);
    cell.setCount(countValue);
    return cell;
  };

  public AggregateAvg(AggregateExpr aggregateExpr, int ordinal, boolean isOriginal) {
    super(aggregateExpr.getArgs(), ordinal, aggregateExpr.isHasDistinct(), aggregateExpr.getType(), isOriginal);
  }

  @Override
  protected void initCalculateFunc() {
    if (isOriginal || hasDistinct) {
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
  public boolean append2Result(Session session, ValueAccessor finalRow, int index, Cell partialResult) {
    AvgCell cell = (AvgCell) partialResult;
    Value count = cell.getCount();

    if (count.isNull()) {
      finalRow.set(index, null);
    } else {
      Value sum = cell.getSum();
      if (count.getType() != sum.getType()) {
        count = ValueConvertor.convertType(session, count, sum.getType(), getSqlType());
      }
      if (ValueConvertor.convertToDouble(session, count, null).getValue() == 0) {
        finalRow.set(index, DoubleValue.getInstance(0));
      } else {
        finalRow.set(index, sum.divide(session, count));
      }
    }
    return true;
  }

  @Override
  protected void initCellFunc() {
    switch (getSqlType().getType()) {
      case TinyInt:
      case SmallInt:
      case MediumInt:
      case Int:
      case BigInt:
        curInitCellFunc = initLongCellFunc;
        break;
      case Decimal:
        curInitCellFunc = initDecimalCellFunc;
        break;
      default:
        curInitCellFunc = initDoubleCellFunc;
    }
  }

  @Override
  public Cell mergePartialResult(Session session, Cell src, Cell dst) {
    return dst.plus(session, src);
  }

  private static InitCellFunc initLongCellFunc = (sqlType, hasDistinct) -> new AvgCell(LongValue.getInstance(0),
          DecimalValue.getInstance(BigDecimal.ZERO), hasDistinct);
  private static InitCellFunc initDecimalCellFunc = (sqlType, hasDistinct) -> new AvgCell(LongValue.getInstance(0),
          DecimalValue.getInstance(BigDecimal.valueOf(0, sqlType.getScale()), sqlType.getPrecision(), sqlType.getScale()), hasDistinct);
  private static InitCellFunc initDoubleCellFunc = (sqlType, hasDistinct) -> new AvgCell(LongValue.getInstance(0),
          DoubleValue.getInstance(0.0d), hasDistinct);
}
