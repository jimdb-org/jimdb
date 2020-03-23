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

import java.math.BigDecimal;

import io.jimdb.core.Session;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.values.BinaryValue;
import io.jimdb.core.values.DateValue;
import io.jimdb.core.values.DecimalValue;
import io.jimdb.core.values.DoubleValue;
import io.jimdb.core.values.JsonValue;
import io.jimdb.core.values.LongValue;
import io.jimdb.core.values.StringValue;
import io.jimdb.core.values.TimeValue;
import io.jimdb.pb.Basepb;
import io.jimdb.pb.Metapb.SQLType;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings()
public abstract class AggregateFunc {

  protected AggFuncExec currentFunction;
  private Expression[] args;
  private int ordinal;
  private SQLType sqlType;
  protected InitCellFunc curInitCellFunc;
  protected boolean hasDistinct;
  protected boolean isOriginal;

  public AggregateFunc(Expression[] args, int ordinal, boolean hasDistinct, SQLType sqlType) {
    this.args = args;
    this.ordinal = ordinal;
    this.sqlType = sqlType;
    this.hasDistinct = hasDistinct;
    initFunc();
  }

  public AggregateFunc(Expression[] args, int ordinal, boolean hasDistinct, SQLType sqlType, boolean isOriginal) {
    this.args = args;
    this.ordinal = ordinal;
    this.sqlType = sqlType;
    this.isOriginal = isOriginal;
    this.hasDistinct = hasDistinct;
    initFunc();
  }

  private void initFunc() {
    initCalculateFunc();
    initCellFunc();
  }

  protected abstract void initCalculateFunc();

  protected void initCellFunc() {
    switch (sqlType.getType()) {
      case TinyInt:
      case SmallInt:
      case MediumInt:
      case Int:
      case Bit:
      case Year:
        curInitCellFunc = initIntegerCellFunc;
        break;
      case Float:
      case Double:
        curInitCellFunc = initDoubleCellFunc;
        break;
      case Varchar:
      case Char:
      case NChar:
      case Text:
        curInitCellFunc = initStringCellFunc;
        break;
      case Binary:
      case VarBinary:
        curInitCellFunc = initBinaryCellFunc;
        break;
      case Date:
      case TimeStamp:
      case DateTime:
        curInitCellFunc = initDataCellFunc;
        break;
      case Time:
        curInitCellFunc = initTimeCellFunc;
        break;
      case Decimal:
      case BigInt:
        curInitCellFunc = initDecimalCellFunc;
        break;
      case Json:
        curInitCellFunc = initJsonCellFunc;
        break;
      default:
        throw new RuntimeException("unknow type:" + sqlType.getType());
    }
  }

  public Cell initCell() {
    return curInitCellFunc.init(sqlType, hasDistinct);
  }

  public Cell calculatePartialResult(Session session, ValueAccessor[] rowsInGroup, Cell partialResult) {
    return null;
  }

  public Cell mergePartialResult(Session session, Cell src, Cell dst) {
    return null;
  }

  public abstract boolean append2Result(Session session, ValueAccessor finalRow, int index, Cell partialResult);

  public Expression[] getArgs() {
    return args;
  }

  public int getOrdinal() {
    return ordinal;
  }

  public SQLType getSqlType() {
    return sqlType;
  }

  /**
   * AggFuncExec
   */
  @FunctionalInterface
  interface AggFuncExec {
    Cell apply(Session session, ValueAccessor[] rows, AggregateFunc aggregateFunc, Cell cell);
  }

  /**
   * InitCellFunc
   */
  @FunctionalInterface
  interface InitCellFunc {
    Cell init(SQLType sqlType, boolean hasDistinct);
  }

  protected static InitCellFunc initIntegerCellFunc = (sqlType, hasDistinct) -> new CommonCell(LongValue.getInstance(0), hasDistinct);
  private static InitCellFunc initStringCellFunc = (sqlType, hasDistinct) -> new CommonCell(StringValue.getInstance(""), hasDistinct);
  private static InitCellFunc initBinaryCellFunc = (sqlType, hasDistinct) -> new CommonCell(BinaryValue.getInstance(new byte[]{}), hasDistinct);
  private static InitCellFunc initDoubleCellFunc = (sqlType, hasDistinct) -> new CommonCell(DoubleValue.getInstance(0.0d), hasDistinct);
  private static InitCellFunc initDecimalCellFunc = (sqlType, hasDistinct) -> new CommonCell(
          DecimalValue.getInstance(BigDecimal.valueOf(0, sqlType.getScale()), (int) sqlType.getPrecision(), sqlType.getScale()), hasDistinct);
  private static InitCellFunc initTimeCellFunc = (sqlType, hasDistinct) -> new CommonCell(TimeValue.getInstance(null), hasDistinct);
  private static InitCellFunc initDataCellFunc = (sqlType, hasDistinct) -> new CommonCell(DateValue.getInstance("", Basepb.DataType.Date), hasDistinct);
  private static InitCellFunc initJsonCellFunc = (sqlType, hasDistinct) -> new CommonCell(JsonValue.getInstance(""), hasDistinct);
}
