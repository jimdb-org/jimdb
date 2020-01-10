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
package io.jimdb.core.expression;

import java.util.List;

import io.jimdb.core.Session;
import io.jimdb.core.expression.functions.FuncType;
import io.jimdb.core.types.Types;
import io.jimdb.core.values.DateValue;
import io.jimdb.core.values.DecimalValue;
import io.jimdb.core.values.DoubleValue;
import io.jimdb.core.values.JsonValue;
import io.jimdb.core.values.LongValue;
import io.jimdb.core.values.StringValue;
import io.jimdb.core.values.TimeValue;
import io.jimdb.core.values.UnsignedLongValue;
import io.jimdb.core.values.Value;
import io.jimdb.core.values.YearValue;
import io.jimdb.common.exception.JimException;
import io.jimdb.pb.Basepb;
import io.jimdb.pb.Metapb.SQLType;

/**
 * Expression represents a scalar expression for SQL.
 *
 * @version V1.0
 */
public abstract class Expression implements Cloneable {
  protected SQLType resultType;

  public abstract Value exec(ValueAccessor accessor) throws JimException;

  public abstract LongValue execLong(Session session, ValueAccessor accessor) throws JimException;

  public abstract UnsignedLongValue execUnsignedLong(Session session, ValueAccessor accessor) throws JimException;

  public abstract DoubleValue execDouble(Session session, ValueAccessor accessor) throws JimException;

  public abstract DecimalValue execDecimal(Session session, ValueAccessor accessor) throws JimException;

  public abstract StringValue execString(Session session, ValueAccessor accessor) throws JimException;

  public abstract DateValue execDate(Session session, ValueAccessor accessor) throws JimException;

  public abstract TimeValue execTime(Session session, ValueAccessor accessor) throws JimException;

  public abstract YearValue execYear(Session session, ValueAccessor accessor) throws JimException;

  public abstract JsonValue execJson(Session session, ValueAccessor accessor) throws JimException;

  public abstract Expression resolveOffset(Schema schema, boolean isClone) throws JimException;

  public abstract ExpressionType getExprType();

  public List<Point> convertToPoints(Session session) {
    return Points.fullRangePoints();
  }

  // check if the expression (condition) can be pushed into index planner
  public abstract boolean check(ConditionChecker conditionChecker);

  @Override
  public abstract Expression clone();

  @Override
  public boolean equals(Object obj) {
    return super.equals(obj);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public String toString() {
    return super.toString();
  }

  protected void clone(Expression expr) {
    expr.resultType = this.resultType;
  }

  public SQLType getResultType() {
    return resultType;
  }

  public void setResultType(SQLType resultType) {
    this.resultType = resultType;
  }

  public boolean isTempColumn() {
    if (getExprType() != ExpressionType.COLUMN) {
      return false;
    }

    if (!Types.isDateType(resultType) && resultType.getType() != Basepb.DataType.Time) {
      return false;
    }
    return true;
  }

  public boolean isFuncExpr(FuncType funcType) {
    return this.getExprType() == ExpressionType.FUNC && ((FuncExpr) this).getFuncType() == funcType;
  }
}
