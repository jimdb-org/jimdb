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

import java.util.Collections;
import java.util.List;

import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.exception.JimException;
import io.jimdb.core.Session;
import io.jimdb.core.types.Types;
import io.jimdb.core.values.DateValue;
import io.jimdb.core.values.DecimalValue;
import io.jimdb.core.values.DoubleValue;
import io.jimdb.core.values.JsonValue;
import io.jimdb.core.values.LongValue;
import io.jimdb.core.values.NullValue;
import io.jimdb.core.values.StringValue;
import io.jimdb.core.values.TimeValue;
import io.jimdb.core.values.UnsignedLongValue;
import io.jimdb.core.values.Value;
import io.jimdb.core.values.ValueConvertor;
import io.jimdb.core.values.YearValue;
import io.jimdb.pb.Basepb.DataType;
import io.jimdb.pb.Metapb.SQLType;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings("FCCD_FIND_CLASS_CIRCULAR_DEPENDENCY")
public final class ValueExpr extends Expression {
  public static final ValueExpr ZERO = new ValueExpr(LongValue.getInstance(0), Types.buildSQLType(DataType.TinyInt));
  public static final ValueExpr ONE = new ValueExpr(LongValue.getInstance(1), Types.buildSQLType(DataType.TinyInt));
  public static final ValueExpr EMPTY = new ValueExpr(StringValue.getInstance(null), Types.buildSQLType(DataType.Varchar));
  public static final ValueExpr NULL = new ValueExpr(NullValue.getInstance(), Types.buildSQLType(DataType.TinyInt));

  private Value value = null;
  private Expression lazyExpr = null;

  public ValueExpr() {
  }

  public ValueExpr(final Value value, final SQLType resultType) {
    this.value = value;
    this.resultType = resultType;
  }

  public ValueExpr(final Value value, final SQLType resultType, final Expression lazyExpr) {
    this.value = value;
    this.resultType = resultType;
    this.lazyExpr = lazyExpr;
  }

  public void setLazyExpr(Expression lazyExpr) {
    this.lazyExpr = lazyExpr;
  }

  public Expression getLazyExpr() {
    return lazyExpr;
  }

  public void setValue(Value value) {
    this.value = value;
  }

  public Value getValue() {
    return value;
  }

  public boolean isNull() {
    return value == null || value.isNull();
  }

  @Override
  public ExpressionType getExprType() {
    return ExpressionType.CONST;
  }

  @Override
  public List<Point> convertToPoints(Session session) {
    Value value = this.exec(ValueAccessor.EMPTY);
    if (value.isNull()) {
      return Collections.emptyList();
    }

    boolean boolValue = ValueConvertor.convertToBool(session, value);
    if (!boolValue) {
      return Collections.emptyList();
    }

    return Points.fullRangePoints();
  }

  @Override
  public boolean check(ConditionChecker conditionChecker) {
    return true;
  }

  @Override
  public ValueExpr clone() {
    if (lazyExpr != null) {
      ValueExpr result = new ValueExpr(this.value, this.resultType);
      result.lazyExpr = this.lazyExpr;
      clone(result);
      return result;
    }
    return this;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof ValueExpr)) {
      return false;
    }
    ValueExpr otherVal = (ValueExpr) obj;
    try {
      this.exec(ValueAccessor.EMPTY);
    } catch (Throwable e) {
      return false;
    }
    try {
      otherVal.exec(ValueAccessor.EMPTY);
    } catch (Throwable e) {
      return false;
    }
    // TODO: arg session is null (though it's never used inside)
    int compare = this.value.compareTo(null, otherVal.value);
    if (compare != 0) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = value != null ? value.hashCode() : 0;
    return 31 * result + (lazyExpr != null ? lazyExpr.hashCode() : 0);
  }

  @Override
  public Expression resolveOffset(Schema schema, boolean isClone) throws JimException {
    return this;
  }

  @Override
  public Value exec(ValueAccessor accessor) throws JimException {
    if (lazyExpr != null && lazyExpr.getExprType() == ExpressionType.FUNC) {
      final FuncExpr funcExpr = (FuncExpr) lazyExpr;
      final Value v = funcExpr.exec(accessor);
      if (v.isNull()) {
        value = v;
      } else {
        value = ValueConvertor.convertType(funcExpr.getSession(), v, resultType);
      }
    }
    return value;
  }

  @Override
  public LongValue execLong(Session session, ValueAccessor accessor) throws JimException {
    if (lazyExpr != null) {
      Value v = lazyExpr.exec(accessor);
      if (v.isNull()) {
        return null;
      }
      value = ValueConvertor.convertToLong(session, v, null);
      return (LongValue) value;
    } else if (resultType.getType() == DataType.Null || value.isNull()) {
      return null;
    }
    return ValueConvertor.convertToLong(session, value, null);
  }

  @Override
  public UnsignedLongValue execUnsignedLong(Session session, ValueAccessor accessor) throws JimException {
    if (lazyExpr != null) {
      Value v = lazyExpr.exec(accessor);
      if (v.isNull()) {
        return null;
      }
      value = ValueConvertor.convertToUnsignedLong(session, v, null);
      return (UnsignedLongValue) value;
    } else if (resultType.getType() == DataType.Null || value.isNull()) {
      return null;
    }
    return ValueConvertor.convertToUnsignedLong(session, value, null);
  }

  @Override
  public DoubleValue execDouble(Session session, ValueAccessor accessor) throws JimException {
    if (lazyExpr != null) {
      Value v = lazyExpr.exec(accessor);
      if (v.isNull()) {
        return null;
      }

      value = ValueConvertor.convertToDouble(session, v, null);
      return (DoubleValue) value;
    } else if (resultType.getType() == DataType.Null || value.isNull()) {
      return null;
    }

    return ValueConvertor.convertToDouble(session, value, null);
  }

  @Override
  public DecimalValue execDecimal(Session session, ValueAccessor accessor) throws JimException {
    if (lazyExpr != null) {
      Value v = lazyExpr.exec(accessor);
      if (v.isNull()) {
        return null;
      }

      value = ValueConvertor.convertToDecimal(session, v, null);
      return (DecimalValue) value;
    } else if (resultType.getType() == DataType.Null || value.isNull()) {
      return null;
    }

    return ValueConvertor.convertToDecimal(session, value, null);
  }

  @Override
  public StringValue execString(Session session, ValueAccessor accessor) throws JimException {
    if (lazyExpr != null) {
      Value v = lazyExpr.exec(accessor);
      if (v.isNull()) {
        return null;
      }

      value = ValueConvertor.convertToString(session, v, null);
      return (StringValue) value;
    } else if (resultType.getType() == DataType.Null || value.isNull()) {
      return null;
    }

    return ValueConvertor.convertToString(session, value, null);
  }

  @Override
  public DateValue execDate(Session session, ValueAccessor accessor) throws JimException {
    if (lazyExpr != null) {
      Value v = lazyExpr.exec(accessor);
      if (v.isNull()) {
        return null;
      }

      value = ValueConvertor.convertToDate(session, v, null);
      return (DateValue) value;
    } else if (resultType.getType() == DataType.Null || value.isNull()) {
      return null;
    }

    return ValueConvertor.convertToDate(session, value, null);
  }

  @Override
  public TimeValue execTime(Session session, ValueAccessor accessor) throws JimException {
    if (lazyExpr != null) {
      Value v = lazyExpr.exec(accessor);
      if (v.isNull()) {
        return null;
      }

      value = ValueConvertor.convertToTime(session, v, null);
      return (TimeValue) value;
    } else if (resultType.getType() == DataType.Null || value.isNull()) {
      return null;
    }

    return ValueConvertor.convertToTime(session, value, null);
  }

  @Override
  public YearValue execYear(Session session, ValueAccessor accessor) throws JimException {
    if (lazyExpr != null) {
      Value v = lazyExpr.exec(accessor);
      if (v.isNull()) {
        return null;
      }

      value = ValueConvertor.convertToYear(session, v, null);
      return (YearValue) value;
    } else if (resultType.getType() == DataType.Null || value.isNull()) {
      return null;
    }

    return ValueConvertor.convertToYear(session, value, null);
  }

  @Override
  public JsonValue execJson(Session session, ValueAccessor accessor) throws JimException {
    throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_NOT_SUPPORTED_YET, "Json");
  }

  @Override
  public String toString() {
    if (this.lazyExpr != null) {
      try {
        this.value = this.exec(ValueAccessor.EMPTY);
      } catch (JimException ignored) {
      }
    }
    return this.value.getString();
  }
}
