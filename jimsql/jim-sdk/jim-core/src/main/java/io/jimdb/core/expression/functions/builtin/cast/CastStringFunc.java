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
package io.jimdb.core.expression.functions.builtin.cast;

import io.jimdb.common.exception.JimException;
import io.jimdb.core.Session;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.expression.functions.Func;
import io.jimdb.core.values.DateValue;
import io.jimdb.core.values.DecimalValue;
import io.jimdb.core.values.DoubleValue;
import io.jimdb.core.values.LongValue;
import io.jimdb.core.values.StringValue;
import io.jimdb.core.values.TimeValue;
import io.jimdb.core.values.UnsignedLongValue;
import io.jimdb.core.values.Value;
import io.jimdb.core.values.ValueConvertor;
import io.jimdb.core.values.YearValue;
import io.jimdb.pb.Exprpb;
import io.jimdb.pb.Metapb.SQLType;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings("FCCD_FIND_CLASS_CIRCULAR_DEPENDENCY")
final class CastStringFunc extends Func {
  private CastStringFunc() {
  }

  protected CastStringFunc(Exprpb.ExprType code, Expression[] args, SQLType resultType, Session session) {
    super();
    this.code = code;
    this.name = code.name();
    this.args = args;
    this.resultType = resultType;
    this.session = session;
  }

  @Override
  public CastStringFunc clone() {
    CastStringFunc result = new CastStringFunc();
    clone(result);
    return result;
  }

  @Override
  public StringValue execString(ValueAccessor accessor) throws JimException {
    return args[0].execString(session, accessor);
  }

  @Override
  public DoubleValue execDouble(ValueAccessor accessor) throws JimException {
    final Expression arg = args[0];
    final Value value = arg.execString(session, accessor);
    if (value == null) {
      return null;
    }

    return ValueConvertor.convertToDouble(session, value, resultType);
  }

  @Override
  public DecimalValue execDecimal(ValueAccessor accessor) throws JimException {
    final Expression arg = args[0];
    final Value value = arg.execString(session, accessor);
    if (value == null) {
      return null;
    }

    return ValueConvertor.convertToDecimal(session, value, resultType);
  }

  @Override
  public LongValue execLong(ValueAccessor accessor) throws JimException {
    final Expression arg = args[0];
    final Value value = arg.execString(session, accessor);
    if (value == null) {
      return null;
    }

    return ValueConvertor.convertToLong(session, value, resultType);
  }

  @Override
  public UnsignedLongValue execUnsignedLong(ValueAccessor accessor) throws JimException {
    final Expression arg = args[0];
    final Value value = arg.execString(session, accessor);
    if (value == null) {
      return null;
    }

    return ValueConvertor.convertToUnsignedLong(session, value, resultType);
  }

  @Override
  public DateValue execDate(ValueAccessor accessor) throws JimException {
    final Expression arg = args[0];
    final Value value = arg.execString(session, accessor);
    if (value == null) {
      return null;
    }

    return ValueConvertor.convertToDate(session, value, resultType);
  }

  @Override
  public TimeValue execTime(ValueAccessor accessor) throws JimException {
    final Expression arg = args[0];
    final Value value = arg.execString(session, accessor);
    if (value == null) {
      return null;
    }

    return ValueConvertor.convertToTime(session, value, resultType);
  }

  @Override
  public YearValue execYear(ValueAccessor accessor) throws JimException {
    final Expression arg = args[0];
    final Value value = arg.execString(session, accessor);
    if (value == null) {
      return null;
    }

    return ValueConvertor.convertToYear(session, value, resultType);
  }
}
