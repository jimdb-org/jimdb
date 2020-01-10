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
package io.jimdb.core.expression.functions;

import io.jimdb.core.Session;
import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.exception.JimException;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.expression.functions.builtin.cast.CastFuncBuilder;
import io.jimdb.pb.Basepb.DataType;
import io.jimdb.pb.Exprpb.ExprType;
import io.jimdb.pb.Metapb.SQLType;
import io.jimdb.core.types.Types;
import io.jimdb.core.types.ValueType;
import io.jimdb.core.values.DateValue;
import io.jimdb.core.values.DecimalValue;
import io.jimdb.core.values.DoubleValue;
import io.jimdb.core.values.JsonValue;
import io.jimdb.core.values.LongValue;
import io.jimdb.core.values.StringValue;
import io.jimdb.core.values.TimeValue;
import io.jimdb.core.values.UnsignedLongValue;
import io.jimdb.core.values.YearValue;

import com.google.common.base.Preconditions;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings("EI_EXPOSE_REP")
public abstract class Func implements Cloneable {
  protected String name;
  protected SQLType resultType;
  protected ExprType code;
  protected Session session;
  protected Expression[] args;

  protected Func() {
  }

  protected Func(Session session, Expression[] args, ValueType retTp, ValueType... expectArgs) {
    Preconditions.checkArgument(args.length == expectArgs.length, "the length of args and expectArgs is not equal");

    for (int i = 0; i < args.length; i++) {
      switch (expectArgs[i]) {
        case LONG:
          args[i] = wrapCastToInt(session, args[i], false);
          break;
        case UNSIGNEDLONG:
        case YEAR:
          args[i] = wrapCastToInt(session, args[i], true);
          break;
        case DOUBLE:
          args[i] = wrapCastToDouble(session, args[i]);
          break;
        case DECIMAL:
          args[i] = wrapCastToDecimal(session, args[i]);
          break;
        case STRING:
          args[i] = wrapCastToString(session, args[i], false);
          break;
        case BINARY:
          args[i] = wrapCastToString(session, args[i], true);
          break;
        case DATE:
          args[i] = wrapCastToDate(session, args[i]);
          break;
        case TIME:
          args[i] = wrapCastToTime(session, args[i]);
          break;
        default:
          throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_NOT_SUPPORTED_YET, expectArgs[i].name());
      }
    }

    final SQLType sqlType;
    final SQLType.Builder builder = SQLType.newBuilder();
    builder.setCharset(Types.DEFAULT_CHARSET.name())
            .setCollate(Types.DEFAULT_COLLATE);
    switch (retTp) {
      case LONG:
        builder.setType(DataType.BigInt)
                .setPrecision(Types.MAX_INT_WIDTH)
                .setBinary(true);
        sqlType = builder.build();
        break;
      case UNSIGNEDLONG:
        builder.setType(DataType.BigInt)
                .setPrecision(Types.MAX_INT_WIDTH)
                .setBinary(true)
                .setUnsigned(true);
        sqlType = builder.build();
        break;
      case DOUBLE:
        builder.setType(DataType.Double)
                .setPrecision(Types.MAX_REAL_WIDTH)
                .setScale(Types.UNDEFINE_WIDTH)
                .setBinary(true);
        sqlType = builder.build();
        break;
      case DECIMAL:
        builder.setType(DataType.Decimal)
                .setPrecision(Types.MAX_DEC_WIDTH)
                .setBinary(true);
        sqlType = builder.build();
        break;
      case STRING:
        builder.setType(DataType.Varchar)
                .setScale(Types.UNDEFINE_WIDTH);
        sqlType = builder.build();
        break;
      case BINARY:
        builder.setType(DataType.Binary)
                .setScale(Types.UNDEFINE_WIDTH);
        sqlType = builder.build();
        break;
      case DATE:
        builder.setType(DataType.TimeStamp)
                .setPrecision(Types.MAX_DATETIME_FSP_WIDTH)
                .setScale(6)
                .setBinary(true);
        sqlType = builder.build();
        break;
      case TIME:
        builder.setType(DataType.Time)
                .setPrecision(Types.MAX_TIME_FSP_WIDTH)
                .setScale(6)
                .setBinary(true);
        sqlType = builder.build();
        break;
      case YEAR:
        builder.setType(DataType.Year)
                .setPrecision(4)
                .setScale(Types.UNDEFINE_WIDTH)
                .setBinary(true);
        sqlType = builder.build();
        break;
      default:
        throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_NOT_SUPPORTED_YET, String.format("cast type(%s)", retTp.name()));
    }

    this.session = session;
    this.args = args;
    this.resultType = sqlType;
  }

  @Override
  public abstract Func clone();

  protected void clone(Func f) {
    f.name = this.name;
    f.resultType = this.resultType;
    f.session = this.session;
    f.code = this.code;
    if (this.args != null) {
      Expression[] args = new Expression[this.args.length];
      for (int i = 0; i < args.length; i++) {
        args[i] = this.args[i].clone();
      }
      f.args = args;
    }
  }

  public String getName() {
    return name;
  }

  public ExprType getCode() {
    return code;
  }

  public Expression[] getArgs() {
    return args;
  }

  public void setArgs(Expression[] args) {
    this.args = args;
  }

  public SQLType getResultType() {
    return resultType;
  }

  public void setResultType(SQLType resultType) {
    this.resultType = resultType;
  }

  public Session getSession() {
    return session;
  }

  public LongValue execLong(ValueAccessor accessor) throws JimException {
    throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_FUNC_OPERATION_ERROR, name, "execLong");
  }

  public UnsignedLongValue execUnsignedLong(ValueAccessor accessor) throws JimException {
    throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_FUNC_OPERATION_ERROR, name, "execUnsignedLong");
  }

  public DoubleValue execDouble(ValueAccessor accessor) throws JimException {
    throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_FUNC_OPERATION_ERROR, name, "execDouble");
  }

  public DecimalValue execDecimal(ValueAccessor accessor) throws JimException {
    throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_FUNC_OPERATION_ERROR, name, "execDecimal");
  }

  public StringValue execString(ValueAccessor accessor) throws JimException {
    throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_FUNC_OPERATION_ERROR, name, "execString");
  }

  public DateValue execDate(ValueAccessor accessor) throws JimException {
    throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_FUNC_OPERATION_ERROR, name, "execDate");
  }

  public TimeValue execTime(ValueAccessor accessor) throws JimException {
    throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_FUNC_OPERATION_ERROR, name, "execTime");
  }

  public YearValue execYear(ValueAccessor accessor) throws JimException {
    throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_FUNC_OPERATION_ERROR, name, "execYear");
  }

  public JsonValue execJson(ValueAccessor accessor) throws JimException {
    throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_FUNC_OPERATION_ERROR, name, "execJson");
  }

  protected final Expression wrapCastToInt(Session session, Expression arg, boolean unsigned) {
    ValueType vt = Types.sqlToValueType(arg.getResultType());
    if (!unsigned && vt == ValueType.LONG) {
      return arg;
    }
    if (unsigned && vt == ValueType.UNSIGNEDLONG) {
      return arg;
    }

    SQLType.Builder builder = SQLType.newBuilder();
    builder.setType(DataType.BigInt)
            .setPrecision(arg.getResultType().getPrecision())
            .setBinary(true)
            .setUnsigned(unsigned)
            .setCharset(Types.DEFAULT_CHARSET.name())
            .setCollate(Types.DEFAULT_COLLATE);
    return CastFuncBuilder.build(session, builder.build(), arg);
  }

  protected final Expression wrapCastToDouble(Session session, Expression arg) {
    if (Types.sqlToValueType(arg.getResultType()) == ValueType.DOUBLE) {
      return arg;
    }

    SQLType.Builder builder = SQLType.newBuilder();
    builder.setType(DataType.Double)
            .setPrecision(arg.getResultType().getPrecision())
            .setScale(Types.UNDEFINE_WIDTH)
            .setBinary(true)
            .setUnsigned(arg.getResultType().getUnsigned())
            .setCharset(Types.DEFAULT_CHARSET.name())
            .setCollate(Types.DEFAULT_COLLATE);
    return CastFuncBuilder.build(session, builder.build(), arg);
  }

  protected final Expression wrapCastToDecimal(Session session, Expression arg) {
    if (Types.sqlToValueType(arg.getResultType()) == ValueType.DECIMAL) {
      return arg;
    }

    SQLType.Builder builder = SQLType.newBuilder();
    builder.setType(DataType.Decimal)
            .setPrecision(arg.getResultType().getPrecision())
            .setScale(Types.UNDEFINE_WIDTH)
            .setBinary(true)
            .setUnsigned(arg.getResultType().getUnsigned())
            .setCharset(Types.DEFAULT_CHARSET.name())
            .setCollate(Types.DEFAULT_COLLATE);
    return CastFuncBuilder.build(session, builder.build(), arg);
  }

  protected final Expression wrapCastToString(Session session, Expression arg, boolean binary) {
    ValueType vt = Types.sqlToValueType(arg.getResultType());
    if (!binary && vt == ValueType.STRING) {
      return arg;
    }
    if (binary && vt == ValueType.BINARY) {
      return arg;
    }

    int precision = arg.getResultType().getPrecision();
    if (arg.getResultType().getType() == DataType.Decimal && precision != Types.UNDEFINE_WIDTH) {
      precision += 2;
    }
    if (vt == ValueType.LONG || vt == ValueType.UNSIGNEDLONG) {
      precision = Types.MAX_INT_WIDTH;
    }

    SQLType.Builder builder = SQLType.newBuilder();
    builder.setType(DataType.Varchar)
            .setPrecision(precision)
            .setScale(Types.UNDEFINE_WIDTH)
            .setCharset(Types.DEFAULT_CHARSET.name())
            .setCollate(Types.DEFAULT_COLLATE);
    return CastFuncBuilder.build(session, builder.build(), arg);
  }

  protected final Expression wrapCastToDate(Session session, Expression arg) {
    ValueType vt = Types.sqlToValueType(arg.getResultType());
    if (vt == ValueType.DATE) {
      return arg;
    }

    SQLType.Builder builder = SQLType.newBuilder();
    builder.setType(DataType.Date)
            .setPrecision(Types.MAX_DATETIME_FSP_WIDTH)
            .setScale(6)
            .setCharset(Types.DEFAULT_CHARSET.name())
            .setCollate(Types.DEFAULT_COLLATE);
    return CastFuncBuilder.build(session, builder.build(), arg);
  }

  protected final Expression wrapCastToTime(Session session, Expression arg) {
    ValueType vt = Types.sqlToValueType(arg.getResultType());
    if (vt == ValueType.TIME) {
      return arg;
    }
    SQLType.Builder builder = SQLType.newBuilder();
    builder.setType(DataType.Time)
            .setPrecision(Types.MAX_TIME_FSP_WIDTH)
            .setScale(6)
            .setCharset(Types.DEFAULT_CHARSET.name())
            .setCollate(Types.DEFAULT_COLLATE);
    return CastFuncBuilder.build(session, builder.build(), arg);
  }
}
