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
package io.jimdb.core.expression.functions.builtin.compare;

import java.util.function.BiFunction;

import io.jimdb.core.expression.functions.Func;
import io.jimdb.core.expression.functions.UnaryFuncBuilder;
import io.jimdb.core.Session;
import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.exception.BaseException;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.pb.Exprpb.ExprType;
import io.jimdb.pb.Metapb;
import io.jimdb.core.types.Types;
import io.jimdb.core.types.ValueType;
import io.jimdb.core.values.LongValue;
import io.jimdb.core.values.Value;

/**
 * @version V1.0
 */
public final class IsNullFunc extends Func {
  private BiFunction<Session, ValueAccessor, Value> executor;

  private IsNullFunc() {
  }

  protected IsNullFunc(Session session, ExprType code, BiFunction<Session, ValueAccessor, Value> executor,
                       Expression[] args, ValueType... expectArgs) {
    super(session, args, ValueType.LONG, expectArgs);
    this.code = code;
    this.name = code.name();
    this.executor = executor;
    Metapb.SQLType.Builder builder = resultType.toBuilder();
    builder.setPrecision(1);
    this.resultType = builder.build();
  }

  @Override
  public IsNullFunc clone() {
    IsNullFunc result = new IsNullFunc();
    clone(result);
    result.executor = executor;
    return result;
  }

  @Override
  public LongValue execLong(ValueAccessor accessor) throws BaseException {
    Value value = executor.apply(session, accessor);
    return (value == null || value.isNull()) ? LongValue.getInstance(1) : LongValue.getInstance(0);
  }

  /**
   *
   */
  public static final class IsNullFuncBuilder extends UnaryFuncBuilder {
    public IsNullFuncBuilder(String name) {
      super(name);
    }

    @Override
    protected Func doBuild(Session session, Expression[] args) {
      final Expression arg = args[0];
      ValueType argType = Types.sqlToValueType(arg.getResultType());
      if (argType == ValueType.JSON || argType == ValueType.BINARY) {
        argType = ValueType.STRING;
      }

      final ExprType code;
      final BiFunction<Session, ValueAccessor, Value> executor;
      switch (argType) {
        case LONG:
          code = ExprType.IntIsNull;
          executor = arg::execLong;
          break;
        case UNSIGNEDLONG:
          code = ExprType.IntIsNull;
          executor = arg::execUnsignedLong;
          break;
        case DOUBLE:
          code = ExprType.RealIsNull;
          executor = arg::execDouble;
          break;
        case DECIMAL:
          code = ExprType.DecimalIsNull;
          executor = arg::execDecimal;
          break;
        case STRING:
          code = ExprType.StringIsNull;
          executor = arg::execString;
          break;
        case DATE:
          code = ExprType.DateIsNull;
          executor = arg::execDate;
          break;
        case TIME:
          code = ExprType.TimeIsNull;
          executor = arg::execTime;
          break;
        default:
          throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_NOT_SUPPORTED_YET, "ValueType(" + argType.name() + ")");
      }

      return new IsNullFunc(session, code, executor, args, argType);
    }
  }
}
