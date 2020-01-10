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

import io.jimdb.core.expression.functions.Func;
import io.jimdb.core.expression.functions.FuncType;
import io.jimdb.core.Session;
import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.ExpressionUtil;
import io.jimdb.core.expression.FuncExpr;
import io.jimdb.core.expression.ValueExpr;
import io.jimdb.pb.Exprpb.ExprType;
import io.jimdb.pb.Metapb.SQLType;
import io.jimdb.core.types.Types;
import io.jimdb.core.types.ValueType;
import io.jimdb.core.values.NullValue;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings("FCCD_FIND_CLASS_CIRCULAR_DEPENDENCY")
public final class CastFuncBuilder {
  private CastFuncBuilder() {
  }

  public static Expression build(Session session, SQLType type, Expression... args) {
    if (args == null || args.length != 1) {
      throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT, "CAST");
    }
    final ValueType argType = Types.sqlToValueType(args[0].getResultType());
    if (argType == ValueType.NULL) {
      return new ValueExpr(NullValue.getInstance(), type);
    }

    final Func castFunc;
    final ValueType vt = Types.sqlToValueType(type);
    switch (vt) {
      case LONG:
      case UNSIGNEDLONG:
      case YEAR:
        castFunc = buildCastToInt(session, type, args);
        break;
      case DOUBLE:
        castFunc = buildCastToDouble(session, type, args);
        break;
      case DECIMAL:
        castFunc = buildCastToDecimal(session, type, args);
        break;
      case STRING:
      case BINARY:
        castFunc = buildCastToString(session, type, args);
        break;
      case DATE:
        castFunc = buildCastToDate(session, type, args);
        break;
      case TIME:
        castFunc = buildCastToTime(session, type, args);
        break;
      default:
        throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_NOT_SUPPORTED_YET, String.format("cast type(%s)", type.getType().name()));
    }

    FuncExpr funcExpr = new FuncExpr(FuncType.Cast, castFunc, type);
    return vt == ValueType.JSON ? funcExpr : ExpressionUtil.foldConst(funcExpr).getT1();
  }

  private static Func buildCastToInt(Session session, SQLType type, Expression[] args) {
    final ValueType argType = Types.sqlToValueType(args[0].getResultType());

    switch (argType) {
      case LONG:
      case UNSIGNEDLONG:
      case YEAR:
        return new CastIntFunc(ExprType.CastIntToInt, args, type, session);
      case DOUBLE:
        return new CastDoubleFunc(ExprType.CastRealToInt, args, type, session);
      case DECIMAL:
        return new CastDecimalFunc(ExprType.CastDecimalToInt, args, type, session);
      case STRING:
      case BINARY:
        return new CastStringFunc(ExprType.CastStringToInt, args, type, session);
      case DATE:
        return new CastDateFunc(ExprType.CastDateToInt, args, type, session);
      case TIME:
        return new CastTimeFunc(ExprType.CastTimeToInt, args, type, session);
      default:
        throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_NOT_SUPPORTED_YET, String.format("cast type(%s) to int", args[0].getResultType().getType().name()));
    }
  }

  private static Func buildCastToDouble(Session session, SQLType type, Expression[] args) {
    final ValueType argType = Types.sqlToValueType(args[0].getResultType());

    switch (argType) {
      case LONG:
      case UNSIGNEDLONG:
      case YEAR:
        return new CastIntFunc(ExprType.CastIntToReal, args, type, session);
      case DOUBLE:
        return new CastDoubleFunc(ExprType.CastRealToReal, args, type, session);
      case DECIMAL:
        return new CastDecimalFunc(ExprType.CastDecimalToReal, args, type, session);
      case STRING:
      case BINARY:
        return new CastStringFunc(ExprType.CastStringToReal, args, type, session);
      case DATE:
        return new CastDateFunc(ExprType.CastDateToReal, args, type, session);
      case TIME:
        return new CastTimeFunc(ExprType.CastTimeToReal, args, type, session);
      default:
        throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_NOT_SUPPORTED_YET, String.format("cast type(%s) to double", args[0].getResultType().getType().name()));
    }
  }

  private static Func buildCastToDecimal(Session session, SQLType type, Expression[] args) {
    final ValueType argType = Types.sqlToValueType(args[0].getResultType());

    switch (argType) {
      case LONG:
      case UNSIGNEDLONG:
        return new CastIntFunc(ExprType.CastIntToDecimal, args, type, session);
      case DOUBLE:
        return new CastDoubleFunc(ExprType.CastRealToDecimal, args, type, session);
      case DECIMAL:
        return new CastDecimalFunc(ExprType.CastDecimalToDecimal, args, type, session);
      case STRING:
      case BINARY:
        return new CastStringFunc(ExprType.CastStringToDecimal, args, type, session);
      case DATE:
        return new CastDateFunc(ExprType.CastDateToDecimal, args, type, session);
      case TIME:
        return new CastTimeFunc(ExprType.CastTimeToDecimal, args, type, session);
      default:
        throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_NOT_SUPPORTED_YET, String.format("cast type(%s) to decimal", args[0].getResultType().getType().name()));
    }
  }

  private static Func buildCastToString(Session session, SQLType type, Expression[] args) {
    final ValueType argType = Types.sqlToValueType(args[0].getResultType());

    switch (argType) {
      case LONG:
      case UNSIGNEDLONG:
      case YEAR:
        return new CastIntFunc(ExprType.CastIntToString, args, type, session);
      case DOUBLE:
        return new CastDoubleFunc(ExprType.CastRealToString, args, type, session);
      case DECIMAL:
        return new CastDecimalFunc(ExprType.CastDecimalToString, args, type, session);
      case STRING:
      case BINARY:
        return new CastStringFunc(ExprType.CastStringToString, args, type, session);
      case DATE:
        return new CastDateFunc(ExprType.CastDateToString, args, type, session);
      case TIME:
        return new CastTimeFunc(ExprType.CastTimeToString, args, type, session);
      default:
        throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_NOT_SUPPORTED_YET, String.format("cast type(%s) to string", args[0].getResultType().getType().name()));
    }
  }

  private static Func buildCastToDate(Session session, SQLType type, Expression[] args) {
    final ValueType argType = Types.sqlToValueType(args[0].getResultType());

    switch (argType) {
      case LONG:
      case UNSIGNEDLONG:
        return new CastIntFunc(ExprType.CastIntToDate, args, type, session);
      case DOUBLE:
        return new CastDoubleFunc(ExprType.CastRealToDate, args, type, session);
      case DECIMAL:
        return new CastDecimalFunc(ExprType.CastDecimalToDate, args, type, session);
      case STRING:
      case BINARY:
        return new CastStringFunc(ExprType.CastStringToDate, args, type, session);
      case DATE:
        return new CastDateFunc(ExprType.CastDateToDate, args, type, session);
      case TIME:
        return new CastTimeFunc(ExprType.CastTimeToDate, args, type, session);
      default:
        throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_NOT_SUPPORTED_YET, String.format("cast type(%s) to date", args[0].getResultType().getType().name()));
    }
  }

  private static Func buildCastToTime(Session session, SQLType type, Expression[] args) {
    final ValueType argType = Types.sqlToValueType(args[0].getResultType());

    switch (argType) {
      case LONG:
      case UNSIGNEDLONG:
        return new CastIntFunc(ExprType.CastIntToTime, args, type, session);
      case DOUBLE:
        return new CastDoubleFunc(ExprType.CastRealToTime, args, type, session);
      case DECIMAL:
        return new CastDecimalFunc(ExprType.CastDecimalToTime, args, type, session);
      case STRING:
      case BINARY:
        return new CastStringFunc(ExprType.CastStringToTime, args, type, session);
      case DATE:
        return new CastDateFunc(ExprType.CastDateToTime, args, type, session);
      case TIME:
        return new CastTimeFunc(ExprType.CastTimeToTime, args, type, session);
      default:
        throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_NOT_SUPPORTED_YET, String.format("cast type(%s) to date", args[0].getResultType().getType().name()));
    }
  }
}
