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

import io.jimdb.core.expression.functions.BinaryFuncBuilder;
import io.jimdb.core.expression.functions.Func;
import io.jimdb.core.expression.functions.FuncType;
import io.jimdb.core.Session;
import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.ExpressionType;
import io.jimdb.core.expression.ExpressionUtil;
import io.jimdb.core.expression.ValueExpr;
import io.jimdb.core.expression.functions.builtin.compare.comparators.ComparatorFacade;
import io.jimdb.pb.Metapb;
import io.jimdb.core.types.Types;
import io.jimdb.core.types.ValueType;
import io.jimdb.core.values.LongValue;
import io.jimdb.core.values.UnsignedLongValue;
import io.jimdb.core.values.Value;
import io.jimdb.core.values.ValueConvertor;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.util.function.Tuple2;

/**
 * @version V1.0
 */
@SuppressFBWarnings("CLI_CONSTANT_LIST_INDEX")
public final class CompareFuncBuilder extends BinaryFuncBuilder {
  private FuncType cmpType;

  public CompareFuncBuilder(String name) {
    super(name);
  }

  public void setCmpType(FuncType cmpType) {
    this.cmpType = cmpType;
  }

  @Override
  protected Func doBuild(Session session, Expression[] exprs) {
    Expression[] args = this.wrapArgs(session, exprs);
    ValueType retType = ComparatorFacade.getCompareType(args[0], args[1]);
    return this.buildCmpFunc(session, args, retType);
  }

  private Expression[] wrapArgs(Session session, Expression[] args) {
    ValueType arg1Type = Types.sqlToValueType(args[0].getResultType());
    ValueType arg2Type = Types.sqlToValueType(args[1].getResultType());
    boolean arg1Int = arg1Type == ValueType.LONG || arg1Type == ValueType.UNSIGNEDLONG;
    boolean arg2Int = arg2Type == ValueType.LONG || arg2Type == ValueType.UNSIGNEDLONG;
    ValueExpr arg1Const = args[0].getExprType() == ExpressionType.CONST ? (ValueExpr) args[0] : null;
    ValueExpr arg2Const = args[1].getExprType() == ExpressionType.CONST ? (ValueExpr) args[1] : null;
    Expression arg1Result = args[0];
    Expression arg2Result = args[1];
    boolean isErr = false;
    boolean isINF = false;
    boolean isNegINF = false;

    if (arg1Int && arg1Const == null && !arg2Int && arg2Const != null) {
      Tuple2<ValueExpr, Boolean> arg2Wrap = ExpressionUtil.rewriteComparedConstant(session, arg2Const, args[0].getResultType(), this.cmpType);
      arg2Result = arg2Wrap.getT1();
      isErr = arg2Wrap.getT2();
      ValueType arg2ResultType = Types.sqlToValueType(arg2Result.getResultType());
      if (isErr && (arg2ResultType == ValueType.LONG || arg2ResultType == ValueType.UNSIGNEDLONG)) {
        final long v;
        final Value arg2Value = ((ValueExpr) arg2Result).getValue();
        switch (arg2Value.getType()) {
          case LONG:
            v = ((LongValue) arg2Value).getValue();
            break;
          case UNSIGNEDLONG:
            v = ((UnsignedLongValue) arg2Value).getValue().longValue();
            break;
          default:
            v = ValueConvertor.convertToLong(session, arg2Value, null).getValue();
        }

        if ((v & 1) == 1) {
          isINF = true;
        } else {
          isNegINF = true;
        }
      }
    }

    if (arg2Int && arg2Const == null && !arg1Int && arg1Const != null) {
      Tuple2<ValueExpr, Boolean> arg1Wrap = ExpressionUtil.rewriteComparedConstant(session, arg1Const, args[1].getResultType(), this.cmpType.getOppositeCompare());
      arg1Result = arg1Wrap.getT1();
      isErr = arg1Wrap.getT2();
      ValueType arg1ResultType = Types.sqlToValueType(arg1Result.getResultType());
      if (isErr && (arg1ResultType == ValueType.LONG || arg1ResultType == ValueType.UNSIGNEDLONG)) {
        final long v;
        final Value arg1Value = ((ValueExpr) arg1Result).getValue();
        switch (arg1Value.getType()) {
          case LONG:
            v = ((LongValue) arg1Value).getValue();
            break;
          case UNSIGNEDLONG:
            v = ((UnsignedLongValue) arg1Value).getValue().longValue();
            break;
          default:
            v = ValueConvertor.convertToLong(session, arg1Value, null).getValue();
        }

        if ((v & 1) == 1) {
          isNegINF = true;
        } else {
          isINF = true;
        }
      }
    }

    if (isErr && (cmpType == FuncType.Equality || cmpType == FuncType.LessThanOrEqualOrGreaterThan)) {
      return new Expression[]{ ValueExpr.ZERO.clone(), ValueExpr.ONE.clone() };
    }
    if (isINF) {
      return new Expression[]{ ValueExpr.ZERO.clone(), ValueExpr.ONE.clone() };
    }
    if (isNegINF) {
      return new Expression[]{ ValueExpr.ONE.clone(), ValueExpr.ZERO.clone() };
    }
    return new Expression[]{ arg1Result, arg2Result };
  }

  private Func buildCmpFunc(Session session, Expression[] args, ValueType cmpTp) {
    final Func cmpFunc;
    switch (cmpType) {
      case Equality:
        cmpFunc = new EqualityFunc(session, args, ValueType.LONG, cmpTp);
        break;
      case NotEqual:
        cmpFunc = new NotEqualityFunc(session, args, ValueType.LONG, cmpTp);
        break;
      case LessThan:
        cmpFunc = new LessThanFunc(session, args, ValueType.LONG, cmpTp);
        break;
      case LessThanOrEqual:
        cmpFunc = new LessThanOrEqualFunc(session, args, ValueType.LONG, cmpTp);
        break;
      case GreaterThan:
        cmpFunc = new GreaterThanFunc(session, args, ValueType.LONG, cmpTp);
        break;
      case GreaterThanOrEqual:
        cmpFunc = new GreaterThanOrEqualFunc(session, args, ValueType.LONG, cmpTp);
        break;
      default:
        throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_NOT_SUPPORTED_YET, "Function(" + cmpType.name() + ")");
    }

    Metapb.SQLType.Builder builder = cmpFunc.getResultType().toBuilder();
    builder.setPrecision(1);
    cmpFunc.setResultType(builder.build());
    return cmpFunc;
  }
}
