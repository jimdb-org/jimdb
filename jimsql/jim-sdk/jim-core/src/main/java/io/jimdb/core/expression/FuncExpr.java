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

import static io.jimdb.core.expression.Points.MAX_POINT;
import static io.jimdb.core.expression.Points.MIN_POINT;

import java.util.ArrayList;
import java.util.List;

import io.jimdb.common.exception.BaseException;
import io.jimdb.core.Session;
import io.jimdb.core.expression.functions.Func;
import io.jimdb.core.expression.functions.FuncType;
import io.jimdb.core.expression.functions.builtin.cast.CastFuncBuilder;
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
import io.jimdb.pb.Metapb.SQLType;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings()
public final class FuncExpr extends Expression {
  private final Func func;
  private final FuncType funcType;

  public FuncExpr(FuncType funcType, Func func, SQLType resultType) {
    this.funcType = funcType;
    this.func = func;
    this.resultType = resultType;
  }

  public static Expression build(Session session, FuncType funcType, SQLType type, boolean isFold, Expression... args) {
    Preconditions.checkArgument(type != null, "SQLType must not be null");
    if (funcType == FuncType.Cast) {
      return CastFuncBuilder.build(session, type, args[0]);
    }

    Func func = funcType.buildFunction(session, args);
    SQLType retType = type;
    if (func.getResultType() != Types.UNDEFINE_SQLTYPE) {
      retType = func.getResultType();
    }

    FuncExpr funcExpr = new FuncExpr(funcType, func, retType);
    return isFold ? ExpressionUtil.foldConst(funcExpr).getT1() : funcExpr;
  }

  public FuncType getFuncType() {
    return this.funcType;
  }

  public Func getFunc() {
    return func;
  }

  public Expression[] getArgs() {
    return func.getArgs();
  }

  public Session getSession() {
    return func.getSession();
  }

  @Override
  public ExpressionType getExprType() {
    return ExpressionType.FUNC;
  }

  @Override
  public List<Point> convertToPoints(Session session) {
    Expression[] args = func.getArgs();

    switch (this.funcType) {
      case Equality:
      case NotEqual:
      case GreaterThan:
      case GreaterThanOrEqual:
      case LessThan:
      case LessThanOrEqual:
        return extractFromBinaryOp();
      case BooleanAnd:
        return Points.intersect(session, args[0].convertToPoints(session), args[1].convertToPoints(session));
      case BooleanOr:
        return Points.union(session, args[0].convertToPoints(session), args[1].convertToPoints(session));
      default:
        throw new UnsupportedOperationException("unsupported funcType : " + this.funcType);
    }
  }

  private List<Point> extractFromBinaryOp() {

    Expression[] args = func.getArgs();

    Value value = null;
    FuncType funcType = this.funcType;
    // a = 1
    if (args[0].getExprType() == ExpressionType.COLUMN) {
      value = ((ValueExpr) args[1]).getValue();
    } else if (args[1].getExprType() == ExpressionType.COLUMN) {
      // 1 = a

      value = ((ValueExpr) args[0]).getValue();
      switch (funcType) {
        // 1 >= a  --> a <= 1
        case GreaterThanOrEqual:
          funcType = FuncType.LessThanOrEqual;
          break;
        // 1 > a --> a < 1
        case GreaterThan:
          funcType = FuncType.LessThan;
          break;
        //1 < a --> a > 1
        case LessThan:
          funcType = FuncType.GreaterThan;
          break;
        // 1 <= a --> a >= 1
        case LessThanOrEqual:
          funcType = FuncType.GreaterThanOrEqual;
          break;
        default:
          funcType = this.funcType;
      }
    }

    List<Point> points = new ArrayList<>(4);
    switch (funcType) {
      case Equality:
        Point startPointEq = new Point(value, true);
        Point endPointEq = new Point(value, false);
        points.add(startPointEq);
        points.add(endPointEq);
        break;
      case NotEqual:
        Point startPointNe1 = MIN_POINT;
        Point endPointNe1 = new Point(value, false, false);

        Point startPointNe2 = new Point(value, true, false);
        Point endPointNe2 = MAX_POINT;
        points.add(startPointNe1);
        points.add(endPointNe1);
        points.add(startPointNe2);
        points.add(endPointNe2);
        break;
      case LessThan:
        Point startPointLt = MIN_POINT;
        Point endPointLt = new Point(value, false, false);
        points.add(startPointLt);
        points.add(endPointLt);
        break;
      case LessThanOrEqual:
        Point startPointLe = MIN_POINT;
        Point endPointLe = new Point(value, false);
        points.add(startPointLe);
        points.add(endPointLe);
        break;
      case GreaterThan:
        Point startPointGt = new Point(value, true, false);
        Point endPointGt = MAX_POINT;
        points.add(startPointGt);
        points.add(endPointGt);
        break;
      case GreaterThanOrEqual:
        Point startPointGe = new Point(value, true);
        Point endPointGe = MAX_POINT;
        points.add(startPointGe);
        points.add(endPointGe);
        break;

      default:
        throw new UnsupportedOperationException("unsupported funcType : " + this.funcType);
    }
    return points;
  }

  @Override
  public boolean check(ConditionChecker conditionChecker) {
    if (func.getArgs().length == 2 && (funcType == FuncType.BooleanAnd || funcType == FuncType.BooleanOr
            || funcType == FuncType.Equality || funcType == FuncType.NotEqual
            || funcType == FuncType.GreaterThanOrEqual || funcType == FuncType.GreaterThan
            || funcType == FuncType.LessThanOrEqual || funcType == FuncType.LessThan)) {
      boolean flag = true;
      for (int i = 0; i < func.getArgs().length; i++) {
        flag = flag & func.getArgs()[i].check(conditionChecker);
      }
      return flag;
    }
    return false;
  }

  public List<Expression> flattenDNFCondition() {
    return extractExpressions(FuncType.BooleanOr);
  }

  public List<Expression> flattenCNFCondition() {
    return extractExpressions(FuncType.BooleanAnd);
  }

  private List<Expression> extractExpressions(FuncType funcType) {
    List<Expression> expressions = Lists.newArrayListWithCapacity(func.getArgs().length);

    for (Expression expression: func.getArgs()) {
      if (expression.isFuncExpr(funcType)) {
        expressions.addAll(((FuncExpr) expression).extractExpressions(funcType));
      } else {
        expressions.add(expression);
      }
    }

    return expressions;
  }

  @Override
  public FuncExpr clone() {
    FuncExpr result = new FuncExpr(this.funcType, this.func.clone(), this.resultType);
    this.clone(result);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof FuncExpr)) {
      return false;
    }

    FuncExpr otherFunc = (FuncExpr) obj;
    if (!this.func.getName().equals(otherFunc.func.getName())) {
      return false;
    }

    Expression[] args = this.getArgs();
    Expression[] otherFuncArgs = otherFunc.getArgs();
    if (args.length != otherFuncArgs.length) {
      return false;
    }
    for (int i = 0; i < args.length; i++) {
      if (!args[i].equals(otherFuncArgs[i])) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    int result = func != null ? func.hashCode() : 0;
    return 31 * result + (funcType != null ? funcType.hashCode() : 0);
  }

  @Override
  public Expression resolveOffset(Schema schema, boolean isClone) throws BaseException {
    FuncExpr result = isClone ? this.clone() : this;

    for (Expression arg : result.func.getArgs()) {
      arg.resolveOffset(schema, false);
    }
    return result;
  }

  @Override
  public Value exec(ValueAccessor accessor) throws BaseException {
    if (resultValueType == null) {
      resultValueType = Types.sqlToValueType(resultType);
    }
    switch (resultValueType) {
      case NULL:
        return null;
      case LONG:
        return this.execLong(getSession(), accessor);
      case UNSIGNEDLONG:
        return this.execUnsignedLong(getSession(), accessor);
      case DOUBLE:
        return this.execDouble(getSession(), accessor);
      case DECIMAL:
        return this.execDecimal(getSession(), accessor);
      case STRING:
        return this.execString(getSession(), accessor);
      case DATE:
        return this.execDate(getSession(), accessor);
      case TIME:
        return this.execTime(getSession(), accessor);
      case YEAR:
        return this.execYear(getSession(), accessor);
      case BINARY:
        return this.execString(getSession(), accessor);
      default:
        return this.execJson(getSession(), accessor);
    }
  }

  @Override
  public LongValue execLong(Session session, ValueAccessor accessor) throws BaseException {
    final LongValue result = func.execLong(accessor);
    if (result == null || result.isNull()) {
      return null;
    }
    return result;
  }

  @Override
  public UnsignedLongValue execUnsignedLong(Session session, ValueAccessor accessor) throws BaseException {
    final UnsignedLongValue result = func.execUnsignedLong(accessor);
    if (result == null || result.isNull()) {
      return null;
    }
    return result;
  }

  @Override
  public DoubleValue execDouble(Session session, ValueAccessor accessor) throws BaseException {
    final DoubleValue result = func.execDouble(accessor);
    if (result == null || result.isNull()) {
      return null;
    }
    return result;
  }

  @Override
  public DecimalValue execDecimal(Session session, ValueAccessor accessor) throws BaseException {
    final DecimalValue result = func.execDecimal(accessor);
    if (result == null || result.isNull()) {
      return null;
    }
    return result;
  }

  @Override
  public StringValue execString(Session session, ValueAccessor accessor) throws BaseException {
    final StringValue result = func.execString(accessor);
    if (result == null || result.isNull()) {
      return null;
    }
    return result;
  }

  @Override
  public DateValue execDate(Session session, ValueAccessor accessor) throws BaseException {
    final DateValue result = func.execDate(accessor);
    if (result == null || result.isNull()) {
      return null;
    }
    return result;
  }

  @Override
  public TimeValue execTime(Session session, ValueAccessor accessor) throws BaseException {
    final TimeValue result = func.execTime(accessor);
    if (result == null || result.isNull()) {
      return null;
    }
    return result;
  }

  @Override
  public YearValue execYear(Session session, ValueAccessor accessor) throws BaseException {
    final YearValue result = func.execYear(accessor);
    if (result == null || result.isNull()) {
      return null;
    }
    return result;
  }

  @Override
  public JsonValue execJson(Session session, ValueAccessor accessor) throws BaseException {
    final JsonValue result = func.execJson(accessor);
    if (result == null || result.isNull()) {
      return null;
    }
    return result;
  }

  @Override
  public String toString() {
    final StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(this.func.getName()).append('(');
    for (Expression arg : this.getArgs()) {
      stringBuilder.append(arg).append(',');
    }
    stringBuilder.deleteCharAt(stringBuilder.length() - 1).append(')');
    return stringBuilder.toString();
  }
}
