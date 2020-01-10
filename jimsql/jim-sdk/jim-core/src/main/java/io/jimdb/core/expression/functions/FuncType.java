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

import java.util.EnumMap;

import io.jimdb.core.expression.functions.builtin.GetVarFunc;
import io.jimdb.core.Session;
import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.exception.JimException;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.functions.builtin.GetParamFunc;
import io.jimdb.core.expression.functions.builtin.arithmetic.AddFunc;
import io.jimdb.core.expression.functions.builtin.arithmetic.DIVFunc;
import io.jimdb.core.expression.functions.builtin.arithmetic.DivideFunc;
import io.jimdb.core.expression.functions.builtin.arithmetic.ModulusFunc;
import io.jimdb.core.expression.functions.builtin.arithmetic.MultiplyFunc;
import io.jimdb.core.expression.functions.builtin.arithmetic.SubtractFunc;
import io.jimdb.core.expression.functions.builtin.compare.CompareFuncBuilder;
import io.jimdb.core.expression.functions.builtin.compare.IsNullFunc;
import io.jimdb.core.expression.functions.builtin.logic.AndFunc;
import io.jimdb.core.expression.functions.builtin.logic.NotFunc;
import io.jimdb.core.expression.functions.builtin.logic.OrFunc;
import io.jimdb.core.expression.functions.builtin.logic.XorFunc;
import io.jimdb.core.expression.functions.builtin.math.CeilFunc;
import io.jimdb.core.expression.functions.builtin.math.FloorFunc;
import io.jimdb.core.expression.functions.builtin.others.InFunc;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings({ "FCCD_FIND_CLASS_CIRCULAR_DEPENDENCY" })
public enum FuncType {
  Add(new AddFunc.AddFuncBuilder("Add"), FuncDialect.DEFAULT_DIALECT),
  Subtract(new SubtractFunc.MinusFuncBuilder("Subtract"), FuncDialect.DEFAULT_DIALECT),
  Multiply(new MultiplyFunc.MultiplyFuncBuilder("Multiply"), FuncDialect.DEFAULT_DIALECT),
  Divide(new DivideFunc.DivideFuncBuilder("Divide"), FuncDialect.DEFAULT_DIALECT),
  DIV(new DIVFunc.DIVFuncBuilder("DIV"), FuncDialect.DEFAULT_DIALECT),
  MOD(new ModulusFunc.ModFuncBuilder("MOD"), FuncDialect.DEFAULT_DIALECT),
  Modulus(new ModulusFunc.ModFuncBuilder("Modulus"), FuncDialect.DEFAULT_DIALECT),
  Equality(new CompareFuncBuilder("Equality"), FuncDialect.DEFAULT_DIALECT),
  In(new InFunc.InFuncBuilder("In"), FuncDialect.DEFAULT_DIALECT),
  LessThanOrEqualOrGreaterThan(new CompareFuncBuilder("LessThanOrEqualOrGreaterThan"), FuncDialect.DEFAULT_DIALECT),
  NotEqual(new CompareFuncBuilder("NotEqual"), FuncDialect.DEFAULT_DIALECT),
  LessThanOrGreater(new CompareFuncBuilder("LessThanOrGreater"), FuncDialect.DEFAULT_DIALECT),
  GreaterThan(new CompareFuncBuilder("GreaterThan"), FuncDialect.DEFAULT_DIALECT),
  GreaterThanOrEqual(new CompareFuncBuilder("GreaterThanOrEqual"), FuncDialect.DEFAULT_DIALECT),
  LessThan(new CompareFuncBuilder("LessThan"), FuncDialect.DEFAULT_DIALECT),
  LessThanOrEqual(new CompareFuncBuilder("LessThanOrEqual"), FuncDialect.DEFAULT_DIALECT),
  BooleanOr(new OrFunc.OrFuncBuilder("BooleanOr"), FuncDialect.DEFAULT_DIALECT),
  BooleanAnd(new AndFunc.AndFuncBuilder("BooleanAnd"), FuncDialect.DEFAULT_DIALECT),
  Not(new NotFunc.NotFuncBuilder("BooleanNot"), FuncDialect.DEFAULT_DIALECT),
  BitwiseXor(new XorFunc.XorFuncBuilder("BooleanXor"), FuncDialect.DEFAULT_DIALECT),

  SetVariable(null, new FuncDialect(false, false)),
  GetVariable(new GetVarFunc.GetVarFuncBuilder("GetVariable"), new FuncDialect(false, false)),
  GetParam(new GetParamFunc.GetParamFuncBuilder("GetParam"), new FuncDialect(false, false)),
  DATABASE(new DatabaseFunc.DatabaseBuilder("DATABASE"), new FuncDialect(false, false)),

  Cast(null, FuncDialect.DEFAULT_DIALECT),
  ISNULL(new IsNullFunc.IsNullFuncBuilder("ISNULL"), FuncDialect.DEFAULT_DIALECT),
  VALUES(null, new FuncDialect(false, false)),
  Ceil(new CeilFunc.CeilFuncBuilder("Ceil"), FuncDialect.DEFAULT_DIALECT),
  Floor(new FloorFunc.FloorFuncBuilder("Floor"), FuncDialect.DEFAULT_DIALECT);
//  Variable(null, new FuncDialect(false, false));

  private static final EnumMap<FuncType, FuncType> OPPOSITE_CMP;

  static {
    OPPOSITE_CMP = new EnumMap(FuncType.class);
    OPPOSITE_CMP.put(FuncType.GetVariable, FuncType.GetVariable);
    OPPOSITE_CMP.put(FuncType.LessThan, FuncType.GreaterThan);
    OPPOSITE_CMP.put(FuncType.GreaterThan, FuncType.LessThan);
    OPPOSITE_CMP.put(FuncType.GreaterThanOrEqual, FuncType.LessThanOrEqual);
    OPPOSITE_CMP.put(FuncType.LessThanOrEqual, FuncType.GreaterThanOrEqual);
    OPPOSITE_CMP.put(FuncType.Equality, FuncType.Equality);
    OPPOSITE_CMP.put(FuncType.NotEqual, FuncType.NotEqual);
    OPPOSITE_CMP.put(FuncType.LessThanOrEqualOrGreaterThan, FuncType.LessThanOrEqualOrGreaterThan);
  }

  private final FuncBuilder builder;
  private final FuncDialect dialect;

  FuncType(FuncBuilder builder, FuncDialect dialect) {
    this.builder = builder;
    this.dialect = dialect;
    if (builder instanceof CompareFuncBuilder) {
      ((CompareFuncBuilder) builder).setCmpType(this);
    }
  }

  @SuppressFBWarnings("EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS")
  public static FuncType forName(String name) throws JimException {
    try {
      return FuncType.valueOf(name);
    } catch (Exception ex) {
      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_NOT_SUPPORTED_YET, ex, "Function(" + name + ")");
    }
  }

  public Func buildFunction(Session session, Expression... args) {
    return builder.build(session, args);
  }

  public FuncDialect getDialect() {
    return dialect;
  }

  public FuncType getOppositeCompare() {
    return OPPOSITE_CMP.get(this);
  }
}
