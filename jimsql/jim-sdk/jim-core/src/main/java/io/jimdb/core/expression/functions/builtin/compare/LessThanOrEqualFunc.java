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

import java.util.EnumMap;

import io.jimdb.core.expression.functions.Func;
import io.jimdb.core.expression.functions.builtin.compare.comparators.ExprComparator;
import io.jimdb.core.Session;
import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.exception.BaseException;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.expression.functions.builtin.compare.comparators.ComparatorFacade;
import io.jimdb.pb.Exprpb;
import io.jimdb.core.types.ValueType;
import io.jimdb.core.values.LongValue;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 *
 */
@SuppressFBWarnings("CLI_CONSTANT_LIST_INDEX")
public final class LessThanOrEqualFunc extends Func {
  protected static final EnumMap<ValueType, Exprpb.ExprType> EXPRTYPE_MAP;

  static {
    EXPRTYPE_MAP = new EnumMap<>(ValueType.class);
    EXPRTYPE_MAP.put(ValueType.LONG, Exprpb.ExprType.LessOrEqualInt);
    EXPRTYPE_MAP.put(ValueType.UNSIGNEDLONG, Exprpb.ExprType.LessOrEqualInt);
    EXPRTYPE_MAP.put(ValueType.STRING, Exprpb.ExprType.LessOrEqualString);
    EXPRTYPE_MAP.put(ValueType.DOUBLE, Exprpb.ExprType.LessOrEqualReal);
    EXPRTYPE_MAP.put(ValueType.DECIMAL, Exprpb.ExprType.LessOrEqualDecimal);
    EXPRTYPE_MAP.put(ValueType.DATE, Exprpb.ExprType.LessOrEqualDate);
    EXPRTYPE_MAP.put(ValueType.TIME, Exprpb.ExprType.LessOrEqualTime);
    EXPRTYPE_MAP.put(ValueType.YEAR, Exprpb.ExprType.LessOrEqualInt);
  }

  private ExprComparator comparator;

  private LessThanOrEqualFunc() {

  }

  protected LessThanOrEqualFunc(Session session, Expression[] args, ValueType retTp, ValueType cmpTp) {
    super(session, args, retTp, cmpTp, cmpTp);
    final Exprpb.ExprType exprCode = EXPRTYPE_MAP.get(cmpTp);
    if (exprCode == null) {
      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_NOT_SUPPORTED_YET, "ValueType(" + cmpTp.name() + ")");
    }
    this.code = exprCode;
    this.name = exprCode.name();
    this.comparator = ComparatorFacade.getComparator(cmpTp);
  }

  @Override
  public LessThanOrEqualFunc clone() {
    LessThanOrEqualFunc result = new LessThanOrEqualFunc();
    clone(result);
    result.comparator = comparator;
    return result;
  }

  @Override
  public LongValue execLong(ValueAccessor accessor) throws BaseException {
    LongValue cmp = comparator.compare(session, args[0], accessor, args[1], accessor, false);
    if (cmp == null) {
      return null;
    }

    return cmp.getValue() <= 0 ? LongValue.getInstance(1) : LongValue.getInstance(0);
  }
}
