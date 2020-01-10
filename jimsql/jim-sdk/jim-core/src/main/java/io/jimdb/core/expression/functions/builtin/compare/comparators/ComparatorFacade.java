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
package io.jimdb.core.expression.functions.builtin.compare.comparators;

import java.util.EnumMap;

import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.ExpressionType;
import io.jimdb.core.types.Types;
import io.jimdb.core.types.ValueType;
import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.pb.Basepb;
import io.jimdb.pb.Metapb.SQLType;

/**
 * @version V1.0
 */
public final class ComparatorFacade {
  private static final EnumMap<ValueType, ExprComparator> COMPARATOR_MAP;

  static {
    COMPARATOR_MAP = new EnumMap<>(ValueType.class);
    COMPARATOR_MAP.put(ValueType.LONG, IntComparator.INSTANCE);
    COMPARATOR_MAP.put(ValueType.UNSIGNEDLONG, IntComparator.INSTANCE);
    COMPARATOR_MAP.put(ValueType.STRING, StringComparator.INSTANCE);
    COMPARATOR_MAP.put(ValueType.DOUBLE, DoubleComparator.INSTANCE);
    COMPARATOR_MAP.put(ValueType.DECIMAL, DecimalComparator.INSTANCE);
    COMPARATOR_MAP.put(ValueType.DATE, DateComparator.INSTANCE);
    COMPARATOR_MAP.put(ValueType.TIME, TimeComparator.INSTANCE);
    COMPARATOR_MAP.put(ValueType.YEAR, IntComparator.INSTANCE);
  }

  private ComparatorFacade() {
  }

  public static ExprComparator getComparator(Expression expr1, Expression expr2) {
    return getComparator(getCompareType(expr1, expr2));
  }

  public static ExprComparator getComparator(ValueType vt) {
    final ExprComparator result = COMPARATOR_MAP.get(vt);
    if (result == null) {
      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_NOT_SUPPORTED_YET, "ValueType(" + vt.name() + ")");
    }
    return result;
  }

  public static ValueType getCompareType(Expression expr1, Expression expr2) {
    SQLType expr1SqlType = expr1.getResultType();
    SQLType expr2SqlType = expr2.getResultType();
    ValueType expr1ValueType = Types.sqlToValueType(expr1SqlType);
    ValueType expr2ValueType = Types.sqlToValueType(expr2SqlType);
    ValueType compType = getCompareType(expr1ValueType, expr2ValueType, expr1SqlType, expr2SqlType);
    if ((expr1ValueType.isString() && expr2SqlType.getType() == Basepb.DataType.Json)
            || (expr2ValueType.isString() && expr1SqlType.getType() == Basepb.DataType.Json)) {
      compType = ValueType.JSON;
    } else if (compType == ValueType.STRING && (Types.isDateType(expr1SqlType) || Types.isDateType(expr2SqlType))) {
      if (expr1SqlType.getType() == expr2SqlType.getType()) {
        compType = expr1ValueType;
      } else {
        compType = ValueType.DATE;
      }
    } else if (expr1SqlType.getType() == Basepb.DataType.Time && expr2SqlType.getType() == Basepb.DataType.Time) {
      compType = ValueType.TIME;
    } else if (compType == ValueType.YEAR && (expr1SqlType.getType() == Basepb.DataType.Year
            || expr2SqlType.getType() == Basepb.DataType.Year)) {
      compType = ValueType.YEAR;
    } else if (compType == ValueType.DOUBLE || compType == ValueType.STRING) {
      boolean expr1Const = expr1.getExprType() == ExpressionType.CONST;
      boolean expr2Const = expr2.getExprType() == ExpressionType.CONST;
      if ((expr1ValueType == ValueType.DECIMAL && !expr1Const && expr2ValueType.isString() && expr2Const)
              || (expr2ValueType == ValueType.DECIMAL && !expr2Const && expr1ValueType.isString() && expr1Const)) {
        compType = ValueType.DECIMAL;
      } else if (expr1.isTempColumn() && expr2Const || expr2.isTempColumn() && expr1Const) {
        ColumnExpr columnExpr = expr1.getExprType() == ExpressionType.COLUMN ? (ColumnExpr) expr1 : (ColumnExpr) expr2;
        if (columnExpr.getResultType().getType() == Basepb.DataType.Time) {
          compType = ValueType.TIME;
        } else {
          compType = ValueType.DATE;
        }
      }
    }
    return compType;
  }

  private static ValueType getCompareType(ValueType arg1ValueType, ValueType arg2ValueType, SQLType arg1SqlType, SQLType arg2SqlType) {
    if (arg1SqlType == Types.UNDEFINE_TYPE || arg2SqlType == Types.UNDEFINE_TYPE) {
      if (arg1SqlType.getType() == arg2SqlType.getType()) {
        return ValueType.STRING;
      }

      if (arg1SqlType == Types.UNDEFINE_TYPE) {
        arg1ValueType = arg2ValueType;
      } else {
        arg2ValueType = arg1ValueType;
      }
    }

    if (arg1ValueType.isString() && arg2ValueType.isString()) {
      return ValueType.STRING;
    }
    if (arg1ValueType == ValueType.LONG && arg2ValueType == ValueType.LONG) {
      return ValueType.LONG;
    }
    if ((arg1ValueType == ValueType.UNSIGNEDLONG || arg1ValueType == ValueType.LONG) && (arg2ValueType == ValueType.UNSIGNEDLONG || arg2ValueType == ValueType.LONG)) {
      return ValueType.UNSIGNEDLONG;
    }
    if ((arg1ValueType == ValueType.DECIMAL || arg1ValueType == ValueType.UNSIGNEDLONG || arg1ValueType == ValueType.LONG)
            && (arg2ValueType == ValueType.DECIMAL || arg2ValueType == ValueType.UNSIGNEDLONG || arg2ValueType == ValueType.LONG)) {
      return ValueType.DECIMAL;
    }
    if ((arg1ValueType == ValueType.STRING && arg2ValueType == ValueType.YEAR)
            || (arg1ValueType == ValueType.YEAR && arg2ValueType == ValueType.STRING)) {
      return ValueType.YEAR;
    }

    return ValueType.DOUBLE;
  }
}
