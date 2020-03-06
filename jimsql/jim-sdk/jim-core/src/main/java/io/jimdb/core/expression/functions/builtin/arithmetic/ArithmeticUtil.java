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
package io.jimdb.core.expression.functions.builtin.arithmetic;

import io.jimdb.core.types.Types;
import io.jimdb.core.types.ValueType;
import io.jimdb.pb.Metapb.SQLType;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings("OCP_OVERLY_CONCRETE_PARAMETER")
final class ArithmeticUtil {
  private ArithmeticUtil() {
  }

  public static ValueType getArithmeticType(SQLType sqlType) {
    if (Types.isDateTime(sqlType)) {
      if (sqlType.getScale() > 0) {
        return ValueType.DECIMAL;
      }
      return sqlType.getUnsigned() ? ValueType.UNSIGNEDLONG : ValueType.LONG;
    }

    if (Types.isBinString(sqlType)) {
      return sqlType.getBinary() ? ValueType.UNSIGNEDLONG : ValueType.LONG;
    }

    ValueType result = Types.sqlToValueType(sqlType);
    if (result != ValueType.DECIMAL && result != ValueType.LONG && result != ValueType.UNSIGNEDLONG) {
      result = ValueType.DOUBLE;
    }
    return result;
  }

  public static SQLType toDoublePrecision(SQLType source, SQLType t1, SQLType t2) {
    int scale = source.getScale();
    int precision = (int) source.getPrecision();
    SQLType.Builder resultType = source.toBuilder();
    if (t1.getScale() > -1 && t2.getScale() > -1) {
      scale = t1.getScale() + t2.getScale();
      if (t1.getPrecision() == Types.UNDEFINE_WIDTH || t2.getPrecision() == Types.UNDEFINE_WIDTH) {
        precision = Types.UNDEFINE_WIDTH;
        resultType.setPrecision(precision)
                .setScale(scale);
        return resultType.build();
      }

      int digit = Math.max((int) t1.getPrecision() - t1.getScale(), (int) t2.getPrecision() - t2.getScale());
      precision = Math.min(digit + scale + 3, Types.MAX_REAL_WIDTH);
      resultType.setPrecision(precision)
              .setScale(scale);
      return resultType.build();
    }

    resultType.setPrecision(Types.UNDEFINE_WIDTH)
            .setScale(Types.UNDEFINE_WIDTH);
    return resultType.build();
  }

  public static SQLType toDecimalPrecision(SQLType source, SQLType t1, SQLType t2) {
    int scale = source.getScale();
    int precision = (int) source.getPrecision();
    SQLType.Builder resultType = source.toBuilder();
    if (t1.getScale() > Types.UNDEFINE_WIDTH || t2.getScale() > Types.UNDEFINE_WIDTH) {
      scale = t1.getScale() > Types.UNDEFINE_WIDTH ? t1.getScale() : 0 + t2.getScale() > Types.UNDEFINE_WIDTH ? t2.getScale() : 0;
      if (scale > Types.MAX_DEC_SCALE) {
        scale = Types.MAX_DEC_SCALE;
      }
      if (t1.getPrecision() == Types.UNDEFINE_WIDTH || t2.getPrecision() == Types.UNDEFINE_WIDTH) {
        precision = Types.UNDEFINE_WIDTH;
        resultType.setPrecision(precision)
                .setScale(scale);
        return resultType.build();
      }

      int digit = Math.max((int) t1.getPrecision() - t1.getScale(), (int) t2.getPrecision() - t2.getScale());
      precision = Math.min(digit + scale + 3, Types.MAX_DEC_WIDTH);
      resultType.setPrecision(precision)
              .setScale(scale);
      return resultType.build();
    }

    resultType.setPrecision(Types.MAX_DEC_WIDTH)
            .setScale(Types.MAX_DEC_SCALE);
    return resultType.build();
  }

  public static SQLType toLongPrecision(SQLType source) {
    SQLType.Builder resultType = source.toBuilder();
    resultType.setPrecision(Types.MAX_INT_WIDTH)
            .setScale(0);
    return resultType.build();
  }

  public static SQLType toDivDoublePrecision(SQLType source) {
    SQLType.Builder resultType = source.toBuilder();
    resultType.setPrecision(Types.MAX_REAL_WIDTH)
            .setScale(Types.NOT_FIXED_DEC);
    return resultType.build();
  }

  public static SQLType toDivDecimalPrecision(SQLType source, SQLType t1, SQLType t2) {
    int decA = t1.getScale();
    int decB = t2.getScale();
    if (decA == -1) {
      decA = 0;
    }
    if (decB == -1) {
      decB = 0;
    }

    int scale = decA + Types.PREC_INCREMENT;
    int precision = (int) source.getPrecision();
    SQLType.Builder resultType = source.toBuilder();
    if (scale > Types.MAX_DEC_SCALE) {
      scale = Types.MAX_DEC_SCALE;
    }

    if (precision == Types.UNDEFINE_WIDTH) {
      precision = Types.MAX_DEC_WIDTH;
      resultType.setPrecision(precision)
              .setScale(scale);
      return resultType.build();
    }

    precision = (int) t1.getPrecision() + decB + Types.PREC_INCREMENT;
    if (precision > Types.MAX_DEC_WIDTH) {
      precision = Types.MAX_DEC_WIDTH;
    }
    resultType.setPrecision(precision)
            .setScale(scale);
    return resultType.build();
  }
}
