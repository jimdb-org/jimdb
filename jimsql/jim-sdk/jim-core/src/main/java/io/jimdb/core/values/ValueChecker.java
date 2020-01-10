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
package io.jimdb.core.values;

import java.math.BigInteger;
import java.util.EnumMap;

import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.pb.Basepb;
import io.jimdb.pb.Metapb.SQLType;

/**
 * check value bound
 *
 * @version V1.0
 */
public class ValueChecker {

  private static final EnumMap<Basepb.DataType, Long> LONG_LOWERBOUND;
  private static final EnumMap<Basepb.DataType, Long> LONG_UPPERBOUND;
  private static final EnumMap<Basepb.DataType, BigInteger> UNSIGNEDLONG_UPPERBOUND;

  static {
    LONG_LOWERBOUND = new EnumMap<>(Basepb.DataType.class);
    LONG_LOWERBOUND.put(Basepb.DataType.TinyInt, Long.valueOf(Byte.MIN_VALUE));
    LONG_LOWERBOUND.put(Basepb.DataType.SmallInt, Long.valueOf(Short.MIN_VALUE));
    LONG_LOWERBOUND.put(Basepb.DataType.MediumInt, Long.valueOf(~((1 << 23) - 1)));
    LONG_LOWERBOUND.put(Basepb.DataType.Int, Long.valueOf(Integer.MIN_VALUE));
    LONG_LOWERBOUND.put(Basepb.DataType.BigInt, Long.MIN_VALUE);

    LONG_UPPERBOUND = new EnumMap<>(Basepb.DataType.class);
    LONG_UPPERBOUND.put(Basepb.DataType.TinyInt, Long.valueOf(Byte.MAX_VALUE));
    LONG_UPPERBOUND.put(Basepb.DataType.SmallInt, Long.valueOf(Short.MAX_VALUE));
    LONG_UPPERBOUND.put(Basepb.DataType.MediumInt, Long.valueOf((1 << 23) - 1));
    LONG_UPPERBOUND.put(Basepb.DataType.Int, Long.valueOf(Integer.MAX_VALUE));
    LONG_UPPERBOUND.put(Basepb.DataType.BigInt, Long.MAX_VALUE);

    UNSIGNEDLONG_UPPERBOUND = new EnumMap<>(Basepb.DataType.class);
    UNSIGNEDLONG_UPPERBOUND.put(Basepb.DataType.TinyInt, BigInteger.valueOf((1 << 8) - 1));
    UNSIGNEDLONG_UPPERBOUND.put(Basepb.DataType.SmallInt, BigInteger.valueOf((1 << 16) - 1));
    UNSIGNEDLONG_UPPERBOUND.put(Basepb.DataType.MediumInt, BigInteger.valueOf((1 << 24) - 1));
    UNSIGNEDLONG_UPPERBOUND.put(Basepb.DataType.Int, BigInteger.valueOf((1L << 32) - 1));
    UNSIGNEDLONG_UPPERBOUND.put(Basepb.DataType.BigInt, new BigInteger("18446744073709551615"));
  }

  public static void checkValue(SQLType sqlType, Value value) {
    switch (value.getType()) {
      case LONG:
        checkLongBound(sqlType, (LongValue) value);
        return;
      case UNSIGNEDLONG:
        checkUnsignedLongBound(sqlType, (UnsignedLongValue) value);
        return;
      default:
        break;
    }
  }

  static void checkLongBound(SQLType sqlType, LongValue value) {
    long result = value.getValue();
    if (sqlType.getUnsigned()) {
      long lower = 0;
      BigInteger upper = UNSIGNEDLONG_UPPERBOUND.get(sqlType.getType());
      if (result < lower) {
        throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OVER_FLOW_DOWN, sqlType.getType().name(),
                Long.toString(result));
      }
      if (BigInteger.valueOf(result).compareTo(upper) > 0) {
        throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OVER_FLOW_UP, sqlType.getType().name(),
                Long.toString(result));
      }
    } else {
      long lower = LONG_LOWERBOUND.get(sqlType.getType());
      long upper = LONG_UPPERBOUND.get(sqlType.getType());
      if (result < lower) {
        throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OVER_FLOW_DOWN, sqlType.getType().name(),
                Long.toString(result));
      }
      if (result > upper) {
        throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OVER_FLOW_UP, sqlType.getType().name(),
                Long.toString(result));
      }
    }
  }

  static void checkUnsignedLongBound(SQLType sqlType, UnsignedLongValue value) {
    BigInteger result = value.getValue();
    if (sqlType.getUnsigned()) {
      BigInteger lower = BigInteger.valueOf(0);
      BigInteger upper = UNSIGNEDLONG_UPPERBOUND.get(sqlType.getType());
      if (result.compareTo(lower) < 0) {
        throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OVER_FLOW_DOWN, sqlType.getType().name(),
                result.toString());
      }
      if (result.compareTo(upper) > 0) {
        throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OVER_FLOW_UP, sqlType.getType().name(),
                result.toString());
      }
    } else {
      long lower = LONG_LOWERBOUND.get(sqlType.getType());
      long upper = LONG_UPPERBOUND.get(sqlType.getType());
      if (result.compareTo(BigInteger.valueOf(lower)) < 0) {
        throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OVER_FLOW_DOWN, sqlType.getType().name(),
                result.toString());
      }
      if (result.compareTo(BigInteger.valueOf(upper)) > 0) {
        throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OVER_FLOW_UP, sqlType.getType().name(),
                result.toString());
      }
    }
  }

  public static BigInteger computeUpperBound(boolean unsigned, Basepb.DataType dataType) {
    BigInteger bigInteger;
    if (unsigned) {
      bigInteger = UNSIGNEDLONG_UPPERBOUND.get(dataType);
    } else {
      long longValue = LONG_UPPERBOUND.get(dataType);
      bigInteger = BigInteger.valueOf(longValue);
    }
    return bigInteger;
  }
}
