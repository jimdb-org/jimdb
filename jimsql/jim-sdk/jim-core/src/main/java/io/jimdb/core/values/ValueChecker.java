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

  private static final EnumMap<Basepb.DataType, Long> SIGNED_LONG_LOWER_BOUND;
  private static final EnumMap<Basepb.DataType, Long> SIGNED_LONG_UPPER_BOUND;
  private static final EnumMap<Basepb.DataType, BigInteger> UNSIGNED_LONG_UPPER_BOUND;

  static {
    SIGNED_LONG_LOWER_BOUND = new EnumMap<>(Basepb.DataType.class);
    SIGNED_LONG_LOWER_BOUND.put(Basepb.DataType.TinyInt, Long.valueOf(Byte.MIN_VALUE));
    SIGNED_LONG_LOWER_BOUND.put(Basepb.DataType.SmallInt, Long.valueOf(Short.MIN_VALUE));
    SIGNED_LONG_LOWER_BOUND.put(Basepb.DataType.MediumInt, Long.valueOf(~((1 << 23) - 1))); //-8388608
    SIGNED_LONG_LOWER_BOUND.put(Basepb.DataType.Int, Long.valueOf(Integer.MIN_VALUE));
    SIGNED_LONG_LOWER_BOUND.put(Basepb.DataType.BigInt, Long.MIN_VALUE); // - 2^63

    SIGNED_LONG_UPPER_BOUND = new EnumMap<>(Basepb.DataType.class);
    SIGNED_LONG_UPPER_BOUND.put(Basepb.DataType.TinyInt, Long.valueOf(Byte.MAX_VALUE));
    SIGNED_LONG_UPPER_BOUND.put(Basepb.DataType.SmallInt, Long.valueOf(Short.MAX_VALUE));
    SIGNED_LONG_UPPER_BOUND.put(Basepb.DataType.MediumInt, Long.valueOf((1 << 23) - 1)); //8388607
    SIGNED_LONG_UPPER_BOUND.put(Basepb.DataType.Int, Long.valueOf(Integer.MAX_VALUE));
    SIGNED_LONG_UPPER_BOUND.put(Basepb.DataType.BigInt, Long.MAX_VALUE); //2^63 - 1

    UNSIGNED_LONG_UPPER_BOUND = new EnumMap<>(Basepb.DataType.class);
    UNSIGNED_LONG_UPPER_BOUND.put(Basepb.DataType.TinyInt, BigInteger.valueOf((1 << 8) - 1));
    UNSIGNED_LONG_UPPER_BOUND.put(Basepb.DataType.SmallInt, BigInteger.valueOf((1 << 16) - 1));
    UNSIGNED_LONG_UPPER_BOUND.put(Basepb.DataType.MediumInt, BigInteger.valueOf((1 << 24) - 1));
    UNSIGNED_LONG_UPPER_BOUND.put(Basepb.DataType.Int, BigInteger.valueOf((1L << 32) - 1));
    UNSIGNED_LONG_UPPER_BOUND.put(Basepb.DataType.BigInt, new BigInteger("18446744073709551615")); // 2^64 - 1
    // A type of BIT(M) enables storage of M-bit values. M can range from 1 to 64
    UNSIGNED_LONG_UPPER_BOUND.put(Basepb.DataType.Bit, new BigInteger("18446744073709551615"));
  }

  public static void checkValue(SQLType sqlType, Value value) {
    if (sqlType == null) {
      return;
    }
    switch (value.getType()) {
      case LONG:
        checkLongBound(sqlType, (LongValue) value);
        return;
      case UNSIGNEDLONG:
        checkUnsignedLongBound(sqlType, (UnsignedLongValue) value);
        return;
      case STRING:
        checkStringBound(sqlType, (StringValue) value);
        return;
      case BINARY:
        checkBinaryBound(sqlType, (BinaryValue) value);
        return;
      default:
        break;
    }
  }


  private static void checkBinaryBound(SQLType sqlType, BinaryValue value) {
    int length = value.getValue().length;
    // check the length of value
    if (sqlType.getPrecision() < length) {
      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_TOO_LONG,
              String.valueOf(sqlType.getPrecision()),
              String.valueOf(length));
    }
    checkCharBound(sqlType, value, length);
  }

  private static void checkStringBound(SQLType sqlType, StringValue value) {
    int length = value.getLength();
    // check the length of value
    if (sqlType.getPrecision() < length) {
      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_TOO_LONG,
              String.valueOf(sqlType.getPrecision()),
              String.valueOf(length));
    }
    checkCharBound(sqlType, value, length);
  }

  private static void checkCharBound(SQLType sqlType, Value value, int length) {
    Basepb.DataType dataType = sqlType.getType();
    switch (dataType) {
      case Char:
      case Binary:
      case TinyBlob:
      case TinyText:
        if (length > 255) {
          throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OVER_FLOW_UP,
                  sqlType.getType().name(), value.getString());
        }
        return;
      case Varchar:
      case VarBinary:
      case Blob:
      case Text:
        if (length > 65535) {
          throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OVER_FLOW_UP,
                  sqlType.getType().name(), value.getString());
        }
        return;
      case MediumBlob:
      case MediumText:
        if (length > 16777215) {
          throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OVER_FLOW_UP,
                  sqlType.getType().name(), value.getString());
        }
        return;
      case LongBlob:
      case LongText:
        // The length of LONGBLOB actually is 4G(4294967296),
        // but the length is only less than 2G because of the range of int.
        if (length > (Integer.MAX_VALUE - 1)) {
          throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OVER_FLOW_UP,
                  sqlType.getType().name(), value.getString());
        }
        return;
      default:
        return;

    }
  }

  private static void checkLongBound(SQLType sqlType, LongValue value) {
    long result = value.getValue();
    if (sqlType.getUnsigned()) {
      long lower = 0;
      BigInteger upper = UNSIGNED_LONG_UPPER_BOUND.get(sqlType.getType());
      if (result < lower) {
        throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OVER_FLOW_DOWN, sqlType.getType().name(),
                Long.toString(result));
      }
      if (BigInteger.valueOf(result).compareTo(upper) > 0) {
        throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OVER_FLOW_UP, sqlType.getType().name(),
                Long.toString(result));
      }
    } else {
      long lower = SIGNED_LONG_LOWER_BOUND.get(sqlType.getType());
      long upper = SIGNED_LONG_UPPER_BOUND.get(sqlType.getType());
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
      BigInteger upper = UNSIGNED_LONG_UPPER_BOUND.get(sqlType.getType());
      if (result.compareTo(lower) < 0) {
        throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OVER_FLOW_DOWN, sqlType.getType().name(),
                result.toString());
      }
      if (result.compareTo(upper) > 0) {
        throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OVER_FLOW_UP, sqlType.getType().name(),
                result.toString());
      }
    } else {
      long lower = SIGNED_LONG_LOWER_BOUND.get(sqlType.getType());
      long upper = SIGNED_LONG_UPPER_BOUND.get(sqlType.getType());
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
      bigInteger = UNSIGNED_LONG_UPPER_BOUND.get(dataType);
    } else {
      long longValue = SIGNED_LONG_UPPER_BOUND.get(dataType);
      bigInteger = BigInteger.valueOf(longValue);
    }
    return bigInteger;
  }
}
