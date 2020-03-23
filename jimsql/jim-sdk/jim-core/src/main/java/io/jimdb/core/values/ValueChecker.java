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

import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.pb.Basepb;
import io.jimdb.pb.Metapb.SQLType;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * check value bound
 *
 * @version V1.0
 */
@SuppressFBWarnings("NAB_NEEDLESS_BOX_TO_UNBOX")
public class ValueChecker {

  private static final long[] SIGNED_INTEGER_LOWER_BOUND;
  private static final long[] SIGNED_INTEGER_UPPER_BOUND;
  private static final long[] UNSIGNED_INTEGER_UPPER_BOUND;
  private static final BigInteger[] SIGNED_LONG_LOWER_BOUND;
  private static final BigInteger[] SIGNED_LONG_UPPER_BOUND;
  private static final BigInteger[] UNSIGNED_LONG_UPPER_BOUND;
  private static final BigInteger BIG_INTEGER_ZERO = BigInteger.valueOf(0);

  static {
    int maxLength = Basepb.DataType.Null_VALUE;
    SIGNED_INTEGER_LOWER_BOUND = new long[maxLength];
    SIGNED_INTEGER_LOWER_BOUND[Basepb.DataType.TinyInt.ordinal()] = Long.valueOf(Byte.MIN_VALUE);
    SIGNED_INTEGER_LOWER_BOUND[Basepb.DataType.SmallInt.ordinal()] = Long.valueOf(Short.MIN_VALUE);
    SIGNED_INTEGER_LOWER_BOUND[Basepb.DataType.MediumInt.ordinal()] = Long.valueOf(~((1 << 23) - 1));   //-8388608
    SIGNED_INTEGER_LOWER_BOUND[Basepb.DataType.Int.ordinal()] = Long.valueOf(Integer.MIN_VALUE);
    SIGNED_INTEGER_LOWER_BOUND[Basepb.DataType.BigInt.ordinal()] = Long.MIN_VALUE;  // - 2^63

    SIGNED_INTEGER_UPPER_BOUND = new long[maxLength];
    SIGNED_INTEGER_UPPER_BOUND[Basepb.DataType.TinyInt.ordinal()] = Long.valueOf(Byte.MAX_VALUE);
    SIGNED_INTEGER_UPPER_BOUND[Basepb.DataType.SmallInt.ordinal()] = Long.valueOf(Short.MAX_VALUE);
    SIGNED_INTEGER_UPPER_BOUND[Basepb.DataType.MediumInt.ordinal()] = Long.valueOf((1 << 23) - 1);   //8388607
    SIGNED_INTEGER_UPPER_BOUND[Basepb.DataType.Int.ordinal()] = Long.valueOf(Integer.MAX_VALUE);
    SIGNED_INTEGER_UPPER_BOUND[Basepb.DataType.BigInt.ordinal()] = Long.MAX_VALUE;  //2^63 - 1

    UNSIGNED_INTEGER_UPPER_BOUND = new long[maxLength];
    UNSIGNED_INTEGER_UPPER_BOUND[Basepb.DataType.TinyInt.ordinal()] = (1 << 8) - 1;
    UNSIGNED_INTEGER_UPPER_BOUND[Basepb.DataType.SmallInt.ordinal()] = (1 << 16) - 1;
    UNSIGNED_INTEGER_UPPER_BOUND[Basepb.DataType.MediumInt.ordinal()] = (1 << 24) - 1;
    UNSIGNED_INTEGER_UPPER_BOUND[Basepb.DataType.Int.ordinal()] = (1L << 32) - 1;

    SIGNED_LONG_LOWER_BOUND = new BigInteger[maxLength];
    SIGNED_LONG_LOWER_BOUND[Basepb.DataType.TinyInt.ordinal()] = BigInteger.valueOf(Byte.MIN_VALUE);
    SIGNED_LONG_LOWER_BOUND[Basepb.DataType.SmallInt.ordinal()] = BigInteger.valueOf(Short.MIN_VALUE);
    SIGNED_LONG_LOWER_BOUND[Basepb.DataType.MediumInt.ordinal()] = BigInteger.valueOf(~((1 << 23) - 1));   //-8388608
    SIGNED_LONG_LOWER_BOUND[Basepb.DataType.Int.ordinal()] = BigInteger.valueOf(Integer.MIN_VALUE);
    SIGNED_LONG_LOWER_BOUND[Basepb.DataType.BigInt.ordinal()] = BigInteger.valueOf(Long.MIN_VALUE);  // - 2^63

    SIGNED_LONG_UPPER_BOUND = new BigInteger[maxLength];
    SIGNED_LONG_UPPER_BOUND[Basepb.DataType.TinyInt.ordinal()] = BigInteger.valueOf(Byte.MAX_VALUE);
    SIGNED_LONG_UPPER_BOUND[Basepb.DataType.SmallInt.ordinal()] = BigInteger.valueOf(Short.MAX_VALUE);
    SIGNED_LONG_UPPER_BOUND[Basepb.DataType.MediumInt.ordinal()] = BigInteger.valueOf((1 << 23) - 1);   //8388607
    SIGNED_LONG_UPPER_BOUND[Basepb.DataType.Int.ordinal()] = BigInteger.valueOf(Integer.MAX_VALUE);
    SIGNED_LONG_UPPER_BOUND[Basepb.DataType.BigInt.ordinal()] = BigInteger.valueOf(Long.MAX_VALUE);  //2^63 - 1

    UNSIGNED_LONG_UPPER_BOUND = new BigInteger[maxLength];
    UNSIGNED_LONG_UPPER_BOUND[Basepb.DataType.TinyInt.ordinal()] = BigInteger.valueOf((1 << 8) - 1);
    UNSIGNED_LONG_UPPER_BOUND[Basepb.DataType.SmallInt.ordinal()] = BigInteger.valueOf((1 << 16) - 1);
    UNSIGNED_LONG_UPPER_BOUND[Basepb.DataType.MediumInt.ordinal()] = BigInteger.valueOf((1 << 24) - 1);
    UNSIGNED_LONG_UPPER_BOUND[Basepb.DataType.Int.ordinal()] = BigInteger.valueOf((1L << 32) - 1);
    UNSIGNED_LONG_UPPER_BOUND[Basepb.DataType.BigInt.ordinal()] = new BigInteger("18446744073709551615");   // 2^64 - 1
    // A type of BIT(M) enables storage of M-bit values. M can range from 1 to 64
    UNSIGNED_LONG_UPPER_BOUND[Basepb.DataType.Bit.ordinal()] = new BigInteger("18446744073709551615");

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
        checkCharBound(sqlType, value, ((StringValue) value).getLength());
        return;
      case BINARY:
        checkCharBound(sqlType, value, ((BinaryValue) value).getValue().length);
        return;
      default:
        break;
    }
  }

  private static void checkCharBound(SQLType fieldSqlType, Value value, int length) {
    if (fieldSqlType.getPrecision() < length) {
      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_TOO_LONG,
              String.valueOf(fieldSqlType.getPrecision()),
              String.valueOf(length));
    }

    Basepb.DataType dataType = fieldSqlType.getType();
    switch (dataType) {
      case Char:
      case Binary:
      case TinyBlob:
      case TinyText:
        if (length > 255) {
          throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OVER_FLOW_UP,
                  dataType.name(), value.getString());
        }
        return;
      case Varchar:
      case VarBinary:
      case Blob:
      case Text:
        if (length > 65535) {
          throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OVER_FLOW_UP,
                  dataType.name(), value.getString());
        }
        return;
      case MediumBlob:
      case MediumText:
        if (length > 16777215) {
          throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OVER_FLOW_UP,
                  dataType.name(), value.getString());
        }
        return;
      case LongBlob:
      case LongText:
        // The length of LONGBLOB actually is 4G(4294967296),
        // but the length is only less than 2G because of the range of int.
        if (length > (Integer.MAX_VALUE - 1)) {
          throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OVER_FLOW_UP,
                  fieldSqlType.getType().name(), value.getString());
        }
        return;
      default:
        return;

    }
  }

  private static void checkLongBound(SQLType fieldSqlType, LongValue value) {
    long result = value.getValue();
    if (fieldSqlType.getUnsigned()) {
      long lower = 0;
      long upper = UNSIGNED_INTEGER_UPPER_BOUND[fieldSqlType.getTypeValue()];
      if (result < lower) {
        throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OVER_FLOW_DOWN, fieldSqlType.getType().name(),
                Long.toString(result));
      }
      if (result > upper) {
        throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OVER_FLOW_UP, fieldSqlType.getType().name(),
                Long.toString(result));
      }
    } else {
      long lower = SIGNED_INTEGER_LOWER_BOUND[fieldSqlType.getTypeValue()];
      long upper = SIGNED_INTEGER_UPPER_BOUND[fieldSqlType.getTypeValue()];
      if (result < lower) {
        throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OVER_FLOW_DOWN, fieldSqlType.getType().name(),
                Long.toString(result));
      }
      if (result > upper) {
        throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OVER_FLOW_UP, fieldSqlType.getType().name(),
                Long.toString(result));
      }
    }
  }

  static void checkUnsignedLongBound(SQLType fieldSqlType, UnsignedLongValue value) {
    BigInteger result = value.getValue();
    if (fieldSqlType.getUnsigned()) {
      BigInteger lower = BIG_INTEGER_ZERO;
      BigInteger upper = UNSIGNED_LONG_UPPER_BOUND[fieldSqlType.getTypeValue()];
      if (result.compareTo(lower) < 0) {
        throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OVER_FLOW_DOWN, fieldSqlType.getType().name(),
                result.toString());
      }
      if (result.compareTo(upper) > 0) {
        throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OVER_FLOW_UP, fieldSqlType.getType().name(),
                result.toString());
      }
    } else {
      BigInteger lower = SIGNED_LONG_LOWER_BOUND[fieldSqlType.getTypeValue()];
      BigInteger upper = SIGNED_LONG_UPPER_BOUND[fieldSqlType.getTypeValue()];
      if (result.compareTo(lower) < 0) {
        throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OVER_FLOW_DOWN, fieldSqlType.getType().name(),
                result.toString());
      }
      if (result.compareTo(upper) > 0) {
        throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OVER_FLOW_UP, fieldSqlType.getType().name(),
                result.toString());
      }
    }
  }

  public static BigInteger getUpperBound(boolean unsigned, Basepb.DataType dataType) {
    BigInteger bigInteger;
    if (unsigned) {
      bigInteger = UNSIGNED_LONG_UPPER_BOUND[dataType.ordinal()];
    } else {
      bigInteger = SIGNED_LONG_UPPER_BOUND[dataType.ordinal()];
    }
    return bigInteger;
  }
}
