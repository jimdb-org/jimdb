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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.TimeZone;

import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.exception.JimException;
import io.jimdb.common.utils.lang.ByteUtil;
import io.jimdb.core.Session;
import io.jimdb.core.types.Types;
import io.jimdb.core.types.ValueType;
import io.jimdb.pb.Basepb.DataType;
import io.jimdb.pb.Metapb.SQLType;

import org.apache.commons.lang3.StringUtils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings({ "EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS", "UP_UNUSED_PARAMETER" })
public final class ValueConvertor {

  private ValueConvertor() {
  }

  public static Value convertType(final Session session, final Value value, final SQLType type) throws JimException {
    return convertType(session, value, Types.sqlToValueType(type), type);
  }

  public static Value convertType(final Session session, final Value value, final ValueType type,
                                  final SQLType sqlType) throws JimException {
    if (type == null || (value.getType() == type && ValueType.DECIMAL != value.getType())) {
      return value;
    }

    try {
      switch (type) {
        case NULL:
          return NullValue.getInstance();
        case STRING:
          return convertToString(session, value, sqlType);
        case LONG:
          return convertToLong(session, value, sqlType);
        case UNSIGNEDLONG:
          return convertToUnsignedLong(session, value, sqlType);
        case DOUBLE:
          return convertToDouble(session, value, sqlType);
        case DECIMAL:
          return convertToDecimal(session, value, sqlType);
        case DATE:
          return convertToDate(session, value, sqlType);
        case TIME:
          return convertToTime(session, value, sqlType);
        case YEAR:
          return convertToYear(session, value, sqlType);
        case BINARY:
          return convertToBinary(session, value, sqlType);
        case JSON:
          return convertToJson(session, value, sqlType);
        default:
          throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_NOT_SUPPORTED_YET, "ValueType(" + type.name() + ")");
      }
    } catch (JimException ex) {
      throw ex;
    } catch (Exception ex) {
      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_CONVERT_ERROR, ex, value.getType().name(),
              type.name());
    }
  }

  public static boolean convertToBool(final Session session, final Value src) {
    switch (src.getType()) {
      case NULL:
        return false;
      case LONG:
        return ((LongValue) src).getValue() != 0;
      case UNSIGNEDLONG:
      case DOUBLE:
      case DECIMAL:
        return src.signum() != 0;
      case STRING:
        return new BigDecimal(((StringValue) src).getValue()).signum() != 0;
      case BINARY:
        return new BigDecimal(new String(((BinaryValue) src).getValue(), Types.DEFAULT_CHARSET)).signum() != 0;
      case DATE:
        return ((DateValue) src).getValue() != null;
      case TIME:
        return ((TimeValue) src).getValue() != 0;
      default:
        throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_CONVERT_ERROR, src.getType().name(),
                "Boolean");
    }
  }

  public static StringValue convertToString(final Session session, final Value src, final SQLType type) {
    final String str;
    switch (src.getType()) {
      case NULL:
        return StringValue.EMPTY;
      case STRING:
        return (StringValue) src;
      case DATE:
        TimeZone zone = session == null ? null : session.getStmtContext().getLocalTimeZone();
        str = ((DateValue) src).convertToString(type == null ? null : type.getType(), zone);
        break;
      case TIME:
        str = ((TimeValue) src).convertToString();
        break;
      case BINARY:
        Charset charset = type == null ? Types.DEFAULT_CHARSET : Charset.forName(type.getCharset());
        str = new String(((BinaryValue) src).getValue(), charset);
        break;
      default:
        str = src.getString();
    }

    return type == null ? StringValue.getInstance(str) : getStringWithType(str, type);
  }

  private static StringValue getStringWithType(String str, SQLType type) {
    final long precision = type.getPrecision();
    if (precision > 0) {
      if (str.length() > precision) {
        throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_TOO_LONG, String.valueOf(precision),
                String.valueOf(str.length()));
      } else if (type.getType() == DataType.Char && str.length() < precision) {
        str = StringUtils.rightPad(str, (int) precision - str.length());
      }
    }

    return StringValue.getInstance(str);
  }

  public static LongValue convertToLong(final Session session, final Value src, final SQLType sqlType) {
    final long result;
    switch (src.getType()) {
      case NULL:
        return LongValue.getInstance(0);
      case LONG:
        return (LongValue) src;
      case UNSIGNEDLONG:
        ValueChecker.checkUnsignedLongBound(sqlType, (UnsignedLongValue) src);
        result = ((UnsignedLongValue) src).getValue().longValue();
        break;
      case DOUBLE:
        result = Types.toLong(((DoubleValue) src).getValue());
        break;
      case DECIMAL:
        result = Types.toLong(((DecimalValue) src).getValue());
        break;
      case BINARY:
        final byte[] val = ((BinaryValue) src).getValue();
        if (val.length <= 8) {
          result = ByteUtil.bytesToLong(val);
        } else {
          result = Long.parseLong(src.getString(), 16);
        }
        break;
      default:
        // str may be a decimal ,so convert to bigDecimal first e.g 1.1
        BigInteger toBigInteger = strToBigInteger((StringValue) src);
        result = toBigInteger.longValue();
    }
    return LongValue.getInstance(result);
  }

  public static UnsignedLongValue convertToUnsignedLong(final Session session, final Value src, final SQLType sqlType) {

    final BigInteger result;
    switch (src.getType()) {
      case NULL:
        return UnsignedLongValue.getInstance(BigInteger.ZERO);
      case UNSIGNEDLONG:
        return (UnsignedLongValue) src;
      case LONG:
        result = BigInteger.valueOf(((LongValue) src).getValue());
        break;
      case DOUBLE:
        result = BigInteger.valueOf(Types.toLong(((DoubleValue) src).getValue()));
        break;
      case DECIMAL:
        result = ((DecimalValue) src).getValue().toBigInteger();
        break;
      case BINARY:
        result = new BigInteger(((BinaryValue) src).getValue());
        break;
      default:
        result = strToBigInteger((StringValue) src);
    }
    return UnsignedLongValue.getInstance(result);
  }

  public static DoubleValue convertToDouble(final Session session, final Value src, final SQLType type) {
    final double result;
    switch (src.getType()) {
      case NULL:
        return DoubleValue.ZERO;
      case LONG:
        result = ((LongValue) src).getValue();
        break;
      case UNSIGNEDLONG:
        result = ((UnsignedLongValue) src).getValue().doubleValue();
        break;
      case DOUBLE:
        return (DoubleValue) src;
      case DECIMAL:
        result = ((DecimalValue) src).getValue().doubleValue();
        break;

      case BINARY:
        result = ByteBuffer.wrap(src.toByteArray()).getDouble();
        break;

      case YEAR:
        result = (double) ((YearValue) src).formatToLong();
        break;
      case DATE:
        result = (double) ((DateValue) src).formatToLong();
        break;
      case TIME:
        result = (double) ((TimeValue) src).formatToLong();
        break;
      default:
        result = Double.parseDouble(src.getString().trim());
    }

    return type == null ? DoubleValue.getInstance(result) : getDoubleWithType(result, type);
  }

  private static DoubleValue getDoubleWithType(double val, SQLType type) {
    int precision = (int) type.getPrecision();
    int scale = type.getScale();
    if (precision > 0 && scale >= 0) {
      if (Double.isNaN(val)) {
        throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OVER_FLOW, type.getType().name(), "NAN");
      }

      if (!Double.isInfinite(val)) {
        val = Types.roundDouble(val, scale);
      }
      double maxDouble = Types.maxDouble(precision, scale);
      if (val > maxDouble) {
        throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OVER_FLOW, type.getType().name(),
                String.valueOf(val));
      } else if (val < -maxDouble) {
        throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OVER_FLOW, type.getType().name(),
                String.valueOf(val));
      }
    }

    if (type.getUnsigned() && val < 0) {
      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OVER_FLOW, type.getType().name(),
              String.valueOf(val));
    }
    return DoubleValue.getInstance(val);
  }

  public static DecimalValue convertToDecimal(final Session session, final Value src, final SQLType type) {
    final BigDecimal result;
    switch (src.getType()) {
      case NULL:
        return DecimalValue.ZERO;
      case LONG:
        result = BigDecimal.valueOf(((LongValue) src).getValue());
        break;
      case UNSIGNEDLONG:
        result = new BigDecimal(((UnsignedLongValue) src).getValue());
        break;
      case DOUBLE:
        final double d = ((DoubleValue) src).getValue();
        if (Double.isInfinite(d) || Double.isNaN(d)) {
          throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OVER_FLOW, type.getType().name(),
                  String.valueOf(d));
        }
        result = BigDecimal.valueOf(d);
        break;
      case DECIMAL:
        result = ((DecimalValue) src).getValue();
        break;
      default:
        result = new BigDecimal(src.getString().trim());
    }

    return type == null ? DecimalValue.getInstance(result, result.precision(), result.scale()) : getDecimalWithType(result, type);
  }

  private static DecimalValue getDecimalWithType(BigDecimal val, SQLType type) {
    int precision = (int) type.getPrecision();
    int scale = type.getScale();
    if (precision > 0 && scale >= 0) {
      int frac = val.scale();
      int prec = val.precision() > frac ? val.precision() : frac;
      if ((prec - frac) > (precision - scale) && val.compareTo(BigDecimal.ZERO) != 0) {
        throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OVER_FLOW, String.format("DECIMAL(%d,%d)",
                precision, scale), val.toString());
      } else if (frac != scale) {
        val = val.setScale(scale, RoundingMode.HALF_EVEN);
      }
    }

    if (type.getUnsigned() && val.signum() < 0) {
      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OVER_FLOW, String.format("DECIMAL(%d,%d)",
              precision, scale), val.toString());
    }
    return DecimalValue.getInstance(val, (int) type.getPrecision(), type.getScale());
  }

  public static DateValue convertToDate(final Session session, final Value src, final SQLType type) {
    switch (src.getType()) {
      case DATE:
        return (DateValue) src;
      case NULL:
        return DateValue.DATE_EMPTY;
      case LONG:
        return DateValue.convertToDateValue(((LongValue) src).getValue(), type.getType(), type.getScale());
      case TIME:
        return DateValue.convertToDateValue((TimeValue) src, type.getType(), type.getScale());
      default:
        TimeZone zone = session == null ? null : session.getStmtContext().getLocalTimeZone();
        return DateValue.getInstance(src.getString().trim(), type.getType(), type.getScale(), zone);
    }
  }

  public static TimeValue convertToTime(final Session session, final Value src, final SQLType type) {
    switch (src.getType()) {
      case TIME:
        return (TimeValue) src;
      case NULL:
        return TimeValue.TIME_EMPTY;
      case LONG:
        return TimeValue.convertToTimeValue(((LongValue) src).getValue(), type.getScale());
      case DATE:
        return TimeValue.convertToTime((DateValue) src);
      default:
        return TimeValue.getInstance(src.getString().trim(), type.getScale());
    }
  }

  public static YearValue convertToYear(final Session session, final Value src, final SQLType sqlType) {
    switch (src.getType()) {
      case YEAR:
        return (YearValue) src;
      case NULL:
        return YearValue.ZERO;
      case LONG:
        return YearValue.getInstance((int) ((LongValue) src).getValue());
      default:
        return YearValue.getInstance(src.getString().trim());
    }
  }

  public static BinaryValue convertToBinary(final Session session, final Value src, final SQLType type) {
    switch (src.getType()) {
      case NULL:
        return BinaryValue.EMPTY;
      case BINARY:
        return (BinaryValue) src;
      case LONG:
        return BinaryValue.getInstance(ByteUtil.longToBytes(((LongValue) src).getValue()));
      case UNSIGNEDLONG:
        return BinaryValue.getInstance(((UnsignedLongValue) src).getValue().toByteArray());

      default:
        return BinaryValue.getInstance(src.getString().getBytes(Types.DEFAULT_CHARSET));
    }
  }

  public static JsonValue convertToJson(final Session session, final Value src, final SQLType type) {
    throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_NOT_SUPPORTED_YET, "Json");
  }

  public static SQLType convertToSafe(final SQLType sqlType) {
    switch (sqlType.getType()) {
      case TinyInt:
      case SmallInt:
      case MediumInt:
      case Int:
      case BigInt:
        return Types.buildSQLType(DataType.BigInt);

      case Float:
      case Double:
      case Varchar:
      case Char:
      case NChar:
      case Text:
        return Types.buildSQLType(sqlType.getType());

      default:
        return sqlType;
    }
  }

  /**
   * convert String to BigDecimal first then to BigInteger
   *
   * @param stringValue
   * @return
   */
  public static BigInteger strToBigInteger(StringValue stringValue) {
    String orgStr = stringValue.getString();
    BigDecimal bigDecimal = new BigDecimal(orgStr.trim());
    return bigDecimal.toBigInteger();
  }
}
