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
package io.jimdb.core.types;

import static com.alibaba.druid.sql.ast.SQLDataType.Constants.BIGINT;
import static com.alibaba.druid.sql.ast.SQLDataType.Constants.BOOLEAN;
import static com.alibaba.druid.sql.ast.SQLDataType.Constants.BYTEA;
import static com.alibaba.druid.sql.ast.SQLDataType.Constants.CHAR;
import static com.alibaba.druid.sql.ast.SQLDataType.Constants.DATE;
import static com.alibaba.druid.sql.ast.SQLDataType.Constants.DECIMAL;
import static com.alibaba.druid.sql.ast.SQLDataType.Constants.INT;
import static com.alibaba.druid.sql.ast.SQLDataType.Constants.NCHAR;
import static com.alibaba.druid.sql.ast.SQLDataType.Constants.NUMBER;
import static com.alibaba.druid.sql.ast.SQLDataType.Constants.REAL;
import static com.alibaba.druid.sql.ast.SQLDataType.Constants.SMALLINT;
import static com.alibaba.druid.sql.ast.SQLDataType.Constants.TEXT;
import static com.alibaba.druid.sql.ast.SQLDataType.Constants.TIMESTAMP;
import static com.alibaba.druid.sql.ast.SQLDataType.Constants.TINYINT;
import static com.alibaba.druid.sql.ast.SQLDataType.Constants.VARCHAR;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.pb.Basepb;
import io.jimdb.pb.Basepb.DataType;
import io.jimdb.pb.Metapb;
import io.jimdb.pb.Metapb.SQLType;

import org.apache.commons.lang3.StringUtils;

import com.alibaba.druid.sql.ast.SQLDataType;
import com.alibaba.druid.sql.ast.SQLDataTypeImpl;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings({ "HARD_CODE_KEY", "OCP_OVERLY_CONCRETE_PARAMETER", "CLI_CONSTANT_LIST_INDEX", "CC_CYCLOMATIC_COMPLEXITY" })
public final class Types {
  public static final String PRIMARY_KEY_NAME = "PRIMARY";

  public static final int FLAG_KEY_PRIMARY = 1 << 1;
  public static final int FLAG_KEY_UNIQUE = 1 << 2;
  public static final int FLAG_KEY_MULTIPLE = 1 << 3;

  public static final long NANOSECOND = 1;
  public static final long MICROSECOND = 1000 * NANOSECOND;
  public static final long MILLISECOND = 1000 * MICROSECOND;
  public static final long SECOND = 1000 * MILLISECOND;
  public static final long MINUTE = 60 * SECOND;
  public static final long HOUR = 60 * MINUTE;
  public static final long DAY = 24 * HOUR;

  public static final String ZERO_DATETIME = "0000-00-00 00:00:00";
  public static final String ZERO_DATE = "0000-00-00";

  public static final BigDecimal MAX_DEC_SIGNEDLONG = BigDecimal.valueOf(Long.MAX_VALUE);
  public static final BigDecimal MIN_DEC_SIGNEDLONG = BigDecimal.valueOf(Long.MIN_VALUE);
  public static final BigInteger MAX_SIGNEDLONG = BigInteger.valueOf(Long.MAX_VALUE);
  public static final BigInteger MAX_UNSIGNEDLONG = new BigInteger("18446744073709551615");
  public static final BigInteger MIN_UNSIGNEDLONG = BigInteger.valueOf(0L);

  public static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
  public static final String DEFAULT_CHARSET_STR = DEFAULT_CHARSET.name();
  public static final String DEFAULT_COLLATE = "utf8_bin";
  private static final SQLType[] SQL_TYPES_SIGNED = initSQLType(false);
  private static final SQLType[] SQL_TYPES_UNSIGNED = initSQLType(true);
  public static final SQLType UNDEFINE_SQLTYPE = buildSQLType(DataType.Invalid);

  public static final int UNDEFINE_WIDTH = -1;
  public static final int NOTFIX_DEC_WIDTH = 31;
  public static final int MAX_INT_WIDTH = 20;
  public static final int MAX_REAL_WIDTH = 23;
  public static final int MAX_FLOAT_WIDTH = 255;
  public static final int MAX_FLOAT_SCALE = 30;
  public static final int MAX_DEC_WIDTH = 65;
  public static final int MAX_DEC_SCALE = 30;
  public static final int MAX_DATE_WIDTH = 10;
  public static final int MAX_DATETIME_NOFSP_WIDTH = 19;
  public static final int MAX_DATETIME_FSP_WIDTH = 26;
  public static final int MAX_DATETIME_WIDTH = 29;
  public static final int MAX_DATETIME_SCALE = 6;
  public static final int MAX_TIME_NOFSP_WIDTH = 10;
  public static final int MAX_TIME_FSP_WIDTH = 15;
  public static final int MAX_TIME_SCALE = 6;
  public static final int MAX_YEAR_WIDTH = 4;
  public static final int MAX_CHAR_WIDTH = 255;
  public static final int MAX_VARCHAR_WIDTH = 65535;
  public static final int MAX_MEDIUM_TEXT_WIDTH = 16777215;
  public static final long MAX_LONG_TEXT_WIDTH = 4294967295L;

  public static final int NOT_FIXED_DEC = 31;
  public static final int PREC_INCREMENT = 4;

  public static final String NOW_FUNC = "NOW";
  public static final String LOCALTIME = "LOCALTIME";
  public static final String LOCALTIME_FUNC = "LOCALTIME";
  public static final String LOCALTIMESTAMP = "LOCALTIMESTAMP";
  public static final String LOCALTIMESTAMP_FUNC = "LOCALTIMESTAMP";
  public static final String CURRENT_TIMESTAMP_FUNC = "CURRENT_TIMESTAMP";

  private static final Map<String, DataType> TYPE_MAP;
  private static final Map<DataType, String> TYPE_DESC_MAP;
  private static final Map<DataType, long[]> TYPE_DEFAULT_PRECISION_MAP;

  static {
    TYPE_MAP = new HashMap<>();
    TYPE_MAP.put("BIT", DataType.Bit);
    TYPE_MAP.put(INT, DataType.Int);
    TYPE_MAP.put(BIGINT, DataType.BigInt);
    TYPE_MAP.put("MEDIUMINT", DataType.MediumInt);
    TYPE_MAP.put(SMALLINT, DataType.SmallInt);
    TYPE_MAP.put(TINYINT, DataType.TinyInt);
    TYPE_MAP.put(BOOLEAN, DataType.TinyInt);
    TYPE_MAP.put("FLOAT", DataType.Float);
    TYPE_MAP.put("DOUBLE", DataType.Double);
    TYPE_MAP.put(REAL, DataType.Double);
    TYPE_MAP.put(DECIMAL, DataType.Decimal);
    TYPE_MAP.put(NUMBER, DataType.Decimal);
    TYPE_MAP.put(VARCHAR, DataType.Varchar);
    TYPE_MAP.put(DATE, DataType.Date);
    TYPE_MAP.put("DATETIME", DataType.DateTime);
    TYPE_MAP.put(TIMESTAMP, DataType.TimeStamp);
    TYPE_MAP.put("TIME", DataType.Time);
    TYPE_MAP.put("YEAR", DataType.Year);
    TYPE_MAP.put(CHAR, DataType.Char);
    TYPE_MAP.put(NCHAR, DataType.NChar);
    TYPE_MAP.put(BYTEA, DataType.Binary);
    TYPE_MAP.put("BINARY", DataType.Binary);
    TYPE_MAP.put("VARBINARY", DataType.VarBinary);
    TYPE_MAP.put("TINYBLOB", DataType.TinyBlob);
    TYPE_MAP.put("BLOB", DataType.Blob);
    TYPE_MAP.put("MEDIUMBLOB", DataType.MediumBlob);
    TYPE_MAP.put("LONGBLOB", DataType.LongBlob);
    TYPE_MAP.put(TEXT, DataType.Text);
    TYPE_MAP.put("TINYTEXT", DataType.TinyText);
    TYPE_MAP.put("MEDIUMTEXT", DataType.MediumText);
    TYPE_MAP.put("LONGTEXT", DataType.LongText);

    TYPE_DESC_MAP = new HashMap<>();
    TYPE_DESC_MAP.put(DataType.Invalid, "unspecified");
    TYPE_DESC_MAP.put(DataType.TinyInt, "tinyint");
    TYPE_DESC_MAP.put(DataType.SmallInt, "smallint");
    TYPE_DESC_MAP.put(DataType.MediumInt, "mediumint");
    TYPE_DESC_MAP.put(DataType.Int, "int");
    TYPE_DESC_MAP.put(DataType.BigInt, "bigint");
    TYPE_DESC_MAP.put(DataType.Bit, "bit");
    TYPE_DESC_MAP.put(DataType.Float, "float");
    TYPE_DESC_MAP.put(DataType.Double, "double");
    TYPE_DESC_MAP.put(DataType.Decimal, "decimal");
    TYPE_DESC_MAP.put(DataType.Date, "date");
    TYPE_DESC_MAP.put(DataType.TimeStamp, "timestamp");
    TYPE_DESC_MAP.put(DataType.DateTime, "datetime");
    TYPE_DESC_MAP.put(DataType.Time, "time");
    TYPE_DESC_MAP.put(DataType.Year, "year");
    TYPE_DESC_MAP.put(DataType.Varchar, "varchar");
    TYPE_DESC_MAP.put(DataType.Char, "char");
    TYPE_DESC_MAP.put(DataType.NChar, "char");
    TYPE_DESC_MAP.put(DataType.Binary, "binary");
    TYPE_DESC_MAP.put(DataType.VarBinary, "varbinary");
    TYPE_DESC_MAP.put(DataType.TinyBlob, "tinyblob");
    TYPE_DESC_MAP.put(DataType.Blob, "blob");
    TYPE_DESC_MAP.put(DataType.MediumBlob, "mediumblob");
    TYPE_DESC_MAP.put(DataType.LongBlob, "longblob");
    TYPE_DESC_MAP.put(DataType.TinyText, "tinytext");
    TYPE_DESC_MAP.put(DataType.Text, "text");
    TYPE_DESC_MAP.put(DataType.MediumText, "mediumtext");
    TYPE_DESC_MAP.put(DataType.LongText, "longtext");
    TYPE_DESC_MAP.put(DataType.Json, "json");

    TYPE_DEFAULT_PRECISION_MAP = new HashMap<>();
    TYPE_DEFAULT_PRECISION_MAP.put(DataType.Null, new long[]{ 0, 0 });
    TYPE_DEFAULT_PRECISION_MAP.put(DataType.Bit, new long[]{ 1, 0 });
    TYPE_DEFAULT_PRECISION_MAP.put(DataType.TinyInt, new long[]{ 4, 0 });
    TYPE_DEFAULT_PRECISION_MAP.put(DataType.SmallInt, new long[]{ 6, 0 });
    TYPE_DEFAULT_PRECISION_MAP.put(DataType.MediumInt, new long[]{ 9, 0 });
    TYPE_DEFAULT_PRECISION_MAP.put(DataType.Int, new long[]{ 11, 0 });
    TYPE_DEFAULT_PRECISION_MAP.put(DataType.BigInt, new long[]{ 20, 0 });
    TYPE_DEFAULT_PRECISION_MAP.put(DataType.Float, new long[]{ 12, -1 });
    TYPE_DEFAULT_PRECISION_MAP.put(DataType.Double, new long[]{ 22, -1 });
    TYPE_DEFAULT_PRECISION_MAP.put(DataType.Decimal, new long[]{ 10, 0 });
    TYPE_DEFAULT_PRECISION_MAP.put(DataType.Year, new long[]{ 4, 0 });
    TYPE_DEFAULT_PRECISION_MAP.put(DataType.Time, new long[]{ 10, 0 });
    TYPE_DEFAULT_PRECISION_MAP.put(DataType.Date, new long[]{ 10, 0 });
    TYPE_DEFAULT_PRECISION_MAP.put(DataType.DateTime, new long[]{ 19, 0 });
    TYPE_DEFAULT_PRECISION_MAP.put(DataType.TimeStamp, new long[]{ 19, 0 });
    TYPE_DEFAULT_PRECISION_MAP.put(DataType.Char, new long[]{ 0, 0 });
    TYPE_DEFAULT_PRECISION_MAP.put(DataType.NChar, new long[]{ 0, 0 });
    TYPE_DEFAULT_PRECISION_MAP.put(DataType.Varchar, new long[]{ 0, 0 });
    TYPE_DEFAULT_PRECISION_MAP.put(DataType.Binary, new long[]{ 1, 0 });
    TYPE_DEFAULT_PRECISION_MAP.put(DataType.TinyBlob, new long[]{ 255, 0 });
    TYPE_DEFAULT_PRECISION_MAP.put(DataType.Blob, new long[]{ 65535, 0 });
    TYPE_DEFAULT_PRECISION_MAP.put(DataType.MediumBlob, new long[]{ 16777215, 0 });
    TYPE_DEFAULT_PRECISION_MAP.put(DataType.LongBlob, new long[]{ 4294967295L, 0 });
    TYPE_DEFAULT_PRECISION_MAP.put(DataType.Text, new long[]{ 4294967295L, 0 });
    TYPE_DEFAULT_PRECISION_MAP.put(DataType.Json, new long[]{ 4294967295L, 0 });
  }

  /**
   * init SQL_TYPES_SIGNED and SQL_TYPES_UNSIGNED
   */
  private static SQLType[] initSQLType(boolean unsigned) {
    DataType[] dataTypes = DataType.values();
    int maxOrdinal = 0;
    for (DataType dataType : dataTypes) {
      if (dataType.ordinal() > maxOrdinal) {
        maxOrdinal = dataType.ordinal();
      }
    }

    SQLType[] sqlTypes = new SQLType[maxOrdinal];
    for (DataType dataType : dataTypes) {
      if (dataType != DataType.UNRECOGNIZED) {
        sqlTypes[dataType.ordinal()] = createSQLType(dataType, unsigned);
      }
    }
    return sqlTypes;
  }

  private static SQLType createSQLType(DataType type, boolean unsigned) {
    SQLType.Builder builder = SQLType.newBuilder();
    builder.setType(type)
            .setUnsigned(unsigned)
            .setScale(UNDEFINE_WIDTH)
            .setPrecision(UNDEFINE_WIDTH)
            .setCharset(DEFAULT_CHARSET_STR)
            .setCollate(DEFAULT_COLLATE);
    return builder.build();
  }

  public static SQLType buildSQLType(SQLDataType sqlType, String colName) {
    if (sqlType == null) {
      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_PARSE_NO_TYPE_ERROR, "after " + colName);
    }
    String typeName = sqlType.getName().toUpperCase();
    DataType dt = TYPE_MAP.get(typeName);
    if (dt == null) {
      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_NOT_SUPPORTED_YET, "SqlType(" + typeName + ")");
    }

    if (StringUtils.isBlank(colName)) {
      return buildSQLType(dt);
    }

    boolean unsigned = false;
    boolean zerofill = false;
    if (sqlType instanceof SQLDataTypeImpl) {
      SQLDataTypeImpl sqlTypeImpl = (SQLDataTypeImpl) sqlType;
      unsigned = sqlTypeImpl.isUnsigned();
      zerofill = sqlTypeImpl.isZerofill();
    }
    if (zerofill) {
      switch (dt) {
        case TinyInt:
        case SmallInt:
        case MediumInt:
        case Int:
        case BigInt:
        case Bit:
        case Decimal:
        case Float:
        case Double:
          unsigned = true;
          break;
        default:
          break;
      }
    }

    DataType[] dts = new DataType[]{ dt };

    long[] precisionAndScale = buildPrecisionAndScale(sqlType.getArguments(), dts, typeName, colName);
    return SQLType.newBuilder().setType(dts[0])
            .setPrecision(precisionAndScale[0])
            .setScale((int) precisionAndScale[1])
            .setCharset(DEFAULT_CHARSET_STR)
            .setCollate(DEFAULT_COLLATE)
            .setUnsigned(unsigned)
            .setZerofill(zerofill)
            .build();
  }

  private static void validPrecisionBound(long precision, long bound, ErrorCode errorCode, String... params) {
    if (precision > bound) {
      throw DBException.get(ErrorModule.EXPR, errorCode, params);
    }
  }

  private static void validScaleBound(int scale, int bound, ErrorCode errorCode, String... params) {
    if (scale > bound) {
      throw DBException.get(ErrorModule.EXPR, errorCode, params);
    }
  }

  private static long[] buildPrecisionAndScale(List<SQLExpr> argList, DataType[] dts, String typeName, String colName) {
    if (argList.isEmpty()) {
      long[] defaults = TYPE_DEFAULT_PRECISION_MAP.get(dts[0]);
      if (defaults != null) {
        return defaults;
      }
      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_PARSE_TYPE_ERROR, typeName + "(" + argList + ")");
    }

    if (argList.size() > 2) {
      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_PARSE_TYPE_TWO_ERROR, typeName + "(" + argList + ")");
    }

    long precision = UNDEFINE_WIDTH;
    int scale = UNDEFINE_WIDTH;
    switch (dts[0]) {
      //tiny/small/medium/int/bigint(M), 0~255
      case TinyInt:
      case SmallInt:
      case MediumInt:
      case Int:
      case BigInt:
        precision = getArg0(argList, typeName);
        validArgNeg(typeName, precision, argList);
        validPrecisionBound(precision, 255, ErrorCode.ER_TOO_BIG_DISPLAYWIDTH, colName, "255");
        break;
      //bit(M), 1~64
      case Bit:
        precision = getArg0(argList, typeName);
        precision = precision == 0 ? 1 : precision;
        validArgNeg(typeName, precision, argList);
        validPrecisionBound(precision, 64, ErrorCode.ER_TOO_BIG_DISPLAYWIDTH, colName, "64");
        break;
      //decimal(M,D), the default of decimal is (10,0);
      case Decimal:
        scale = 0;
        precision = ((SQLIntegerExpr) argList.get(0)).getNumber().longValue();
        validArgNeg(typeName, precision, argList);
        validPrecisionBound(precision, 65, ErrorCode.ER_TOO_BIG_PRECISION, Long.toString(precision), colName, "65");

        if (argList.size() == 2) {
          scale = ((SQLIntegerExpr) argList.get(1)).getNumber().intValue();
          validArgNeg(typeName, scale, argList);
          validScaleBound(scale, 30, ErrorCode.ER_TOO_BIG_SCALE, Integer.toString(scale), colName, "30");
          validScaleBound(scale, (int) precision, ErrorCode.ER_M_BIGGER_THAN_D, typeName);
        }
        break;
      //float(M,D)
      //don't check float default
      //float: If M and D are omitted, values are stored to the limits permitted by the hardware
      case Float:
        precision = ((SQLIntegerExpr) argList.get(0)).getNumber().longValue();
        validArgNeg(typeName, precision, argList);

        if (argList.size() == 2) {
          scale = ((SQLIntegerExpr) argList.get(1)).getNumber().intValue();
          validArgNeg(typeName, scale, argList);
          // float (M,D)
          validPrecisionBound(precision, 255, ErrorCode.ER_TOO_BIG_DISPLAYWIDTH, colName, "255");
          validScaleBound(scale, 30, ErrorCode.ER_TOO_BIG_SCALE, Integer.toString(scale), colName, "30");
          validScaleBound(scale, (int) precision, ErrorCode.ER_M_BIGGER_THAN_D, typeName);
        } else {
          // float (p)
          if (precision >= 24 && precision < 53) {
            dts[0] = DataType.Double;
          } else {
            throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_WRONG_FIELD_SPEC, colName);
          }
        }
        break;
      //double(M,D)
      //don't check double default
      case Double:
        precision = ((SQLIntegerExpr) argList.get(0)).getNumber().longValue();
        validArgNeg(typeName, precision, argList);

        if (argList.size() == 2) {
          scale = ((SQLIntegerExpr) argList.get(1)).getNumber().intValue();
          validArgNeg(typeName, scale, argList);
          // double (M,D)
          validPrecisionBound(precision, 255, ErrorCode.ER_TOO_BIG_DISPLAYWIDTH, colName, "255");
          validScaleBound(scale, 30, ErrorCode.ER_TOO_BIG_SCALE, Integer.toString(scale), colName, "30");
          validScaleBound(scale, (int) precision, ErrorCode.ER_M_BIGGER_THAN_D, typeName);
        } else {
          // double (p)
          throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_PARSE_TYPE_ERROR, typeName + "(" + argList + ")");
        }
        break;
      //char[M], 0~255
      case Char:
      case NChar:
      case Binary:
      case TinyBlob:
      case TinyText:
        precision = getArg0(argList, typeName);
        validArgNeg(typeName, precision, argList);
        validPrecisionBound(precision, MAX_CHAR_WIDTH, ErrorCode.ER_TOO_BIG_FIELDLENGTH, colName, Integer.toString(MAX_CHAR_WIDTH));
        break;
      //varchar[M], 0~65,535
      case Varchar:
      case VarBinary:
      case Blob:
      case Text:
        precision = getArg0(argList, typeName);
        validArgNeg(typeName, precision, argList);
        validPrecisionBound(precision, MAX_VARCHAR_WIDTH, ErrorCode.ER_TOO_BIG_FIELDLENGTH, colName, Integer.toString(MAX_VARCHAR_WIDTH));
        break;
      case MediumBlob:
      case MediumText:
        precision = getArg0(argList, typeName);
        validArgNeg(typeName, precision, argList);
        validPrecisionBound(precision, MAX_MEDIUM_TEXT_WIDTH, ErrorCode.ER_TOO_BIG_FIELDLENGTH, colName, Integer.toString(MAX_MEDIUM_TEXT_WIDTH));
        break;
      case LongBlob:
      case LongText:
        precision = getArg0(argList, typeName);
        validArgNeg(typeName, precision, argList);
        validPrecisionBound(precision, MAX_LONG_TEXT_WIDTH, ErrorCode.ER_TOO_BIG_FIELDLENGTH, colName, Long.toString(MAX_LONG_TEXT_WIDTH));
        break;
      //year[M], 2 or 4
      case Year:
        precision = getArg0(argList, typeName);
        validArgNeg(typeName, precision, argList);
        if (precision != 2) {
          precision = MAX_YEAR_WIDTH;
        }
        break;
      //datetime/timestamp/time[fsp], 0~6
      case DateTime:
      case TimeStamp:
      case Time: {
        scale = (int) getArg0(argList, typeName);
        validArgNeg(typeName, scale, argList);
        validScaleBound(scale, MAX_TIME_SCALE, ErrorCode.ER_TOO_BIG_SCALE, Integer.toString(scale), colName, Integer.toString(MAX_TIME_SCALE));
        break;
      }
      default:
        break;
    }

    return new long[]{ precision, scale };
  }

  private static void validArgNeg(String typeName, long arg, List<SQLExpr> argList) {
    if (arg < 0) {
      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_PARSE_TYPE_ERROR, typeName + "(" + argList + ")");
    }
  }

  private static long getArg0(List<SQLExpr> argList, String typeName) {
    if (argList.size() >= 2) {
      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_PARSE_TYPE_ONE_ERROR, typeName + "(" + argList + ")");
    }
    SQLExpr expr = argList.get(0);
    return ((SQLIntegerExpr) expr).getNumber().longValue();
  }

  public static SQLType buildSQLType(DataType type) {
    return buildSQLType(type, false);
  }

  public static SQLType buildSQLType(DataType type, int precision, int scale) {
    SQLType.Builder builder = SQLType.newBuilder();
    builder.setType(type)
            .setScale(scale)
            .setPrecision(precision)
            .setCharset(DEFAULT_CHARSET_STR)
            .setCollate(DEFAULT_COLLATE);
    return builder.build();
  }

  public static SQLType buildSQLType(DataType type, boolean unsigned) {
    if (unsigned) {
      return SQL_TYPES_UNSIGNED[type.ordinal()];
    }
    return SQL_TYPES_SIGNED[type.ordinal()];
  }

  public static boolean isNumberType(SQLType sqlType) {
    final DataType dt = sqlType.getType();
    return dt == DataType.TinyInt || dt == DataType.SmallInt || dt == DataType.MediumInt || dt == DataType.Int
            || dt == DataType.BigInt || dt == DataType.Decimal || dt == DataType.Float || dt == DataType.Double;
  }

  public static boolean isIntegerType(SQLType sqlType) {
    final DataType dt = sqlType.getType();
    return dt == DataType.TinyInt || dt == DataType.SmallInt || dt == DataType.MediumInt || dt == DataType.Int
            || dt == DataType.BigInt;
  }

  public static boolean isDateType(SQLType sqlType) {
    final DataType dt = sqlType.getType();
    return dt == DataType.Date || dt == DataType.DateTime || dt == DataType.TimeStamp;
  }

  public static boolean isDateTime(SQLType sqlType) {
    final DataType dt = sqlType.getType();
    return dt == DataType.Date || dt == DataType.DateTime || dt == DataType.TimeStamp || dt == DataType.Time;
  }

  public static boolean isYear(SQLType sqlType) {
    final DataType dt = sqlType.getType();
    return dt == DataType.Year;
  }

  public static boolean isString(SQLType sqlType) {
    final DataType dt = sqlType.getType();
    return dt == DataType.Char || dt == DataType.NChar || dt == DataType.Text || dt == DataType.Varchar
            || dt == DataType.Binary || dt == DataType.Invalid;
  }

  public static boolean isCharType(SQLType sqlType) {
    final DataType dt = sqlType.getType();
    return dt == DataType.Char || dt == DataType.NChar;
  }

  public static boolean isVarCharType(SQLType sqlType) {
    final DataType dt = sqlType.getType();
    return dt == DataType.Varchar || dt == DataType.Text;
  }

  public static boolean isFractionable(SQLType sqlType) {
    final DataType dt = sqlType.getType();
    return dt == DataType.DateTime || dt == DataType.Time || dt == DataType.TimeStamp;
  }

  public static boolean isBinString(SQLType sqlType) {
    return sqlType.getBinary() && isString(sqlType) ? true : false;
  }

  public static boolean isTimestampFunc(SQLExpr expr, Basepb.DataType dataType, int fsp, String colName) {

    if (dataType != Basepb.DataType.DateTime && dataType != Basepb.DataType.TimeStamp) {
      return false;
    }

    if (expr instanceof SQLIdentifierExpr
            && ((SQLIdentifierExpr) expr).nameEquals(Types.CURRENT_TIMESTAMP_FUNC)) {
      if (fsp == 0) {
        return true;
      }
      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_INVALID_DEFAULT, colName);
    }

    //colName TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)
    if (expr instanceof SQLMethodInvokeExpr
            && Types.CURRENT_TIMESTAMP_FUNC.equalsIgnoreCase(((SQLMethodInvokeExpr) expr).getMethodName())) {
      List<SQLExpr> argList = ((SQLMethodInvokeExpr) expr).getArguments();
      if (argList != null && argList.size() == 1) {
        int funcFsp = ((SQLIntegerExpr) argList.get(0)).getNumber().intValue();
        if (funcFsp == fsp) {
          return true;
        }
      }

      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_INVALID_DEFAULT, colName);
    }

    return false;
  }

  public static ValueType sqlToValueType(final SQLType type) {
    switch (type.getType()) {
      case Invalid:
      case Null:
        return ValueType.NULL;
      case Varchar:
      case Char:
      case NChar:
      case TinyText:
      case Text:
      case MediumText:
      case LongText:
        return ValueType.STRING;

      case TinyInt:
      case SmallInt:
      case MediumInt:
      case Int:
      case BigInt:
        return type.getUnsigned() ? ValueType.UNSIGNEDLONG : ValueType.LONG;

      case Float:
      case Double:
        return ValueType.DOUBLE;
      case Decimal:
        return ValueType.DECIMAL;

      case Date:
      case TimeStamp:
      case DateTime:
        return ValueType.DATE;
      case Time:
        return ValueType.TIME;
      case Year:
        return ValueType.YEAR;

      case Binary:
      case VarBinary:
      case TinyBlob:
      case Blob:
      case MediumBlob:
      case LongBlob:
        return ValueType.BINARY;

      case Json:
        return ValueType.JSON;
      default:
        throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_NOT_SUPPORTED_YET, "Type(" + type.getType().name() + ")");
    }
  }

  public static double maxDouble(int precision, int scale) {
    int intLen = precision - scale;
    double v = Math.pow(10, intLen);
    v -= Math.pow(10, -scale);
    return v;
  }

  public static double roundDouble(double v, int scale) {
    double mask = Math.pow(10, scale);
    double d = v * mask;
    if (Double.isInfinite(d)) {
      return v;
    }
    return Math.round(d) / mask;
  }

  public static long toLong(final double d) {
    if (d > Long.MAX_VALUE) {
      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OVER_FLOW_UP, "long", Double.toString(d));
    }
    if (d < Long.MIN_VALUE) {
      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OVER_FLOW_DOWN, "long", Double.toString(d));
    }
    return Math.round(d);
  }

  public static long toLong(final BigDecimal dec) {
    if (dec.compareTo(MAX_DEC_SIGNEDLONG) > 0) {
      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OVER_FLOW_UP, dec.toString(), "Long");
    }
    if (dec.compareTo(MIN_DEC_SIGNEDLONG) < 0) {
      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OVER_FLOW_DOWN, dec.toString(), "Long");
    }
    return dec.setScale(0, RoundingMode.HALF_UP).longValue();
  }

  @SuppressFBWarnings("CLI_CONSTANT_LIST_INDEX")
  public static String toDescribe(final Metapb.ColumnInfo columnInfo) {
    String append = "";
    Basepb.DataType dataType = columnInfo.getSqlType().getType();
    String typeStr = TYPE_DESC_MAP.get(dataType);
    String charset = columnInfo.getSqlType().getCharset();
    if ("binary".equalsIgnoreCase(charset)) {
      if (dataType == Basepb.DataType.Varchar) {
        typeStr = typeStr.replaceFirst("char", "binary");
      }
    }

    long defaultPrecision = -1;
    int defaultScale = -1;
    long[] defaults = TYPE_DEFAULT_PRECISION_MAP.get(dataType);
    if (defaults != null) {
      defaultPrecision = defaults[0];
      defaultScale = (int) defaults[1];
    }
    long precision = columnInfo.getSqlType().getPrecision();
    int scale = columnInfo.getSqlType().getScale();
    boolean notDefaultScale = scale != defaultScale && scale != 0 && scale != Types.UNDEFINE_WIDTH;
    if (precision == 0 || precision == Types.UNDEFINE_WIDTH) {
      precision = defaultPrecision;
    }
    if (scale == 0 || scale == Types.UNDEFINE_WIDTH) {
      scale = defaultScale;
    }
    switch (dataType) {
      case TimeStamp:
      case DateTime:
      case Time:
        if (notDefaultScale) {
          append = String.format("(%d)", scale);
        }
        break;

      case Float:
      case Double:
        if (notDefaultScale) {
          append = String.format("(%d,%d)", precision, scale);
        }
        break;

      case Decimal:
        append = String.format("(%d,%d)", precision, scale);
        break;

      case Bit:
      case TinyInt:
      case SmallInt:
      case MediumInt:
      case Int:
      case BigInt:
      case Char:
      case NChar:
      case Varchar:
      case Binary:
      case VarBinary:
      case TinyBlob:
      case Blob:
      case MediumBlob:
      case LongBlob:
      case TinyText:
      case Text:
      case MediumText:
      case LongText:
        append = String.format("(%d)", precision);
        break;

      case Year:
        append = String.format("(%d)", columnInfo.getSqlType().getPrecision());
        break;

      default:
        break;
    }

    return typeStr + append;
  }

//  private ValueType getType(ValueType vt1, SQLType st1, boolean unsign1, ValueType vt2, SQLType st2, boolean unsign2) {
//    if (st1.getType() == DataType.Invalid || st2.getType() == DataType.Invalid) {
//      if (st1.getType() == st2.getType()) {
//        return ValueType.STRING;
//      }
//      if (st1.getType() == DataType.Invalid) {
//        vt1 = vt2;
//      } else {
//        vt2 = vt1;
//      }
//    }
//
//    if (vt1.isString() || vt2.isString()) {
//      return ValueType.STRING;
//    }
//    if (vt1 == ValueType.DOUBLE || vt2 == ValueType.DOUBLE) {
//      return ValueType.DOUBLE;
//    }
//    if (vt1 == ValueType.DECIMAL || vt2 == ValueType.DECIMAL || unsign1 != unsign2) {
//      return ValueType.DECIMAL;
//    }
//    return unsign1 ? ValueType.UNSIGNEDLONG : ValueType.LONG;
//  }
}
