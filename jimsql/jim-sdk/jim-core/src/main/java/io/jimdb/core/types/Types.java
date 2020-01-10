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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings({ "HARD_CODE_KEY", "OCP_OVERLY_CONCRETE_PARAMETER" })
public final class Types {
  public static final String PRIMARY_KEY_NAME = "PRIMARY";

  public static final int FLAG_KEY_PRIMARY = 1 << 1;
  public static final int FLAG_KEY_UNIQUE = 1 << 2;
  public static final int FLAG_KEY_MULTIPLE = 1 << 3;

  public static final BigDecimal MAX_DEC_SIGNEDLONG = BigDecimal.valueOf(Long.MAX_VALUE);
  public static final BigDecimal MIN_DEC_SIGNEDLONG = BigDecimal.valueOf(Long.MIN_VALUE);
  public static final BigInteger MAX_SIGNEDLONG = BigInteger.valueOf(Long.MAX_VALUE);
  public static final BigInteger MAX_UNSIGNEDLONG = new BigInteger("18446744073709551615");
  public static final BigInteger MIN_UNSIGNEDLONG = BigInteger.valueOf(0L);

  public static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
  public static final String DEFAULT_CHARSET_STR = DEFAULT_CHARSET.name();
  public static final String DEFAULT_COLLATE = "utf8_bin";
  public static final SQLType UNDEFINE_TYPE = buildSQLType(DataType.Invalid);

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
  public static final int MAX_TIME_NOFSP_WIDTH = 10;
  public static final int MAX_TIME_FSP_WIDTH = 15;
  public static final int MAX_BLOB_WIDTH = 16777216;
  public static final int MAX_CHAR_WIDTH = 255;
  public static final int MAX_VARCHAR_WIDTH = 65535;
  //
  public static final int NOT_FIXED_DEC = 31;
  public static final int PREC_INCREMENT = 4;

  public static final String CURRENT_TIMESTAMP_FUNC = "CURRENT_TIMESTAMP";
  public static final String NOW_FUNC = "NOW";
  public static final String LOCALTIME = "LOCALTIME";
  public static final String LOCALTIME_FUNC = "LOCALTIME";
  public static final String LOCALTIMESTAMP = "LOCALTIMESTAMP";
  public static final String LOCALTIMESTAMP_FUNC = "LOCALTIMESTAMP";

  private static final Map<String, DataType> DATA_TYPE_MAP;
  private static final Map<DataType, String> DATA_TYPE_DESC_MAP;
  private static final Map<DataType, int[]> DATA_TYPE_DEFAULT_PRECISION_SCALE;

  static {
    DATA_TYPE_MAP = new HashMap<>();
    DATA_TYPE_MAP.put("BIT", DataType.Bit);
    DATA_TYPE_MAP.put(INT, DataType.Int);
    DATA_TYPE_MAP.put(BIGINT, DataType.BigInt);
    DATA_TYPE_MAP.put("MEDIUMINT", DataType.MediumInt);
    DATA_TYPE_MAP.put(SMALLINT, DataType.SmallInt);
    DATA_TYPE_MAP.put(TINYINT, DataType.TinyInt);
    DATA_TYPE_MAP.put(BOOLEAN, DataType.TinyInt);
    DATA_TYPE_MAP.put("FLOAT", DataType.Float);
    DATA_TYPE_MAP.put("DOUBLE", DataType.Double);
    DATA_TYPE_MAP.put(REAL, DataType.Double);
    DATA_TYPE_MAP.put(DECIMAL, DataType.Decimal);
    DATA_TYPE_MAP.put(NUMBER, DataType.Decimal);
    DATA_TYPE_MAP.put(VARCHAR, DataType.Varchar);
    DATA_TYPE_MAP.put(TEXT, DataType.Varchar);
    DATA_TYPE_MAP.put(DATE, DataType.Date);
    DATA_TYPE_MAP.put("DATETIME", DataType.DateTime);
    DATA_TYPE_MAP.put(TIMESTAMP, DataType.TimeStamp);
    DATA_TYPE_MAP.put("TIME", DataType.Time);
    DATA_TYPE_MAP.put("YEAR", DataType.Year);
    DATA_TYPE_MAP.put(CHAR, DataType.Char);
    DATA_TYPE_MAP.put(NCHAR, DataType.NChar);
    DATA_TYPE_MAP.put(BYTEA, DataType.Binary);
  }

  static {
    DATA_TYPE_DESC_MAP = new HashMap<>();
    DATA_TYPE_DESC_MAP.put(DataType.Invalid, "unspecified");
    DATA_TYPE_DESC_MAP.put(DataType.TinyInt, "tinyint");
    DATA_TYPE_DESC_MAP.put(DataType.SmallInt, "smallint");
    DATA_TYPE_DESC_MAP.put(DataType.MediumInt, "mediumint");
    DATA_TYPE_DESC_MAP.put(DataType.Int, "int");
    DATA_TYPE_DESC_MAP.put(DataType.BigInt, "bigint");
    DATA_TYPE_DESC_MAP.put(DataType.Bit, "bit");
    DATA_TYPE_DESC_MAP.put(DataType.Float, "float");
    DATA_TYPE_DESC_MAP.put(DataType.Double, "double");
    DATA_TYPE_DESC_MAP.put(DataType.Decimal, "decimal");
    DATA_TYPE_DESC_MAP.put(DataType.Date, "date");
    DATA_TYPE_DESC_MAP.put(DataType.TimeStamp, "timestamp");
    DATA_TYPE_DESC_MAP.put(DataType.DateTime, "datetime");
    DATA_TYPE_DESC_MAP.put(DataType.Time, "time");
    DATA_TYPE_DESC_MAP.put(DataType.Year, "year");
    DATA_TYPE_DESC_MAP.put(DataType.Varchar, "varchar");
    DATA_TYPE_DESC_MAP.put(DataType.Binary, "binary");
    DATA_TYPE_DESC_MAP.put(DataType.Char, "char");
    DATA_TYPE_DESC_MAP.put(DataType.NChar, "char");
    DATA_TYPE_DESC_MAP.put(DataType.Text, "text");
    DATA_TYPE_DESC_MAP.put(DataType.VarBinary, "varbinary");
    DATA_TYPE_DESC_MAP.put(DataType.Json, "json");
  }

  static {
    DATA_TYPE_DEFAULT_PRECISION_SCALE = new HashMap<>();
    DATA_TYPE_DEFAULT_PRECISION_SCALE.put(DataType.Bit, new int[]{ 1, 0 });
    DATA_TYPE_DEFAULT_PRECISION_SCALE.put(DataType.TinyInt, new int[]{ 4, 0 });
    DATA_TYPE_DEFAULT_PRECISION_SCALE.put(DataType.SmallInt, new int[]{ 6, 0 });
    DATA_TYPE_DEFAULT_PRECISION_SCALE.put(DataType.MediumInt, new int[]{ 9, 0 });
    DATA_TYPE_DEFAULT_PRECISION_SCALE.put(DataType.Int, new int[]{ 11, 0 });
    DATA_TYPE_DEFAULT_PRECISION_SCALE.put(DataType.BigInt, new int[]{ 20, 0 });
    DATA_TYPE_DEFAULT_PRECISION_SCALE.put(DataType.Float, new int[]{ 12, -1 });
    DATA_TYPE_DEFAULT_PRECISION_SCALE.put(DataType.Double, new int[]{ 22, -1 });
    DATA_TYPE_DEFAULT_PRECISION_SCALE.put(DataType.Decimal, new int[]{ 10, 0 });
    DATA_TYPE_DEFAULT_PRECISION_SCALE.put(DataType.Year, new int[]{ 4, 0 });
    DATA_TYPE_DEFAULT_PRECISION_SCALE.put(DataType.Null, new int[]{ 0, 0 });
    DATA_TYPE_DEFAULT_PRECISION_SCALE.put(DataType.Time, new int[]{ 10, 0 });
    DATA_TYPE_DEFAULT_PRECISION_SCALE.put(DataType.Date, new int[]{ 10, 0 });
    DATA_TYPE_DEFAULT_PRECISION_SCALE.put(DataType.DateTime, new int[]{ 19, 0 });
    DATA_TYPE_DEFAULT_PRECISION_SCALE.put(DataType.TimeStamp, new int[]{ 19, 0 });
    DATA_TYPE_DEFAULT_PRECISION_SCALE.put(DataType.Char, new int[]{ 1, 0 });
    DATA_TYPE_DEFAULT_PRECISION_SCALE.put(DataType.NChar, new int[]{ 1, 0 });
    DATA_TYPE_DEFAULT_PRECISION_SCALE.put(DataType.Varchar, new int[]{ 5, 0 });
    DATA_TYPE_DEFAULT_PRECISION_SCALE.put(DataType.Binary, new int[]{ 1, 0 });
    DATA_TYPE_DEFAULT_PRECISION_SCALE.put(DataType.VarBinary, new int[]{ 5, 0 });
    DATA_TYPE_DEFAULT_PRECISION_SCALE.put(DataType.Text, new int[]{ 65535, 0 });
    DATA_TYPE_DEFAULT_PRECISION_SCALE.put(DataType.Json, new int[]{ Integer.MAX_VALUE, 0 });
  }

//  private static final EnumMap<DataType, EnumMap<DataType, DataType>> TYPE_MERGE_MAP;
//
//  static {
//    TYPE_MERGE_MAP = new EnumMap<>(DataType.class);
//    // Tinyint
//    EnumMap<DataType, DataType> tinyIntMap = new EnumMap<>(DataType.class);
//    tinyIntMap.put(Tinyint, Tinyint);
//    tinyIntMap.put(Null, Tinyint);
//    tinyIntMap.put(DataType.Boolean, Tinyint);
//    tinyIntMap.put(Smallint, Smallint);
//    tinyIntMap.put(Int, Int);
//    tinyIntMap.put(BigInt, BigInt);
//    tinyIntMap.put(DataType.Float, DataType.Float);
//    tinyIntMap.put(DataType.Double, DataType.Double);
//    tinyIntMap.put(Varchar, Varchar);
//    tinyIntMap.put(Date, Varchar);
//    tinyIntMap.put(TimeStamp, Varchar);
//    tinyIntMap.put(Decimal, Decimal);
//    tinyIntMap.put(DataType.Number, Decimal);
//    tinyIntMap.put(Char, Char);
//    tinyIntMap.put(NChar, NChar);
//    tinyIntMap.put(Text, Varchar);
//    tinyIntMap.put(Binary, Binary);
//    tinyIntMap.put(VarBinary, VarBinary);
//    tinyIntMap.put(Time, Varchar);
//    tinyIntMap.put(Xml, Varchar);
//    tinyIntMap.put(Json, Varchar);
//    TYPE_MERGE_MAP.put(Tinyint, tinyIntMap);
//    // Smallint
//    EnumMap<DataType, DataType> smallIntMap = new EnumMap<>(DataType.class);
//    smallIntMap.put(Tinyint, Smallint);
//    smallIntMap.put(Null, Smallint);
//    smallIntMap.put(DataType.Boolean, Smallint);
//    smallIntMap.put(Smallint, Smallint);
//    smallIntMap.put(Int, Int);
//    smallIntMap.put(BigInt, BigInt);
//    smallIntMap.put(DataType.Float, DataType.Float);
//    smallIntMap.put(DataType.Double, DataType.Double);
//    smallIntMap.put(Varchar, Varchar);
//    smallIntMap.put(Char, Char);
//    smallIntMap.put(NChar, NChar);
//    smallIntMap.put(Text, Varchar);
//    smallIntMap.put(Time, Varchar);
//    smallIntMap.put(Date, Varchar);
//    smallIntMap.put(TimeStamp, Varchar);
//    smallIntMap.put(Decimal, Decimal);
//    smallIntMap.put(DataType.Number, Decimal);
//    smallIntMap.put(Binary, Binary);
//    smallIntMap.put(VarBinary, VarBinary);
//    smallIntMap.put(Xml, Varchar);
//    smallIntMap.put(Json, Varchar);
//    TYPE_MERGE_MAP.put(Smallint, smallIntMap);
//    // Int
//    EnumMap<DataType, DataType> intMap = new EnumMap<>(DataType.class);
//    intMap.put(Tinyint, Int);
//    intMap.put(Null, Int);
//    intMap.put(DataType.Boolean, Int);
//    intMap.put(Smallint, Int);
//    intMap.put(Int, Int);
//    intMap.put(BigInt, BigInt);
//    intMap.put(DataType.Float, DataType.Double);
//    intMap.put(DataType.Double, DataType.Double);
//    intMap.put(Varchar, Varchar);
//    intMap.put(Date, Varchar);
//    intMap.put(TimeStamp, Varchar);
//    intMap.put(Decimal, Decimal);
//    intMap.put(DataType.Number, Decimal);
//    intMap.put(Char, Char);
//    intMap.put(NChar, NChar);
//    intMap.put(Text, Varchar);
//    intMap.put(Binary, Binary);
//    intMap.put(VarBinary, VarBinary);
//    intMap.put(Time, Varchar);
//    intMap.put(Xml, Varchar);
//    intMap.put(Json, Varchar);
//    TYPE_MERGE_MAP.put(Int, intMap);
//    // BigInt
//    EnumMap<DataType, DataType> bigIntMap = new EnumMap<>(DataType.class);
//    bigIntMap.put(Tinyint, BigInt);
//    bigIntMap.put(Null, BigInt);
//    bigIntMap.put(DataType.Boolean, BigInt);
//    bigIntMap.put(Smallint, BigInt);
//    bigIntMap.put(Int, BigInt);
//    bigIntMap.put(BigInt, BigInt);
//    bigIntMap.put(DataType.Float, DataType.Double);
//    bigIntMap.put(DataType.Double, DataType.Double);
//    bigIntMap.put(Varchar, Varchar);
//    bigIntMap.put(Binary, Binary);
//    bigIntMap.put(Date, Varchar);
//    bigIntMap.put(TimeStamp, Varchar);
//    bigIntMap.put(Decimal, Decimal);
//    bigIntMap.put(DataType.Number, Decimal);
//    bigIntMap.put(Char, Char);
//    bigIntMap.put(NChar, NChar);
//    bigIntMap.put(Text, Varchar);
//    bigIntMap.put(VarBinary, VarBinary);
//    bigIntMap.put(Time, Varchar);
//    bigIntMap.put(Xml, Varchar);
//    bigIntMap.put(Json, Varchar);
//    TYPE_MERGE_MAP.put(BigInt, bigIntMap);
//    // Float
//    EnumMap<DataType, DataType> floatMap = new EnumMap<>(DataType.class);
//    floatMap.put(Tinyint, DataType.Float);
//    floatMap.put(Null, DataType.Float);
//    floatMap.put(DataType.Boolean, DataType.Float);
//    floatMap.put(Smallint, DataType.Float);
//    floatMap.put(Int, DataType.Double);
//    floatMap.put(BigInt, DataType.Double);
//    floatMap.put(DataType.Float, DataType.Float);
//    floatMap.put(DataType.Double, DataType.Double);
//    floatMap.put(Varchar, Varchar);
//    floatMap.put(Binary, Binary);
//    floatMap.put(Date, Varchar);
//    floatMap.put(TimeStamp, Varchar);
//    floatMap.put(Decimal, DataType.Double);
//    floatMap.put(DataType.Number, DataType.Double);
//    floatMap.put(Char, Char);
//    floatMap.put(NChar, NChar);
//    floatMap.put(Text, Varchar);
//    floatMap.put(VarBinary, VarBinary);
//    floatMap.put(Time, VarBinary);
//    floatMap.put(Xml, Varchar);
//    floatMap.put(Json, Varchar);
//    TYPE_MERGE_MAP.put(DataType.Float, floatMap);
//    // Double
//    EnumMap<DataType, DataType> doubleMap = new EnumMap<>(DataType.class);
//    doubleMap.put(Tinyint, DataType.Double);
//    doubleMap.put(Null, DataType.Double);
//    doubleMap.put(DataType.Boolean, DataType.Double);
//    doubleMap.put(Smallint, DataType.Double);
//    doubleMap.put(Int, DataType.Double);
//    doubleMap.put(BigInt, DataType.Double);
//    doubleMap.put(DataType.Float, DataType.Double);
//    doubleMap.put(DataType.Double, DataType.Double);
//    doubleMap.put(Varchar, Varchar);
//    doubleMap.put(Binary, Binary);
//    doubleMap.put(Date, Varchar);
//    doubleMap.put(TimeStamp, Varchar);
//    doubleMap.put(Decimal, DataType.Double);
//    doubleMap.put(DataType.Number, DataType.Double);
//    doubleMap.put(Char, Char);
//    doubleMap.put(NChar, NChar);
//    doubleMap.put(Text, Varchar);
//    doubleMap.put(VarBinary, VarBinary);
//    doubleMap.put(Time, Varchar);
//    doubleMap.put(Xml, Varchar);
//    doubleMap.put(Json, Varchar);
//    TYPE_MERGE_MAP.put(DataType.Double, doubleMap);
//    // Varchar
//
//  }

  private Types() {
  }

  public static SQLType buildSQLType(SQLDataType sqlType, String colName) {
    if (sqlType == null) {
      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_PARSE_NO_TYPE_ERROR, "after " + colName);
    }
    String typeName = sqlType.getName().toUpperCase();
    DataType dt = DATA_TYPE_MAP.get(typeName);
    if (dt == null) {
      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_NOT_SUPPORTED_YET, "SqlType(" + typeName + ")");
    }

    if (StringUtils.isBlank(colName)) {
      return buildSQLType(dt);
    }

    int precision = 0;
    int scale = 0;
    boolean unsigned = false;
    boolean zerofill = false;
    if (sqlType instanceof SQLDataTypeImpl) {
      SQLDataTypeImpl sqlTypeImpl = (SQLDataTypeImpl) sqlType;
      unsigned = sqlTypeImpl.isUnsigned();
      zerofill = sqlTypeImpl.isZerofill();
    }
    String charset = DEFAULT_CHARSET.name();
    String collate = DEFAULT_COLLATE;
//    if (sqlType instanceof SQLCharacterDataType) {
//      SQLCharacterDataType chaDataType = (SQLCharacterDataType) sqlType;
//      charset = chaDataType.getCharSetName();
//      collate = chaDataType.getCollate();
//    }
    List<SQLExpr> argList = sqlType.getArguments();
    switch (dt) {
      //tiny/small/medium/int/bigint(M), 0~255
      case TinyInt:
      case SmallInt:
      case MediumInt:
      case Int:
      case BigInt:
        unsigned = handleNumberZerofill(unsigned, zerofill);
        precision = getArg1(argList, typeName);
        validArgNeg(typeName, precision, argList);
        if (precision > 255) {
          throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_TOO_BIG_DISPLAYWIDTH, colName, "255");
        }
        break;
      //bit(M), 1~64
      case Bit:
        unsigned = handleNumberZerofill(unsigned, zerofill);
        if (argList.isEmpty()) {
          //the default is 1;
          precision = 1;
          break;
        }
        precision = getArg1(argList, typeName);
        validArgNeg(typeName, precision, argList);
        if (precision == 0) {
          precision = 1;
        }
        if (precision > 64) {
          throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_TOO_BIG_DISPLAYWIDTH, colName, "64");
        }
        break;
      //decimal/float/double(M,D)
      case Decimal:
      case Float:
      case Double:
        unsigned = handleNumberZerofill(unsigned, zerofill);
        if (argList.isEmpty()) {
          if (dt == DataType.Decimal) {
            //the default of decimal is (10,0);
            precision = 10;
          }
          //don't check fount/double default
          //float: If M and D are omitted, values are stored to the limits permitted by the hardware
          break;
        }
        if (argList.size() > 2) {
          throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_PARSE_TYPE_TWO_ERROR, typeName + "(" + argList + ")");
        }
        SQLExpr expr = argList.get(0);
        precision = ((SQLIntegerExpr) expr).getNumber().intValue();
        if (argList.size() == 2) {
          expr = argList.get(1);
          scale = ((SQLIntegerExpr) expr).getNumber().intValue();
        }

        validArgNeg(typeName, precision, argList);
        validArgNeg(typeName, scale, argList);

        if (dt == DataType.Decimal && precision > 65) {
          throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_TOO_BIG_PRECISION, precision + "", colName, "65");
        }
        if (argList.size() == 2) {
          // float/double (M,D)
          if ((dt == DataType.Float || dt == DataType.Double) && precision > 255) {
            throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_TOO_BIG_DISPLAYWIDTH, colName, "255");
          }
          if (scale > 30) {
            throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_TOO_BIG_SCALE, scale + "", colName, "30");
          }
          if (scale > precision) {
            throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_M_BIGGER_THAN_D, typeName);
          }
        } else {
          // float/double (p)
          if (dt == DataType.Float) {
            if (precision < 24) {
              dt = DataType.Float;
            } else if (precision < 53) {
              dt = DataType.Double;
            } else {
              throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_WRONG_FIELD_SPEC, colName);
            }
          } else if (dt == DataType.Double) {
            throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_PARSE_TYPE_ERROR, typeName + "(" + argList + ")");
          }
        }
        break;
      //varchar[M], 0~65,535
      case Varchar:
        if (argList.isEmpty()) {
          break;
        }
        precision = getArg1(argList, typeName);
        validArgNeg(typeName, precision, argList);
        //update to no limit, maggie
//        if (precision > 65535) {
//          throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_TOO_BIG_FIELDLENGTH, colName, "65535");
//        }
        break;
      //year[M], 2 or 4
      case Year:
        precision = getArg1(argList, typeName);
        validArgNeg(typeName, precision, argList);
        if (precision != 2) {
          precision = 4;
        }
        break;
      //datetime/timestamp/time[fsp], 0~6
      case DateTime:
      case TimeStamp:
      case Time: {
        if (argList.isEmpty()) {
          break;
        }
        scale = getArg1(argList, typeName);
        validArgNeg(typeName, scale, argList);
        if (scale > 6) {
          //? scale or precision
          throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_TOO_BIG_PRECISION, precision + "", colName, "6");
        }
        break;
      }
      default:
        break;
    }

    return SQLType.newBuilder().setType(dt)
            .setScale(scale).setPrecision(precision)
            .setCharset(charset).setCollate(collate)
            .setUnsigned(unsigned).setZerofill(zerofill).build();
  }

  private static void validArgNeg(String typeName, int arg, List<SQLExpr> argList) {
    if (arg < 0) {
      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_PARSE_TYPE_ERROR, typeName + "(" + argList + ")");
    }
  }

  private static boolean handleNumberZerofill(boolean unsigned, boolean zerofill) {
    if (zerofill) {
      unsigned = true;
    }
    return unsigned;
  }

  private static int getArg1(List<SQLExpr> argList, String typeName) {
    if (argList.isEmpty()) {
      return 0;
    }
    if (argList.size() >= 2) {
      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_PARSE_TYPE_ONE_ERROR, typeName + "(" + argList + ")");
    }
    SQLExpr expr = argList.get(0);
    return ((SQLIntegerExpr) expr).getNumber().intValue();
  }

  public static SQLType buildSQLType(DataType type) {
    SQLType.Builder builder = SQLType.newBuilder();
    builder.setType(type)
            .setScale(UNDEFINE_WIDTH)
            .setPrecision(UNDEFINE_WIDTH)
            .setCharset(DEFAULT_CHARSET.name())
            .setCollate(DEFAULT_COLLATE);
    return builder.build();
  }

  public static SQLType buildSQLType(DataType type, boolean unsigned) {
    SQLType.Builder builder = SQLType.newBuilder();
    builder.setType(type)
            .setUnsigned(unsigned)
            .setScale(UNDEFINE_WIDTH)
            .setPrecision(UNDEFINE_WIDTH)
            .setCharset(DEFAULT_CHARSET.name())
            .setCollate(DEFAULT_COLLATE);
    return builder.build();
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

  public static boolean isTimestampFunc(SQLExpr expr, Basepb.DataType dataType) {
    if (expr instanceof SQLIdentifierExpr && ((SQLIdentifierExpr) expr).nameEquals(Types.CURRENT_TIMESTAMP_FUNC)
            && (dataType == Basepb.DataType.DateTime || dataType == Basepb.DataType.TimeStamp)) {
      return true;
    }
    return false;
  }

  public static ValueType sqlToValueType(final SQLType type) {
    final DataType dt = type.getType();
    if (dt == DataType.Invalid) {
      return ValueType.NULL;
    }

    switch (dt) {
      case Null:
        return ValueType.NULL;
      case Varchar:
      case Text:
      case Char:
      case NChar:
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
        return ValueType.BINARY;
      case Json:
        return ValueType.JSON;
      default:
        throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_NOT_SUPPORTED_YET, "Type(" + type.getType().name() + ")");
    }
  }

  public static double maxDouble(int precision, int scale) {
    int intLen = precision - scale;
    double v = Math.pow(intLen, 10);
    v -= Math.pow(-scale, 10);
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
      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OVER_FLOW_UP, "long", String.valueOf(d));
    }
    if (d < Long.MIN_VALUE) {
      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OVER_FLOW_DOWN, "long", String.valueOf(d));
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
    String typeStr = DATA_TYPE_DESC_MAP.get(dataType);
    String charset = columnInfo.getSqlType().getCharset();
    if ("binary".equalsIgnoreCase(charset)) {
      if (dataType == Basepb.DataType.Varchar) {
        typeStr = typeStr.replaceFirst("char", "binary");
      }
    }

    int defaultPrecision = -1;
    int defaultScale = -1;
    int[] defaults = DATA_TYPE_DEFAULT_PRECISION_SCALE.get(dataType);
    if (defaults != null) {
      defaultPrecision = defaults[0];
      defaultScale = defaults[1];
    }
    int precision = columnInfo.getSqlType().getPrecision();
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
      case Text:
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
