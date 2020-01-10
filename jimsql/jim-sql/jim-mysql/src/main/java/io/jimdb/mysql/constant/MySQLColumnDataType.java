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

package io.jimdb.mysql.constant;

import java.util.HashMap;
import java.util.Map;

import io.jimdb.pb.Basepb.DataType;

/**
 * @version V1.0
 */
public enum MySQLColumnDataType {

  //see https://dev.mysql.com/doc/internals/en/com-query-response.html#column-type

  MYSQL_DATA_TYPE_DECIMAL(0x00),

  MYSQL_DATA_TYPE_TINY(0x01),

  MYSQL_DATA_TYPE_SHORT(0x02),

  MYSQL_DATA_TYPE_LONG(0x03),

  MYSQL_DATA_TYPE_FLOAT(0x04),

  MYSQL_DATA_TYPE_DOUBLE(0x05),

  MYSQL_DATA_TYPE_NULL(0x06),

  MYSQL_DATA_TYPE_TIMESTAMP(0x07),

  MYSQL_DATA_TYPE_LONGLONG(0x08),

  MYSQL_DATA_TYPE_INT24(0x09),

  MYSQL_DATA_TYPE_DATE(0x0a),

  MYSQL_DATA_TYPE_TIME(0x0b),

  MYSQL_DATA_TYPE_DATETIME(0x0c),

  MYSQL_DATA_TYPE_YEAR(0x0d),

  MYSQL_DATA_TYPE_NEWDATE(0x0e),

  MYSQL_DATA_TYPE_VARCHAR(0x0f),

  MYSQL_DATA_TYPE_BIT(0x10),

  MYSQL_DATA_TYPE_TIMESTAMP2(0x11),

  MYSQL_DATA_TYPE_DATETIME2(0x12),

  MYSQL_DATA_TYPE_TIME2(0x13),

  MYSQL_DATA_TYPE_NEWDECIMAL(0xf6),

  MYSQL_DATA_TYPE_ENUM(0xf7),

  MYSQL_DATA_TYPE_SET(0xf8),

  MYSQL_DATA_TYPE_TINY_BLOB(0xf9),

  MYSQL_DATA_TYPE_MEDIUM_BLOB(0xfa),

  MYSQL_DATA_TYPE_LONG_BLOB(0xfb),

  MYSQL_DATA_TYPE_BLOB(0xfc),

  MYSQL_DATA_TYPE_VAR_STRING(0xfd),

  MYSQL_DATA_TYPE_STRING(0xfe),

  MYSQL_DATA_TYPE_GEOMETRY(0xff);

  private static final Map<DataType, MySQLColumnDataType> MYSQL_TYPE_MAP = new HashMap<>(MySQLColumnDataType.values().length, 1);
  private static final Map<MySQLColumnDataType, DataType> MYSQL_DS_TYPE_MAP = new HashMap<>(MySQLColumnDataType.values().length, 1);

  private final int value;

  MySQLColumnDataType(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }

  static {
    MYSQL_TYPE_MAP.put(DataType.TinyInt, MYSQL_DATA_TYPE_TINY);
    MYSQL_TYPE_MAP.put(DataType.SmallInt, MYSQL_DATA_TYPE_SHORT);
    MYSQL_TYPE_MAP.put(DataType.MediumInt, MYSQL_DATA_TYPE_INT24);
    MYSQL_TYPE_MAP.put(DataType.Int, MYSQL_DATA_TYPE_LONG);
    MYSQL_TYPE_MAP.put(DataType.BigInt, MYSQL_DATA_TYPE_LONGLONG);
    MYSQL_TYPE_MAP.put(DataType.Float, MYSQL_DATA_TYPE_FLOAT);
    MYSQL_TYPE_MAP.put(DataType.Double, MYSQL_DATA_TYPE_DOUBLE);
    MYSQL_TYPE_MAP.put(DataType.Decimal, MYSQL_DATA_TYPE_DECIMAL);
    MYSQL_TYPE_MAP.put(DataType.Char, MYSQL_DATA_TYPE_STRING);
    MYSQL_TYPE_MAP.put(DataType.NChar, MYSQL_DATA_TYPE_STRING);
    MYSQL_TYPE_MAP.put(DataType.Varchar, MYSQL_DATA_TYPE_VARCHAR);
    MYSQL_TYPE_MAP.put(DataType.Text, MYSQL_DATA_TYPE_BLOB);
    MYSQL_TYPE_MAP.put(DataType.Binary, MYSQL_DATA_TYPE_STRING);
    MYSQL_TYPE_MAP.put(DataType.VarBinary, MYSQL_DATA_TYPE_VAR_STRING);
    MYSQL_TYPE_MAP.put(DataType.Date, MYSQL_DATA_TYPE_DATE);
    MYSQL_TYPE_MAP.put(DataType.Time, MYSQL_DATA_TYPE_TIME);
    MYSQL_TYPE_MAP.put(DataType.DateTime, MYSQL_DATA_TYPE_DATETIME2);
    MYSQL_TYPE_MAP.put(DataType.TimeStamp, MYSQL_DATA_TYPE_TIMESTAMP);
    MYSQL_TYPE_MAP.put(DataType.Year, MYSQL_DATA_TYPE_YEAR);
    MYSQL_TYPE_MAP.put(DataType.Invalid, MYSQL_DATA_TYPE_NULL);

    MYSQL_DS_TYPE_MAP.put(MYSQL_DATA_TYPE_TINY, DataType.TinyInt);
    MYSQL_DS_TYPE_MAP.put(MYSQL_DATA_TYPE_SHORT, DataType.SmallInt);
    MYSQL_DS_TYPE_MAP.put(MYSQL_DATA_TYPE_INT24, DataType.MediumInt);
    MYSQL_DS_TYPE_MAP.put(MYSQL_DATA_TYPE_LONG, DataType.Int);
    MYSQL_DS_TYPE_MAP.put(MYSQL_DATA_TYPE_LONGLONG, DataType.BigInt);
    MYSQL_DS_TYPE_MAP.put(MYSQL_DATA_TYPE_FLOAT, DataType.Float);
    MYSQL_DS_TYPE_MAP.put(MYSQL_DATA_TYPE_DOUBLE, DataType.Double);
    MYSQL_DS_TYPE_MAP.put(MYSQL_DATA_TYPE_DECIMAL, DataType.Decimal);
    MYSQL_DS_TYPE_MAP.put(MYSQL_DATA_TYPE_STRING, DataType.Char);
    MYSQL_DS_TYPE_MAP.put(MYSQL_DATA_TYPE_STRING, DataType.NChar);
    MYSQL_DS_TYPE_MAP.put(MYSQL_DATA_TYPE_VARCHAR, DataType.Varchar);
    MYSQL_DS_TYPE_MAP.put(MYSQL_DATA_TYPE_BLOB, DataType.Text);
    MYSQL_DS_TYPE_MAP.put(MYSQL_DATA_TYPE_STRING, DataType.Varchar);
    MYSQL_DS_TYPE_MAP.put(MYSQL_DATA_TYPE_VAR_STRING, DataType.Varchar);
    MYSQL_DS_TYPE_MAP.put(MYSQL_DATA_TYPE_DATE, DataType.Date);
    MYSQL_DS_TYPE_MAP.put(MYSQL_DATA_TYPE_TIME, DataType.Time);
    MYSQL_DS_TYPE_MAP.put(MYSQL_DATA_TYPE_DATETIME2, DataType.DateTime);
    MYSQL_DS_TYPE_MAP.put(MYSQL_DATA_TYPE_TIMESTAMP, DataType.TimeStamp);
    MYSQL_DS_TYPE_MAP.put(MYSQL_DATA_TYPE_YEAR, DataType.Year);
    MYSQL_DS_TYPE_MAP.put(MYSQL_DATA_TYPE_NULL, DataType.Invalid);
  }

  public static MySQLColumnDataType valueOfJDBCType(DataType sqlType) {
    return MYSQL_TYPE_MAP.get(sqlType);
  }

  public static DataType valueOfType(MySQLColumnDataType sqlType) {
    return MYSQL_DS_TYPE_MAP.get(sqlType);
  }

  /**
   * Value of.
   *
   * @param value value
   * @return column type
   */
  public static MySQLColumnDataType valueOf(final int value) {
    for (MySQLColumnDataType each : MySQLColumnDataType.values()) {
      if (value == each.value) {
        return each;
      }
    }
    throw new IllegalArgumentException(String.format("Cannot find value '%s' in column type", value));
  }
}
