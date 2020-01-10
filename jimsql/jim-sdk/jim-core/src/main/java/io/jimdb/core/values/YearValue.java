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
package io.jimdb.core.values;

import java.nio.ByteBuffer;

import io.jimdb.core.types.ValueType;
import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;

import org.apache.commons.lang3.StringUtils;

/**
 * @version V1.0
 */
public class YearValue extends Value<YearValue> {

  private int value;

  private static final short YEAR_LOWER = 1901;
  private static final short YEAR_UPPER = 2155;
  public static final YearValue ZERO = new YearValue(0);

  private YearValue(int value) {
    this.value = value;
  }

  public static YearValue getInstance(String sValue) {
    int value;
    if (StringUtils.isBlank(sValue)) {
      value = 0;
    } else if ("0".equals(sValue) || "00".equals(sValue)) {
      value = 2000;
    } else {
      sValue = sValue.substring(0, 4);
      try {
        value = Integer.parseInt(sValue);
      } catch (NumberFormatException e) {
        throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_TRUNCATED, e);
      }
    }
    return getInstance(value);
  }

  public static YearValue getInstance(int value) {
    if (value == 0) {
      return ZERO;
    }

    //two digit handle, 00~69 change to 2000~2069,  70~99 change to 1970~1999
    if (value < 100) {
      if (value <= 69) {
        value = value + 100;
      }
      value += 1900;
    }

    if (value < YEAR_LOWER) {
      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OVER_FLOW_DOWN, "YEAR", "1901");
    }

    if (value > YEAR_UPPER) {
      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OVER_FLOW_UP, "YEAR", "2155");
    }

    return new YearValue(value);
  }

  @Override
  public ValueType getType() {
    return ValueType.YEAR;
  }

  public int getValue() {
    return this.value;
  }

  @Override
  public String getString() {
    return String.format("%d", this.value);
  }

  @Override
  public byte[] toByteArray() {
    ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES);
    byteBuffer.putInt(value);
    return byteBuffer.array();
  }

  public String toString() {
    String format = "%d";
    if (this.value == 0) {
      format = "%04d";
    }
    return String.format(format, this.value);
  }

  @Override
  protected int compareToSafe(YearValue v2) {
    if (v2 == null) {
      return 1;
    }
    return this.value - v2.value;
  }

  public long formatToLong() {
    return this.value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    YearValue yearValue = (YearValue) o;
    return value == yearValue.value;
  }

  @Override
  public int hashCode() {
    return value;
  }
}
