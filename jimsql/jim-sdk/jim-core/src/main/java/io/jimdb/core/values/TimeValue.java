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

import java.nio.ByteBuffer;
import java.sql.Timestamp;

import io.jimdb.core.types.ValueType;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represent the TIME type.
 * <p>
 * [-][H]HH:MM:SS[.fractal]
 * <p>
 * Time scope: '-838:59:59.000000' ~ '838:59:59.000000'
 *
 * @version V1.0
 */
public final class TimeValue extends Value<TimeValue> {
  private static final Logger LOGGER = LoggerFactory.getLogger(TimeValue.class);

  public static final TimeValue TIME_EMPTY = null;
  public static final TimeValue TIME_ZERO = new TimeValue(0, TimeUtil.DEFAULT_FSP, false);
  public static final TimeValue MAX_VALUE = new TimeValue(Long.MAX_VALUE, TimeUtil.DEFAULT_FSP);
  public static final TimeValue MIN_VALUE = new TimeValue(Long.MIN_VALUE, TimeUtil.DEFAULT_FSP);

  //fsp: fractional seconds precision
  private int fsp;
  private long value;
  private boolean negative;

  private TimeValue(long value, int fsp) {
    this.value = value;
    this.fsp = fsp;
    if (value < 0) {
      this.negative = true;
    }
  }

  private TimeValue(long value, int fsp, boolean negative) {
    this.value = value;
    this.fsp = fsp;
    this.negative = negative;
  }

  private TimeValue(int hour, int minute, int second, int micros, int fsp, boolean negative) {
    this(buildTime(hour, minute, second, micros, negative), fsp, negative);
  }

  //string to time
  public static TimeValue convertToTime(String valueS, int fsp) {
    if (StringUtils.isBlank(valueS)) {
      return TIME_ZERO;
    }

    valueS = valueS.trim();
    int signIndex = valueS.indexOf('-');
    if (signIndex == valueS.length() - 1) {
      return TIME_ZERO;
    }

    boolean negative = false;
    if (signIndex != -1) {
      negative = true;
      valueS = valueS.substring(1);
    }
    valueS = valueS.trim();

    int hour;
    int minute;
    int second;
    //a fractional seconds part
    int micros = 0;
    // truncate fractional part
    // decimal point index
    int decimalIndex = valueS.indexOf('.');
    if (decimalIndex > -1) {
      micros = TimeUtil.handleFractionalPart(fsp, valueS.substring(decimalIndex + 1));
      valueS = valueS.substring(0, decimalIndex);
    }

    if ("0".equals(valueS) || "0000-00-00 00:00:00".equals(valueS) || "00000000000000".equals(valueS)) {
      return TIME_ZERO;
    }

    int length = valueS.length();
    switch (length) {
      case 9:
        //HHH:MM:SS or HHH/MM/SS
        hour = Integer.parseInt(valueS.substring(0, 3));
        minute = Integer.parseInt(valueS.substring(4, 6));
        second = Integer.parseInt(valueS.substring(7));
        break;
      case 8:
        //HH:MM:SS
        hour = Integer.parseInt(valueS.substring(0, 2));
        minute = Integer.parseInt(valueS.substring(3, 5));
        second = Integer.parseInt(valueS.substring(6));
        break;
      case 5:
        //HH:MM
        hour = Integer.parseInt(valueS.substring(0, 2));
        minute = Integer.parseInt(valueS.substring(3, 5));
        second = 0;
        break;
      case 15:
        //HH:MM:ss.SSSSSS
        hour = Integer.parseInt(valueS.substring(0, 2));
        minute = Integer.parseInt(valueS.substring(3, 5));
        second = Integer.parseInt(valueS.substring(6, 8));
        fsp = Integer.parseInt(valueS.substring(9));
        break;
      default:
        throw new IllegalArgumentException(String.format("time %s: no valid ", valueS));
    }
    return new TimeValue(hour, minute, second, micros, fsp, negative);
  }

  //https://github.com/mysql/mysql-server/blob/5.7/sql-common/my_time.c
  //long to time
  public static TimeValue convertToTimeValue(long number, int fsp) {
    if (number > TimeUtil.TIME_MAX_VALUE) {
      if (number >= 10000000000L) /* '0001-00-00 00-00-00' */ {
        DateValue value = DateValue.convertToDateValue(number);
        return TimeValue.convertToTime(value);
      }
      //convert to time max_value
      return new TimeValue(TimeUtil.TIME_MAX_HOUR, TimeUtil.TIME_MAX_MINUTE, TimeUtil.TIME_MAX_MINUTE,
              0, fsp, false);
    } else if (number < -TimeUtil.TIME_MAX_VALUE) {
      return new TimeValue(TimeUtil.TIME_MAX_HOUR, TimeUtil.TIME_MAX_MINUTE, TimeUtil.TIME_MAX_MINUTE,
              0, fsp, true);
    }

    boolean negative = number < 0;
    if (negative) {
      number = -number;
    }

    int hour = (int) (number / 10000);
    int minute = (int) (number / 100 % 100);
    int second = (int) (number % 100);

    return new TimeValue(hour, minute, second, 0, fsp, negative);
  }

  //date/datetime/timestamp to time
  public static TimeValue convertToTime(DateValue dateValue) {
    Timestamp timestamp = dateValue.getValue();
    int hour = timestamp.getHours();
    int minute = timestamp.getMinutes();
    int second = timestamp.getSeconds();
    int micros = timestamp.getNanos() / 1000;

    return new TimeValue(hour, minute, second, micros, dateValue.getFsp(), false);
  }

  public String convertToString() {
    long time = this.getValue();
    boolean negative = time < 0;
    if (negative) {
      time = -time;
    }
    int hour = (int) (time / (1000 * 1000) / 3600);
    int minute = (int) ((time / 1000 / 1000 - hour * 3600) / 60);
    int second = (int) (time / 1000 / 1000 - hour * 3600 - minute * 60);
    int micros = (int) (time % (1000 * 1000));
    String timeEncode;
    if (negative) {
      timeEncode = String.format("-%02d:%02d:%02d", hour, minute, second);
    } else {
      timeEncode = String.format("%02d:%02d:%02d", hour, minute, second);
    }
    if (this.fsp > 0) {
      timeEncode = String.format("%s.%s", timeEncode, String.format("%06d", micros).substring(0, fsp));
    }
    return timeEncode;
  }

  //java jdbc TimeUtil.fastTimeCreate
  private static long buildTime(int hour, int minute, int second, int micros, boolean negative) {
    if (hour < 0 || hour > TimeUtil.TIME_MAX_HOUR) {
      throw new IllegalArgumentException("time: hour");
    }

    if (minute < 0 || minute > TimeUtil.TIME_MAX_MINUTE) {
      throw new IllegalArgumentException("time: minute");
    }

    if (second < 0 || second > TimeUtil.TIME_MAX_SECOND) {
      throw new IllegalArgumentException("time: second");
    }
    long time = (hour * 3600L + minute * 60L + second) * 1000L * 1000L + micros;
    if (negative) {
      time = -time;
    }
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("TimeValue: value:{}, {}:{}:{}.{}", time, hour, minute, second, micros);
    }
    return time;
  }

  public static TimeValue getInstance(String value) {
    return getInstance(value, TimeUtil.MAX_FSP);
  }

  public static TimeValue getInstance(String value, int fsp) {
    return convertToTime(value, fsp);
  }

  public static TimeValue getInstance(long value) {
    return getInstance(value, TimeUtil.MAX_FSP);
  }

  public static TimeValue getInstance(long value, int fsp) {
    return new TimeValue(value, fsp);
  }

  public static TimeValue getInstance(int hour, int minute, int second, int micros, boolean negative) {
    return new TimeValue(hour, minute, second, micros, 0, negative);
  }

  public long getValue() {
    return this.value;
  }

  @Override
  public ValueType getType() {
    return ValueType.TIME;
  }

  public boolean isNegative() {
    return this.negative;
  }

  public int getFsp() {
    return this.fsp;
  }

  @Override
  public String getString() {
    return String.format("%d", this.value);
  }

  @Override
  public byte[] toByteArray() {
    ByteBuffer byteBuffer = ByteBuffer.allocate(Long.BYTES);
    byteBuffer.putLong(value);
    return byteBuffer.array();
  }

  @Override
  protected Value plusSafe(TimeValue v2) {
    return null;
  }

  @Override
  protected Value subtractSafe(TimeValue v2) {
    return null;
  }

  @Override
  public boolean isMax() {
    return this == MAX_VALUE;
  }

  @Override
  public boolean isMin() {
    return this == MIN_VALUE;
  }

  @Override
  protected int compareToSafe(TimeValue v2) {
    if (v2 == null) {
      return 1;
    }
    if (this.value == v2.value) {
      return 0;
    } else if (this.value < v2.value) {
      return -1;
    } else {
      return 1;
    }
  }

  public long formatToLong() {
    long v = this.value / 1000000L;
    String hour = String.format("%02d", v / 3600L);
    String minute = String.format("%02d", v % 3600L / 60L);
    String second = String.format("%02d", v % 60L);
    return Long.parseLong(hour + minute + second);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TimeValue timeValue = (TimeValue) o;
    return fsp == timeValue.fsp && value == timeValue.value && negative == timeValue.negative;
  }

  @Override
  public int hashCode() {
    int result = fsp;
    result = 31 * result + (int) (value ^ (value >>> 32));
    return 31 * result + (negative ? 1 : 0);
  }
}
