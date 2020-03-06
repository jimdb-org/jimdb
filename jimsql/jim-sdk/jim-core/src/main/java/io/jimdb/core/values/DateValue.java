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

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Objects;
import java.util.StringTokenizer;
import java.util.TimeZone;

import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.utils.os.SystemClock;
import io.jimdb.core.types.ValueType;
import io.jimdb.pb.Basepb.DataType;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Represent the DATE/DATETIME/TIMESTAMP type.
 * <p>
 * DATE:YYYY-MM-DD
 * DATETIME/TIMESTAMP:YYYY-MM-DD HH:MM:SS[.fractal]
 *
 * @version V1.0
 */
@SuppressFBWarnings({ "EI_EXPOSE_REP", "FCCD_FIND_CLASS_CIRCULAR_DEPENDENCY" })
public final class DateValue extends Value<DateValue> {
  private static final Logger LOGGER = LoggerFactory.getLogger(DateValue.class);

  protected static final Timestamp TIMESTAMP_EMPTY = null;
  protected static final DateValue DATE_EMPTY = null;

  public static final DateValue TIMESTAMP_MIN_VALUE = new DateValue(TimeUtil.TIMESTAMP_MIN, DataType.TimeStamp,
          TimeUtil.MAX_FSP);
  public static final DateValue TIMESTAMP_MAX_VALUE = new DateValue(TimeUtil.TIMESTAMP_MAX, DataType.TimeStamp,
          TimeUtil.MAX_FSP);
  public static final DateValue DATE_MIN_VALUE = new DateValue(TimeUtil.TIMESTAMP_MIN, DataType.Date, TimeUtil.MAX_FSP);
  public static final DateValue DATE_MAX_VALUE = new DateValue(TimeUtil.TIMESTAMP_MAX, DataType.Date, TimeUtil.MAX_FSP);
  public static final DateValue DATETIME_MIN_VALUE = new DateValue(TimeUtil.TIMESTAMP_MIN, DataType.DateTime,
          TimeUtil.MAX_FSP);
  public static final DateValue DATETIME_MAX_VALUE = new DateValue(TimeUtil.TIMESTAMP_MAX, DataType.DateTime,
          TimeUtil.MAX_FSP);

  private static final String DATE_FORMAT = "yyyy-MM-dd";
  private static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
  private static final SimpleDateFormat DATE_FORMATER = new SimpleDateFormat(DATE_FORMAT);
  private static final SimpleDateFormat DATETIME_FORMATER = new SimpleDateFormat(DATETIME_FORMAT);

  private int fsp;
  private final Timestamp value;
  private DataType dateType;
  private String valueString;

  private DateValue(Timestamp timeValue, DataType dateType, int fsp) {
    this.value = timeValue;
    this.dateType = dateType;
    this.fsp = fsp;
  }

  /**
   * DATE: 1000-01-01 ~ 9999-12-31
   * -30610253143 ~ 253402185600
   * DATETIME:1000-01-01 00:00:00.000000 ~ 9999-12-31 23:59:59.999999
   * TIMESTAMP:'1970-01-01 00:00:01.000000' UTC 到'2038-01-19 03:14:07.999999' UTC mysql
   * TIMESTAMP:'1000-01-01 00:00:00.000000' UTC 到'9999-12-31 23:59:59.999999' UTC we
   * @param valueS
   * @param dateType
   * @param fsp
   * @param zone
   */
  private DateValue(String valueS, DataType dateType, int fsp, TimeZone zone) {
    this.dateType = dateType;
    this.valueString = valueS;
    switch (this.dateType) {
      case DateTime:
        zone = null;
        this.value = buildTimestampFromString(valueS, this.dateType, fsp, zone);
        this.fsp = fsp;
        break;
      case TimeStamp:
        this.value = buildTimestampFromString(valueS, this.dateType, fsp, zone);
        this.valueString = DATETIME_FORMATER.format(this.value);
        this.fsp = fsp;
        break;
      default: {   // Date
        this.value = buildDateFromString(valueS);
        this.fsp = 0;
        break;
      }
    }
  }

  public static DateValue convertToDateValue(long number, DataType dateType, int fsp) {
    DateValue dateValue = convertToDateValue(number);
    dateValue.dateType = dateType;
    dateValue.fsp = fsp;
    return dateValue;
  }

  //number_to_datetime()
  //https://github.com/mysql/mysql-server/blob/5.7/sql-common/my_time.c
  public static DateValue convertToDateValue(long number) {
    DataType type = DataType.Date;
    if (number == 0 || number >= 10000101000000L) {
      type = DataType.DateTime;
      /* 9999-99-99 99:99:99 */
      if (number > 99999999999999L) {
        throw new IllegalArgumentException();
      }
      return createDateValue(number, type);
    }
    if (number < 101) {
      throw new IllegalArgumentException();
    }
    if (number <= (70 - 1) * 1000L + 1231L) {
      number = (number + 20000000L) * 1000000L;
      return createDateValue(number, type);
    }
    if (number < 70 * 10000L + 101L) {
      throw new IllegalArgumentException();
    }
    if (number <= 991231L) {
      number = (number + 19000000L) * 1000000L;                 /* YYMMDD, year: 1970-1999 */
      return createDateValue(number, type);
    }
    if (number < 10000101L) {
      throw new IllegalArgumentException();
    }

    if (number <= 99991231L) {
      number = number * 1000000L;
      return createDateValue(number, type);
    }
    if (number < 101000000L) {
      throw new IllegalArgumentException();
    }

    type = DataType.DateTime;

    if (number <= (70 - 1) * 10000000000L + 1231235959L) {
      number = number + 20000000000000L;                   /* YYMMDDHHMMSS, 2000-2069 */
      return createDateValue(number, type);
    }
    if (number < 70 * 10000000000L + 101000000L) {
      throw new IllegalArgumentException();
    }

    if (number <= 991231235959L) {
      number = number + 19000000000000L;    /* YYMMDDHHMMSS, 1970-1999 */
      return createDateValue(number, type);
    }

    return createDateValue(number, type);
  }

  //time
  public static DateValue convertToDateValue(TimeValue timeValue, DataType dateType, int fsp) {
    long time = timeValue.getValue();

    int hour = (int) (time / (1000 * 1000) / 3600);
    int minute = (int) ((time / 1000 / 1000 - hour * 3600) / 60);
    int second = (int) (time / 1000 / 1000 - hour * 3600 - minute * 60);
    int micros = (int) (time % (1000 * 1000));
    Timestamp timestamp = buildValue(0, 0, 0, hour, minute, second, micros, dateType);
    return new DateValue(timestamp, dateType, fsp);
  }

  private Timestamp buildDateFromString(String valueS) {
    valueS = valueS.trim();

    // truncate fractional part
    int dec = valueS.indexOf('.');
    if (dec > -1) {
      valueS = valueS.substring(0, dec);
    }
    if ("".equals(valueS) || "0".equals(valueS) || "0000-00-00".equals(valueS) || "0000-00-00 00:00:00".equals(valueS)
            || "00000000000000".equals(valueS)) {
      return TIMESTAMP_EMPTY;
    } else {
      int length = valueS.length();

      if (length == 8) {
        return buildValue(1970, 1, 1, 0, 0, 0, 0, DataType.Date); // Return EPOCH for TIME
      } else if (length < 10) {
        throw new IllegalArgumentException(String.format("date %s: no valid ", valueS));
      }

      int year;
      int month;
      int day;
      if (length == 18) {
        StringTokenizer st = new StringTokenizer(valueS, "- ");
        year = Integer.parseInt(st.nextToken());
        month = Integer.parseInt(st.nextToken());
        day = Integer.parseInt(st.nextToken());
      } else {
        year = Integer.parseInt(valueS.substring(0, 4));
        month = Integer.parseInt(valueS.substring(5, 7));
        day = Integer.parseInt(valueS.substring(8, 10));
      }
      return buildValue(year, month, day, 0, 0, 0, 0, DataType.Date);
    }
  }

  //see: mysql-connector ResultSetImpl.java
  private Timestamp buildTimestampFromString(String valueS, DataType type, int fsp, TimeZone zone) {
    if (StringUtils.isBlank(valueS)) {
      throw new IllegalArgumentException(String.format("Incorrect datetime value: '%s' for column 'current_now' at "
              + "row 1", valueS));
    }

    int year = 0;
    int month = 0;
    int day = 0;
    int hour = 0;
    int minutes = 0;
    int seconds = 0;
    int micros = 0;

    valueS = valueS.trim();

    // check for the fractional part
    int decimalIndex = valueS.indexOf('.');
    if (decimalIndex > -1) {
      micros = TimeUtil.handleFractionalPart(fsp, valueS.substring(decimalIndex + 1));
      valueS = valueS.substring(0, decimalIndex);
    }

    int length = valueS.length();

    if ((length > 0) && (valueS.charAt(0) == '0') && ("0".equals(valueS) || "0000-00-00".equals(valueS)
            || "0000-00-00 00:00:00".equals(valueS) || "00000000000000".equals(valueS))) {
      return TIMESTAMP_EMPTY;
    }

    switch (length) {
      //YYYY-MM-DD hh:mm:ss
      case 19: {
        year = Integer.parseInt(valueS.substring(0, 4));
        month = Integer.parseInt(valueS.substring(5, 7));
        day = Integer.parseInt(valueS.substring(8, 10));
        hour = Integer.parseInt(valueS.substring(11, 13));
        minutes = Integer.parseInt(valueS.substring(14, 16));
        seconds = Integer.parseInt(valueS.substring(17, 19));
        break;
      }
      //YYYYMMDDhhmmss
      case 14: {
        year = Integer.parseInt(valueS.substring(0, 4));
        month = Integer.parseInt(valueS.substring(4, 6));
        day = Integer.parseInt(valueS.substring(6, 8));
        hour = Integer.parseInt(valueS.substring(8, 10));
        minutes = Integer.parseInt(valueS.substring(10, 12));
        seconds = Integer.parseInt(valueS.substring(12, 14));
        break;
      }
      //YYMMDDhhmmss
      case 12: {
        year = getYear(valueS.substring(0, 2));
        month = Integer.parseInt(valueS.substring(2, 4));
        day = Integer.parseInt(valueS.substring(4, 6));
        hour = Integer.parseInt(valueS.substring(6, 8));
        minutes = Integer.parseInt(valueS.substring(8, 10));
        seconds = Integer.parseInt(valueS.substring(10, 12));
        break;
      }
      case 10: {
        //YYYY-MM-DD
        if ((dateType == DataType.Date) || (valueS.indexOf('-') != -1)) {
          year = Integer.parseInt(valueS.substring(0, 4));
          month = Integer.parseInt(valueS.substring(5, 7));
          day = Integer.parseInt(valueS.substring(8, 10));
          hour = 0;
          minutes = 0;
        } else {
          //YYMMDDHHMM
          //two-digit year
          year = getYear(valueS.substring(0, 2));
          month = Integer.parseInt(valueS.substring(2, 4));
          day = Integer.parseInt(valueS.substring(4, 6));
          hour = Integer.parseInt(valueS.substring(6, 8));
          minutes = Integer.parseInt(valueS.substring(8, 10));
        }
        break;
      }
      case 8: {
        //hh:mm:ss
        if (valueS.indexOf(':') != -1) {
          hour = Integer.parseInt(valueS.substring(0, 2));
          minutes = Integer.parseInt(valueS.substring(3, 5));
          seconds = Integer.parseInt(valueS.substring(6, 8));
          year = 1970;
          month = 1;
          day = 1;
          break;
        }
        //YYYYMMDD
        year = Integer.parseInt(valueS.substring(0, 4));
        month = Integer.parseInt(valueS.substring(4, 6));
        day = Integer.parseInt(valueS.substring(6, 8));
        break;
      }
      case 6: {
        //YYMMDD
        year = getYear(valueS.substring(0, 2));
        month = Integer.parseInt(valueS.substring(2, 4));
        day = Integer.parseInt(valueS.substring(4, 6));
        break;
      }
      case 4: {
        //YYMM
        year = getYear(valueS.substring(0, 2));
        month = Integer.parseInt(valueS.substring(2, 4));
        day = 1;
        break;
      }
      case 2: {
        //YY
        year = getYear(valueS.substring(0, 2));
        month = 1;
        day = 1;
        break;
      }

      default:
        throw new IllegalArgumentException();
    }
    return buildValue(year, month, day, hour, minutes, seconds, micros, type, zone);
  }

  private static DateValue createDateValue(long number, DataType dateType) {
    long ymdPart = number / 1000000L;
    long mdhPart = number - ymdPart * 1000000L;
    int year = (int) (ymdPart / 10000L);
    ymdPart %= 10000L;
    int month = (int) ymdPart / 100;
    int day = (int) ymdPart % 100;
    int hour = (int) (mdhPart / 10000L);
    mdhPart %= 10000L;
    int minute = (int) (mdhPart / 100);
    int second = (int) (mdhPart % 100);

    Timestamp timestamp = buildValue(year, month, day, hour, minute, second, 0, dateType);
    return new DateValue(timestamp, dateType, TimeUtil.DEFAULT_FSP);
  }

  private static void checkScopeForDate(int year, int month, int day) {
    if (year < 1000 || year > 9999) {
      throw new IllegalArgumentException("date: year");
    }

    if (month < 1 || month > 12) {
      throw new IllegalArgumentException("date: month");
    }

    if (day < 1 || day > 31) {
      throw new IllegalArgumentException("date: day");
    }
  }

  //DATETIME:1000-01-01 00:00:00.000000 ~ 9999-12-31 23:59:59.999999
  private static void checkScopeForDateTime(int year, int month, int day, int hour, int minute, int second) {
    if (year < 1000 || year > 9999) {
      throw new IllegalArgumentException("datetime: year");
    }

    if (month < 1 || month > 12) {
      throw new IllegalArgumentException("datetime: month");
    }

    if (day < 1 || day > 31) {
      throw new IllegalArgumentException("datetime: day");
    }
    if (hour < 0 || hour >= 24) {
      throw new IllegalArgumentException("datetime: hour");
    }

    if (minute < 0 || minute >= 60) {
      throw new IllegalArgumentException("datetime: minute");
    }

    if (second < 0 || second >= 60) {
      throw new IllegalArgumentException("datetime: second");
    }
  }

//  //TIMESTAMP:'1970-01-01 00:00:01.000000' UTC 到'2038-01-19 03:14:07.999999' UTC
//  private static void checkScopeForTimestamp(int year, int month, int day, int hour, int minute, int second) {
//    if (year < 1970 || year > 2038) {
//      throw new IllegalArgumentException("timestamp: year");
//    }
//
//    if (month < 1 || month > 12) {
//      throw new IllegalArgumentException("timestamp: month");
//    }
//
//    if (day < 1 || day > 31) {
//      throw new IllegalArgumentException("timestamp: day");
//    }
//    if (hour < 0 || hour >= 24) {
//      throw new IllegalArgumentException("timestamp: hour");
//    }
//
//    if (minute < 0 || minute >= 60) {
//      throw new IllegalArgumentException("timestamp: minute");
//    }
//
//    if (second < 0 || second >= 60) {
//      throw new IllegalArgumentException("timestamp: second");
//    }
//  }

  private static Timestamp buildValue(int year, int month, int day, int hour, int minute, int second, int micros,
                                      DataType dateType) {
    return buildValue(year, month, day, hour, minute, second, micros, dateType, null);
  }

  private static Timestamp buildValue(int year, int month, int day, int hour, int minute, int second, int micros,
                                      DataType dateType, TimeZone zone) {
    switch (dateType) {
      case Date:
        checkScopeForDate(year, month, day);
        break;
      case TimeStamp:
      case DateTime:
        checkScopeForDateTime(year, month, day, hour, minute, second);
        break;
      default:
        throw new IllegalArgumentException("date value type");
    }

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("DateValue: {}-{}-{} {}:{}:{}.{}", year, month, day, hour, minute, second, micros);
    }
    Calendar dateCal = new GregorianCalendar(year, month - 1, day, hour, minute, second);
    Timestamp ts = new Timestamp(dateCal.getTimeInMillis());
    ts.setNanos(micros * 1000);
    if (zone != null) {
      return TimeUtil.changeTimeZone(ts, zone, TimeUtil.UTC_ZONE);
    }
    return ts;
  }

  private int getYear(String sValue) {
    YearValue yearValue = YearValue.getInstance(sValue);
    return yearValue.getValue();
  }

  public String convertToString(DataType type) {
    if (type == null) {
      type = this.getDateType();
    }
    String dataEncode;
    Timestamp value = this.getValue();
    if (type == DataType.Date) {
      if (value == null) {
        dataEncode = "0000-00-00";
      } else {
        dataEncode = new SimpleDateFormat(DATE_FORMAT).format(value);
      }
      return dataEncode;
    }
    String fspPart;
    if (value == null) {
      dataEncode = "0000-00-00 00:00:00";
      fspPart = "000000";
    } else {
      dataEncode = DATETIME_FORMATER.format(value);
      fspPart = String.format("%06d", value.getNanos() / 1000);
    }
    if (this.fsp > 0) {
      dataEncode = String.format("%s.%s", dataEncode, fspPart.substring(0, fsp));
    }

    return dataEncode;
  }

  public static DateValue getNow(DataType dateType, int fsp, TimeZone fromZone) {
    Timestamp now;
    if (DataType.TimeStamp != dateType) {
      now = new Timestamp(SystemClock.currentTimeMillis());
    } else {
      Calendar calendar = Calendar.getInstance(fromZone);
      Timestamp from = new Timestamp(calendar.getTimeInMillis());
      now = TimeUtil.changeTimeZone(from, fromZone, TimeUtil.UTC_ZONE);
    }
    int micros = 0;
    if (fsp > 0) {
      micros = TimeUtil.handleFractionalPart(fsp, String.format("%d", now.getNanos() / 1000));
    }
    now.setNanos(micros * 1000);

    return new DateValue(now, dateType, fsp);
  }

  public static DateValue convertToMysqlNow(DateValue dateValue, int fsp) {
    if (dateValue.fsp == fsp) {
      return dateValue;
    }
    Timestamp value = (Timestamp) dateValue.value.clone();
    int micros = 0;
    if (fsp > 0) {
      String fractionalPartStr = String.format("%d", value.getNanos() / 1000);
      micros = Integer.parseInt(fractionalPartStr.substring(0, fsp + 1));
    }
    value.setNanos(micros * 1000);
    return new DateValue(value, DataType.DateTime, fsp);
  }

  public static DateValue getInstance(String value, DataType dateType, int fsp, TimeZone zone) {
    return new DateValue(value, dateType, fsp, zone);
  }

  public static DateValue getInstance(String value, DataType dateType, int fsp) {
    return new DateValue(value, dateType, fsp, null);
  }

  public static DateValue getInstance(String value, DataType dateType) {
    return new DateValue(value, dateType, TimeUtil.MAX_FSP, null);
  }

  public static DateValue getInstance(long value, DataType dateType, int fsp) {
    Timestamp timeValue = TimeUtil.decodeUint64ToTimestamp(value);
    return new DateValue(timeValue, dateType, fsp);
  }

  public static DateValue convertTimeZone(DateValue fromValue, TimeZone fz, TimeZone tz) {
    Timestamp timeValue = TimeUtil.changeTimeZone(fromValue.value, fz, tz);
    return new DateValue(timeValue, fromValue.getDateType(), fromValue.getFsp());
  }

  public Timestamp getValue() {
    return this.value;
  }

  public DataType getDateType() {
    return dateType;
  }

  public int getFsp() {
    return fsp;
  }

  @Override
  public ValueType getType() {
    return ValueType.DATE;
  }

  @Override
  public String getString() {
    return String.format("%d", TimeUtil.encodeTimestampToUint64(this.value));
  }

  @Override
  public byte[] toByteArray() {
    return value.toString().getBytes(StandardCharsets.UTF_8);
  }

  @Override
  protected int compareToSafe(DateValue v2) {
    return value.compareTo(v2.value);
  }

  @Override
  public boolean isMax() {
    switch (dateType) {
      case TimeStamp:
        return this == TIMESTAMP_MAX_VALUE;
      case Date:
        return this == DATE_MAX_VALUE;
      case DateTime:
        return this == DATETIME_MAX_VALUE;
      default:
        throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_TYPE_INVALID);
    }
  }

  @Override
  public boolean isMin() {
    switch (dateType) {
      case TimeStamp:
        return this == TIMESTAMP_MIN_VALUE;
      case Date:
        return this == DATE_MIN_VALUE;
      case DateTime:
        return this == DATETIME_MIN_VALUE;
      default:
        throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_TYPE_INVALID);
    }
  }

  public long formatToLong() {
    return Long.parseLong(new SimpleDateFormat("yyyyMMddHHmmss").format(this.value.getTime()));
  }

  public String formatToString() {
    if (valueString != null) {
      return valueString;
    }
    if (this.dateType == DataType.Date) {
      return DATE_FORMATER.format(this.value.getTime());
    } else {
      return DATETIME_FORMATER.format(this.value.getTime());
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DateValue dateValue = (DateValue) o;
    return fsp == dateValue.fsp && Objects.equals(value, dateValue.value) && dateType == dateValue.dateType;
  }

  @Override
  public int hashCode() {
    int result = fsp;
    result = 31 * result + (value != null ? value.hashCode() : 0);
    return 31 * result + (dateType != null ? dateType.getNumber() : 0);
  }
}
