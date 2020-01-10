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

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @version V1.0
 */
public class TimeUtil {
  private static final Logger LOGGER = LoggerFactory.getLogger(TimeUtil.class);

  public static final TimeZone UTC_ZONE = TimeZone.getTimeZone("UTC");

  protected static final int MAX_FSP = 6;
  protected static final int MIN_FSP = 0;
  protected static final int DEFAULT_FSP = 0;

  protected static final int NANO_SECOND = 1;
  private static final int MICRO_SECOND = 1000 * NANO_SECOND;
  private static final int MILL_SECOND = 1000 * MICRO_SECOND;
  private static final int SECOND = 1000 * MILL_SECOND;
  private static final int MINUTE = 60 * SECOND;
  private static final int HOUR = 60 * MINUTE;

  protected static final int TIME_MAX_HOUR = 838;
  protected static final int TIME_MAX_MINUTE = 59;
  protected static final int TIME_MAX_SECOND = 59;

  //838:59:59.000000, TIME_MAX_HOUR*10000 + TIME_MAX_MINUTE*100 + TIME_MAX_SECOND + 0 micro second
  protected static final int TIME_MAX_VALUE = 843959;

  protected static final Timestamp TIMESTAMP_MAX = Timestamp.valueOf("9999-12-31 23:59:59.999999");
  protected static final Timestamp TIMESTAMP_MIN = Timestamp.valueOf("1000-01-01 00:00:00.000000");

  protected static int handleFractionalPart(int fsp, String fractionalPartS) {
    if (StringUtils.isBlank(fractionalPartS)) {
      return 0;
    }
    int fractionalP;
    int actFsp = fractionalPartS.length();
    // on fsp scope
    if (actFsp <= fsp) {
      fractionalP = Integer.parseInt(fractionalPartS);
      fractionalP = fractionalP * (int) Math.pow(10, TimeUtil.MAX_FSP - actFsp);
    } else {
      //round
      fractionalP = Integer.parseInt(fractionalPartS.substring(0, fsp + 1));
      fractionalP = (fractionalP + 5) / 10;
      fractionalP = fractionalP * (int) Math.pow(10, TimeUtil.MAX_FSP - fsp);
    }
    return fractionalP;
  }

  //
//    1 bit  0
//   17 bits year*13+month   (year 0-9999, month 0-12)
//    5 bits day             (0-31)
//    5 bits hour            (0-23)
//    6 bits minute          (0-59)
//    6 bits second          (0-59)
//   24 bits microseconds    (0-999999)
//
//   Total: 64 bits = 8 bytes
//
//   0YYYYYYY.YYYYYYYY.YYdddddh.hhhhmmmm.mmssssss.ffffffff.ffffffff.ffffffff
//
  public static long encodeTimestampToUint64(Timestamp time) {
    if (time == null) {
      return 0;
    }

    int year = time.getYear() + 1900;
    int month = time.getMonth() + 1;
    int day = time.getDate();
    int hour = time.getHours();
    int minute = time.getMinutes();
    int second = time.getSeconds();
    int micros = time.getNanos() / 1000;
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("time: {}-{}-{} {}:{}:{}.{}", year, month, day, hour, minute, second, micros);
    }
    long ymd = ((year * 13 + month) << 5) | day;
    long hms = hour << 12 | minute << 6 | second;
    return ((ymd << 17 | hms) << 24) | micros;
  }

  public static Timestamp decodeUint64ToTimestamp(long timeLong) {
    if (timeLong == 0) {
      return null;
    }
    long ymdHms = timeLong >> 24;
    long ymd = ymdHms >> 17;
    int day = (int) ymd & ((1 << 5) - 1);
    long ym = ymd >> 5;
    int month = (int) (ym % 13);
    int year = (int) (ym / 13);

    long hms = ymdHms & ((1 << 17) - 1);
    int second = (int) (hms & ((1 << 6) - 1));
    int minute = (int) ((hms >> 6) & ((1 << 6) - 1));
    int hour = (int) (hms >> 12);
    int microSec = (int) (timeLong % (1 << 24));
    return new Timestamp(year - 1900, month - 1, day, hour, minute, second, microSec * 1000);
  }

  public static Timestamp changeTimeZone(Timestamp from, TimeZone fromTz, TimeZone toTz) {
    if (fromTz == null || toTz == null || fromTz.getID().equals(toTz.getID())) {
      return from;
    }
    Calendar fromCal = Calendar.getInstance(fromTz);
    fromCal.setTime(from);

    int fromOffset = fromCal.get(Calendar.ZONE_OFFSET) + fromCal.get(Calendar.DST_OFFSET);
    Calendar toCal = Calendar.getInstance(toTz);
    toCal.setTime(from);

    int toOffset = toCal.get(Calendar.ZONE_OFFSET) + toCal.get(Calendar.DST_OFFSET);
    int offsetDiff = fromOffset - toOffset;

    long toTime = toCal.getTime().getTime();
    toTime -= offsetDiff;
    return new Timestamp(toTime);
  }
}
