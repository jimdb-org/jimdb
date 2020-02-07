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
package io.jimdb.values;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import io.jimdb.common.utils.os.SystemClock;
import io.jimdb.core.values.DateValue;
import io.jimdb.core.values.TimeUtil;
import io.jimdb.pb.Basepb.DataType;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * DateValue Tester.
 *
 * @version 1.0
 */
public class DateValueTest {

  @Before
  public void before() throws Exception {
  }

  @After
  public void after() throws Exception {
  }

  @Test
  public void testCal() throws Exception {
    //    9999-12-31 23:59:59.999999
    Calendar dateCal = new GregorianCalendar();
    dateCal.clear();
    dateCal.set(9999, 11, 31, 23, 59, 59);
    Timestamp ts = new Timestamp(dateCal.getTimeInMillis());
    ts.setNanos(999999000);
    System.out.println(ts.getTime() / 1000);
    System.out.println(ts.getNanos());
  }

  @Test
  public void testTimeZone() throws Exception {

    TimeZone localZone = TimeZone.getTimeZone("Asia/Shanghai");

    Timestamp timestamp = new Timestamp(SystemClock.currentTimeMillis());
    System.out.println(timestamp);

    Timestamp targetTimestamp = TimeUtil.changeTimeZone(timestamp, localZone, TimeZone.getTimeZone("America/Chicago"));
    System.out.println(targetTimestamp);

    Timestamp targetTimestamp2 = TimeUtil.changeTimeZone(timestamp, localZone, TimeUtil.UTC_ZONE);
    System.out.println(targetTimestamp2);

    DateValue timestampValue = DateValue.getNow(DataType.TimeStamp, 3, localZone);
    System.out.println(timestampValue.convertToString(null, localZone));

    DateValue datetimeValue = DateValue.getNow(DataType.DateTime, 4, null);
    System.out.println(datetimeValue.convertToString(null, null));

    DateValue dateValue = DateValue.getNow(DataType.Date, 0, null);
    System.out.println(dateValue.convertToString(null, null));

//    String ids[] = TimeZone.getAvailableIDs();
//    System.out.println(TimeZone.getDefault().getDisplayName());
//    for(String id: ids) {
//      System.out.println(id);
//    }
  }

  @Test
  public void testGetInstanceFromString() throws Exception {
    String[] sValues = new String[]{ "2015-07-21 12:12:12.1212",
            "2015-07-21 12:12:12.",
            "2015-07-21",
            "20150721" };
    for (String s : sValues) {
      DateValue value = DateValue.getInstance(s, DataType.DateTime, 6, null);
      System.out.println("datetime: " + value.getValue());
    }
    //2015-07-21 12:12:12.1212
    //2015-07-21 12:12:12.0
    //2015-07-21 00:00:00.0
    //2015-07-21 00:00:00.0

    for (String s : sValues) {
      DateValue value = DateValue.getInstance(s, DataType.TimeStamp, 6, null);
      System.out.println("timestamp: " + value.getValue());
    }
    //2015-07-21 12:12:12.1212
    //2015-07-21 12:12:12.0
    //2015-07-21 00:00:00.0
    //2015-07-21 00:00:00.0

    for (String s : sValues) {
      DateValue value = DateValue.getInstance(s, DataType.Date, 0, null);
      System.out.println("date: " + value.getValue());
    }
    //2015-07-21 00:00:00.0
    //2015-07-21 00:00:00.0
    //2015-07-21 00:00:00.0
    //1970-01-01 00:00:00.0
  }

  @Test
  public void testGetInstanceFromLong() throws Exception {

    Long[] lValues = new Long[]{ 1217172222L, 15708451193L };

    //2000-12-17 17:22:22.0
    //2005-09-09 21:12:33.0  -> 2001-57-8 45:11:93

    for (Long l : lValues) {
      DateValue value = DateValue.convertToDateValue(l);
      System.out.println(value.getValue());
    }
  }

  @Test
  public void testTimestampFromLong() {
    Timestamp timestamp = new Timestamp(2019 - 1900, 9, 29, 0, 0, 0, 0);
    System.out.println(timestamp);
    System.out.println(TimeUtil.encodeTimestampToUint64(timestamp));
    long tt = 1847735887547334656L;
    System.out.println(TimeUtil.decodeUint64ToTimestamp(tt));
  }
}
