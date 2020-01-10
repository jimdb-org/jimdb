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
package io.jimdb.test.mysql.dml;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import io.jimdb.test.TestUtil;
import io.jimdb.test.mysql.SqlTestBase;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;

import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

/**
 * Execute sql with prepare mode , if the result is correct, rebuild range is correct.
 * @version V1.0
 */
public class RangeRebuildTest extends SelectIndexTest {

  private static final Logger LOG = LoggerFactory.getLogger(RangeRebuildTest.class);

  @Test
  public void testSelect() {
    SelectCols select = new SelectCols(INT_TABLE_DATARESULT, new String[] { "i1", "i2", "i3" } );
    List<String> expected = this.filter(select,
            row -> row.get("i1").compareTo(1) == 0
                    && row.get("i2").compareTo(2005) == 1
                    && row.get("i2").compareTo(2007) == -1);
    LOG.info("expected ==> "+JSON.toJSONString(expected));
    Assert.assertTrue("expected is empty !", expected.isEmpty());
    execPrepareQuery("select " + select.select + " from " + select.tableName + " where i1 = ? and i2 > ? and i2 < ? ", expected,1,2005,2007);
  }


  @Test
  public void testUpdate() {
    SelectCols select = new SelectCols(INT_TABLE_DATARESULT, new String[] { "i1", "i2", "i3" } );
    List<String> expected = this.filter(select,
            row -> row.get("i1").compareTo(1) == 0
                    );
    LOG.info("expected ==> {}", JSON.toJSONString(expected));

    //prepare argument refresh has a bug
//    execPrepareUpdate("update " + select.tableName + " set i1 = ? where i1 = ? ", expected.size(), 100,1);
//    execPrepareUpdate("update " + select.tableName + " set i1 = ? where i1 = ? ", expected.size(), 1,100);
  }


  @Test
  public void testIndex4Int02() {
    SelectCols select = new SelectCols(INT_TABLE_DATARESULT, new String[] { "i1", "i2", "i3" } );
    List<String> expected = this.filter(select,
            row -> row.get("i1").compareTo(3) == 0
                    && row.get("i2").compareTo(2000) == 0
                    && row.get("i3").compareTo(3030) == 0);
    LOG.info("expected ==> {}", JSON.toJSONString(expected));
    Assert.assertTrue("expected is empty !", !expected.isEmpty());
    execPrepareQuery("select "+select.select+" from " + select.tableName + " where i1 = ? and i2 = ? and i3 = ? ", expected,3,2000,3030);
  }

  @Test
  public void testIndex4BigInt01() {
    SelectCols select = new SelectCols(INT_TABLE_DATARESULT, new String[] { "b1", "b2", "b3" } );
    List<String> expected = this.filter(select,
            row -> row.get("b1").compareTo(10000L) == 0
                    && row.get("b2").compareTo(12006L) == 1
                    && row.get("b2").compareTo(12009L) == -1);
    LOG.info("expected ==> {}", JSON.toJSONString(expected));
    Assert.assertTrue("expected is empty !", !expected.isEmpty());
    execPrepareQuery("select " +select.select+ " from " + select.tableName + " where b1 = ? and b2 > ?  and b2 < ? ", expected,10000,12006 , 12009);
  }

  @Test
  public void testIndex4BigInt02() {
    SelectCols select = new SelectCols(INT_TABLE_DATARESULT, new String[] { "b1", "b2", "b3" } );
    List<String> expected = this.filter(select,
            row -> row.get("b1").compareTo(10000L) == 0
                    && row.get("b2").compareTo(12006L) == 0
                    && row.get("b3").compareTo(13006L) == 0);
    LOG.info("expected ==> {}", JSON.toJSONString(expected));
    Assert.assertTrue("expected is empty !", !expected.isEmpty());
    execPrepareQuery("select " + select.select + " from " + select.tableName + " where b1 = ? and b2 = ? and b3 = ?", expected,10000,12006,13006);
  }


  @Test
  public void testIndex4Varchar01() {
    SelectCols select = new SelectCols(INT_TABLE_DATARESULT, new String[] { "v1", "v2", "v3" } );
    List<String> expected = this.filter(select,
            row -> row.get("v1").compareTo("v1-9") == 0
                    && row.get("v2").compareTo("v2-9") < 0
                    && row.get("v2").compareTo("v2-6") > 0);
    LOG.info("expected ==> {}", JSON.toJSONString(expected));
    Assert.assertTrue("expected is empty !", !expected.isEmpty());
    execPrepareQuery("select " + select.select + " from " + select.tableName + " where v1 = ? and v2 < ? and v2 > ? ", expected, "v1-9","v2-9","v2-6");
  }


  @Test
  public void testIndex4Double02() {
    SelectCols select = new SelectCols(FLOAT_TABLE_DATARESULT, new String[] { "d1", "d2", "d3" } );
    List<String> expected = this.filter(select,
            row -> row.get("d1").compareTo(13.5) == 0
                    && row.get("d2").compareTo(15000.0) == 0
                    && row.get("d3").compareTo(475000.0) == 0);
    LOG.info("expected ==> {}", JSON.toJSONString(expected));
    Assert.assertTrue("expected is empty !", !expected.isEmpty());
    execPrepareQuery("select " + select.select + " from " + select.tableName + " where d1 = ? and d2 = ? and d3 = ? ", expected,13.5 ,15000.0,475000.0);
  }

//
//  @Test
//  public void testIndex4DFD_UNIQ() {
//    SelectCols select = new SelectCols(FLOAT_TABLE_DATARESULT, new String[] { "d3", "f3", "decimal3" } );
//    List<String> expected = this.filter(select,
//            row -> row.get("d3").compareTo(13.5f) == 0
//                    && row.get("f3").compareTo(15000.0f) == 0
//                    && row.get("decimal3").compareTo(15000.0f) == 0);
//    LOG.info("expected ==> {}", JSON.toJSONString(expected));
//    Assert.assertTrue("expected is empty !", !expected.isEmpty());
//    execPrepareQuery("select " + select.select + " from " + select.tableName + " where d3 = 1 and f3 = 1 and decimal3 = 1 ", expected,13.5f,15000.0f,15000.0f);
//  }
//
//  @Test
//  public void testIndex4_DATE_COMPOSITE_UNIQ() throws ParseException {
//    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
//    DateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
//    DateFormat yearFormat = new SimpleDateFormat("yyyy");
//    Date date1 = dateFormat.parse("1990-01-03");
//    Date datetime1 = dateTimeFormat.parse("1990-01-03 12:00:00");
//    Date year1 = yearFormat.parse("2036");
//
//    SelectCols select = new SelectCols(DATETIME_TABLE_DATARESULT, new String[] { "date2", "datetime2" ,"year2" } );
//    List<String> expected = this.filter(select,
//            row -> row.get("date2").compareTo(date1) == 0
//                    && row.get("datetime2").compareTo("1990-01-03 12:00:00") == 0
//                    && row.get("year2").compareTo(year1) == 0);
//    execPrepareQuery("select " + select.select + " from " + select.tableName + " where date2 = '1990-01-03' and datetime2 = '1990-01-03 12:00:00' and year2 = 2036", expected);
//  }
//
//  @Test
//  public void testIndex4_TIME_COMPOSITE01() throws ParseException {
//    DateFormat timeFormat = new SimpleDateFormat("hh:mm:ss");
//    DateFormat timeStampFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
//    Date time1 = timeFormat.parse("12:00:20");
//    Date timestamp1 = timeStampFormat.parse("1990-01-02 11:00:00");
//
//    SelectCols select = new SelectCols(DATETIME_TABLE_DATARESULT, new String[] { "time1", "timestamp1" } );
//    List<String> expected = this.filter(select,
//            row -> row.get("time1").compareTo(time1) == 0
//                    && row.get("timestamp1").compareTo(timestamp1) == 1);
//    execPrepareQuery("select " + select.select + " from " + select.tableName + " where time1 = '12:00:20' and timestamp1 > '1990-01-02 11:00:00' ", expected);
//  }
}
