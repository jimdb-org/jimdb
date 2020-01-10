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

import java.math.BigInteger;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import io.jimdb.test.mysql.SqlTestBase;

import org.junit.After;
import org.junit.Test;

import reactor.util.function.Tuple2;
import reactor.util.function.Tuple3;
import reactor.util.function.Tuples;

/**
 * @version V1.0
 */
public class ValueCheckerTest extends SqlTestBase {
  private static String catalogName = "maggie";
  private static String tableName = "sqlscope";

  private static final String EXTRA = "comment 'REPLICA =1' \nengine = memory";

  public void createCatalogIfNotExist() {
    String sql = String.format("Create database IF NOT EXISTS %s ", catalogName);
    execUpdate(sql, 0, true);
  }

  public void createTable(String sql) {
    execUpdate(sql, 0, true);
  }

  public void dropTableIfExist() {
    //delete table
    execUpdate(String.format("drop table IF EXISTS %s.%s", catalogName, tableName), 0, true);
  }

  public void prepareTable(String sql) {

    createCatalogIfNotExist();

    dropTableIfExist();

    try {
      Thread.sleep(1000);
    } catch (Exception ignored) {
    }

    createTable(sql);

    try {
      Thread.sleep(2000);
    } catch (Exception ignored) {
    }
  }

  @After
  public void tearDown() {
    try {
      dropTableIfExist();

      Thread.sleep(1000);
    } catch (Exception ignored) {
    }
  }

  //default signed
  @Test
  public void testIntSigned() {
    String sql = String.format("create table %s.%s (id bigint unsigned primary key auto_increment, " +
                    "v_tiny tinyint null," +
                    "v_small smallint null," +
                    "v_medium mediumint null," +
                    "v_int int null," +
                    "v_bigint bigint null) " +
                    "AUTO_INCREMENT=0 %s",
            catalogName, tableName, EXTRA);

    //create table
    prepareTable(sql);

    //tinyint    -128 ~ 127
    //smallint   -32768 ~ 32767
    //mediumint  -8388608 ~ 8388607
    //int        -2147483648 ~ 2147483647
    //bigint     -9223372036854775808~9223372036854775807
    List<Tuple3<String, Long, Long>> singedList = new ArrayList<>();
    singedList.add(Tuples.of("v_tiny", Long.valueOf(Byte.MIN_VALUE), Long.valueOf(Byte.MAX_VALUE)));
    singedList.add(Tuples.of("v_small", Long.valueOf(Short.MIN_VALUE), Long.valueOf(Short.MAX_VALUE)));
    singedList.add(Tuples.of("v_medium", Long.valueOf(~((1 << 23) - 1)), Long.valueOf((1 << 23) - 1)));
    singedList.add(Tuples.of("v_int", Long.valueOf(Integer.MIN_VALUE), Long.valueOf(Integer.MAX_VALUE)));
    singedList.add(Tuples.of("v_bigint", Long.MIN_VALUE, Long.MAX_VALUE));

    for (int i = 0; i < singedList.size(); i++) {
      Tuple3 tuple3 = singedList.get(i);
      String field = (String) tuple3.getT1();
      Long minValue = (Long) tuple3.getT2();
      Long maxValue = (Long) tuple3.getT3();

      String minValueLower = Long.toString(minValue - 1);
      String maxValueUpper = Long.toString(maxValue + 1);
      if (i == singedList.size() - 1) {
        minValueLower = new BigInteger("-9223372036854775809").toString();
        maxValueUpper = new BigInteger("9223372036854775808").toString();
      }

      String insertSql = String.format("insert into %s.%s (%s) values (%s)", catalogName, tableName, field, minValueLower);
      SQLException exception = new SQLException(
              String.format("Data truncation: Out of range value for column '%s' at row 1", field), "22001", 1264);
      execUpdate(insertSql, exception, true);

      insertSql = String.format("insert into %s.%s (%s) values (%d), (0), (%d)", catalogName, tableName, field, minValue, maxValue);
      execUpdate(insertSql, 3, true);

      String selectSql = String.format("select %s from %s.%s where %s = %d or %s = 0 or %s = %d ",
              field, catalogName, tableName, field, minValue, field, field, maxValue);
      List<String> s = execQuery(selectSql);
      System.out.println(Arrays.toString(s.toArray()));

      String deleteSql = String.format("delete from %s.%s where %s = %d or %s = 0 or %s = %d", catalogName, tableName,
              field, minValue, field, field, maxValue);
      execUpdate(deleteSql, 3, true);

      insertSql = String.format("insert into %s.%s  (%s) values (%s)", catalogName, tableName, field, maxValueUpper);
      exception = new SQLException(
              String.format("Data truncation: Out of range value for column '%s' at row 1", field), "22001", 1264);
      execUpdate(insertSql, exception, true);
    }
  }

  @Test
  public void testIntUnsigned() {
    String sql = String.format("create table %s.%s (id bigint unsigned primary key auto_increment, " +
                    "v_tiny tinyint unsigned null," +
                    "v_small smallint unsigned null," +
                    "v_medium mediumint unsigned null," +
                    "v_int int unsigned null," +
                    "v_bigint bigint unsigned null) " +
                    "AUTO_INCREMENT=0 %s",
            catalogName, tableName, EXTRA);

    //create table
    prepareTable(sql);

    //tinyint   unsigned  0 ~ 255
    //smallint  unsigned  0 ~ 65535
    //mediumint unsigned  0 ~ 16777215
    //int       unsigned  0 ~ 4294967295
    //bigint    unsigned  0 ~ 18446744073709551615
    List<Tuple2<String, BigInteger>> unsingedList = new ArrayList<>();
    unsingedList.add(Tuples.of("v_tiny", BigInteger.valueOf((1 << 8) - 1)));
    unsingedList.add(Tuples.of("v_small", BigInteger.valueOf((1 << 16) - 1)));
    unsingedList.add(Tuples.of("v_medium", BigInteger.valueOf((1 << 24) - 1)));
    unsingedList.add(Tuples.of("v_int", BigInteger.valueOf((1L << 32) - 1)));
    unsingedList.add(Tuples.of("v_bigint", new BigInteger("18446744073709551615")));

    BigInteger minValue = new BigInteger("0");
    for (int i = 0; i < unsingedList.size(); i++) {
      Tuple2 tuple2 = unsingedList.get(i);
      String field = (String) tuple2.getT1();
      BigInteger maxValue = (BigInteger) tuple2.getT2();

      String minValueLower = BigInteger.valueOf(-1).toString();
      String maxValueUpper = maxValue.add(BigInteger.valueOf(1)).toString();

      String insertSql = String.format("insert into %s.%s (%s) values (%s)", catalogName, tableName, field, minValueLower);
      SQLException exception = new SQLException(
              String.format("Data truncation: Out of range value for column '%s' at row 1", field), "22001", 1264);
      execUpdate(insertSql, exception, true);

      insertSql = String.format("insert into %s.%s (%s) values (%d), (%d)", catalogName, tableName, field, minValue, maxValue);
      execUpdate(insertSql, 2, true);

      String selectSql = String.format("select %s from %s.%s where %s = %d or %s = %d ",
              field, catalogName, tableName, field, minValue, field, maxValue);
      List<String> s = execQuery(selectSql);
      System.out.println(Arrays.toString(s.toArray()));

      String deleteSql = String.format("delete from %s.%s where %s = %d or %s = %d", catalogName, tableName,
              field, minValue, field, maxValue);
      execUpdate(deleteSql, 2, true);

      insertSql = String.format("insert into %s.%s  (%s) values (%s)", catalogName, tableName, field, maxValueUpper);
      exception = new SQLException(
              String.format("Data truncation: Out of range value for column '%s' at row 1", field), "22001", 1264);
      execUpdate(insertSql, exception, true);
    }
  }

  @Test
  public void testDecimal() {

  }

  @Test
  public void testFloatDouble() {

  }

  //date       1000-01-01 ~ 9999-12-31
  //datetime   1000-01-01 00:00:00.000000 ~ 9999-12-31 23:59:59.999999
  //timestamp  '1000-01-01 00:00:00.000000' UTC 到'9999-12-31 23:59:59.999999' UTC
  //time       '-838:59:59.000000' ~ '838:59:59.000000'
  //year       0 ~ 2155
  @Test
  public void testDateTimeYear() {
    String sql = String.format("create table %s.%s (id bigint unsigned primary key auto_increment, " +
                    "v_date date null," +
                    "v_datetime datetime null," +
                    "v_timestamp timestamp null," +
                    "v_time time null," +
                    "v_year year null) " +
                    "AUTO_INCREMENT=0 %s",
            catalogName, tableName, EXTRA);

    //create table
    prepareTable(sql);

    List<Tuple2<String, String>> dateBound = new ArrayList<>();
    dateBound.add(Tuples.of("1000-01-01", "9999-12-31"));
    dateBound.add(Tuples.of("1000-01-01 00:00:00.000000", "9999-12-31 23:59:59.999999"));
    dateBound.add(Tuples.of("1000-01-01 00:00:00.000000", "9999-12-31 23:59:59.999999"));
    dateBound.add(Tuples.of("-838:59:59.000000", "9999-12-31"));
    dateBound.add(Tuples.of("-838:59:59.000000", "838:59:59.000000"));
    dateBound.add(Tuples.of("0", "2155"));

    for (int i = 0; i < dateBound.size(); i++) {
//      Tuple2 tuple2 = dateBound.get(i);
//      String minValue = (String) tuple2.getT1();
//      String maxValue = (String) tuple2.getT2();
//
//      String insertSql = String.format("insert into %s.%s (%s) values (%s)", catalogName, tableName, field, minValueLower);
//      SQLException exception = new SQLException(
//              String.format("Data truncation: Out of range value for column '%s' at row 1", field), "22001", 1264);
//      execUpdate(insertSql, exception, true);
//
//      insertSql = String.format("insert into %s.%s (%s) values (%d), (0), (%d)", catalogName, tableName, field, minValue, maxValue);
//      execUpdate(insertSql, 3, true);
//
//      String selectSql = String.format("select %s from %s.%s where %s = %d or %s = 0 or %s = %d ",
//              field, catalogName, tableName, field, minValue, field, field, maxValue);
//      List<String> s = execQuery(selectSql);
//      System.out.println(Arrays.toString(s.toArray()));
//
//      String deleteSql = String.format("delete from %s.%s where %s = %d or %s = 0 or %s = %d", catalogName, tableName,
//              field, minValue, field, field, maxValue);
//      execUpdate(deleteSql, 3, true);
//
//      insertSql = String.format("insert into %s.%s  (%s) values (%s)", catalogName, tableName, field, maxValueUpper);
//      exception = new SQLException(
//              String.format("Data truncation: Out of range value for column '%s' at row 1", field), "22001", 1264);
//      execUpdate(insertSql, exception, true);
    }
  }

  @Test
  public void testChar() {

  }
}
