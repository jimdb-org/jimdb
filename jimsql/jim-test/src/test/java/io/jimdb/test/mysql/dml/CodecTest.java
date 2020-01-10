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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import io.jimdb.test.TestUtil;
import io.jimdb.test.mysql.SqlTestBase;

import org.junit.Before;
import org.junit.Test;

/**
 * @version V1.0
 */
public class CodecTest extends SqlTestBase {
  private static String catalogName = "maggie";
  private static String tableName = "sqltest";

  private static final String EXTRA = "comment 'REPLICA =1' \nengine = memory";

  public void createTable() {
    String sql = String.format("create table %s.%s (id bigint unsigned primary key auto_increment, " +
                    "name varchar(20) not null, " +
                    "num bigint unsigned not null," +
                    "UNIQUE KEY nameidx (name) ) " +
                    "AUTO_INCREMENT=0  %s",
            catalogName, tableName, EXTRA);
    execUpdate(sql, 0, true);


  }

  @Before
  public void tearUp() {
//    prepareTable();
  }

  public void prepareTable() {
//    createCatalog(catalogName);
//    try {
//      Thread.sleep(1000);
//    } catch (Exception ignored) {
//    }

    //delete table
    execUpdate(String.format("drop table IF EXISTS %s.%s", catalogName, tableName), 0, true);

    try {
      Thread.sleep(1000);
    } catch (Exception ignored) {
    }

    createTable();

    try {
      Thread.sleep(2000);
    } catch (Exception ignored) {
    }
  }

  @Test
  public void testBatchInsert() {
    prepareTable();
    int num = 5000;
    CountDownLatch latch = new CountDownLatch(num);
    ThreadPoolExecutor executor = new ThreadPoolExecutor(8,
            8, 1, TimeUnit.MINUTES, new LinkedBlockingDeque<>(num));
    long startTime = System.currentTimeMillis();
    for (int i = 1; i <= num; i++) {
      int temp = i;
      executor.execute(() -> {
        try {
          String sql = "INSERT INTO maggie.sqltest(name, num) VALUES(" + String.format("'%s',14757395258967667293", temp) + ")";
          System.out.println(sql);
          execUpdate(sql, 1, true);
        } catch (Throwable e) {
          e.printStackTrace();
          System.out.println("executor err " + e.getMessage());
          return;
        } finally {
          latch.countDown();
        }
      });
    }
    try {
      latch.await();
      long elapsedTime = System.currentTimeMillis() - startTime;
      System.out.println("execute " + num + " elapse time:" + elapsedTime);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      executor.shutdown();
    }
  }

  @Test
  public void testSelect() {
    int num = 10;
    CountDownLatch latch = new CountDownLatch(num);
    ThreadPoolExecutor executor = new ThreadPoolExecutor(8,
            8, 1, TimeUnit.MINUTES, new LinkedBlockingDeque<>(num));
    for (int i = 1; i <= num; i++) {
      int temp = i;
      executor.execute(() -> {
        try {
          String sql = "select id, name, num from maggie.sqltest where name = " + String.format("'%s'", temp);
          System.out.println(sql);
          List<String> s = execQuery(sql);
          System.out.println(Arrays.toString(s.toArray()));
        } catch (Throwable e) {
          e.printStackTrace();
          System.out.println("executor err " + e.getMessage());
          return;
        } finally {
          latch.countDown();
        }
      });
    }
    try {
      latch.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      executor.shutdown();
    }
  }

  @Test
  public void testBatchDelete() {
    for (int i = 1; i < 5; i++) {
      try {
        String sql = "delete from maggie.sqltest where name = " + String.format("%s", i);
        System.out.println(sql);
        execUpdate(sql, 1, true);
      } catch (Throwable e) {
        e.printStackTrace();
        System.out.println("executor err " + e.getMessage());
        return;
      }
    }
  }

  /**
   *
   * @see @https://dev.mysql.com/doc/refman/5.7/en/integer-types.html
   */
  @Test
  public void testCodec() {

    final int TINYINT_COL_MIN = -128;
    final int TINYINT_COL_MAX = 127;
    final int TINYINT_COL_UNSIGN_MIN = 0;
    final int TINYINT_COL_UNSIGN_MAX = 255;

    short SMALLINT_COL_MIN = -32768;
    short SMALLINT_COL_MAX = 32767;
    final int SMALLINT_COL_UNSIGN_MIN = 0;
    final int SMALLINT_COL_UNSIGN_MAX = 65535;

    int INT_COL_MIN = -2147483648;
    int INT_COL_MAX = 2147483647;
    int INT_COL_UNSIGN_MIN = 0;
    long INT_COL_UNSIGN_MAX = 4294967295L;

    long BIGINT_COL_MIN = Long.MIN_VALUE;
    long BIGINT_COL_MAX = Long.MAX_VALUE;
    long BIGINT_COL_UNSIGN_MIN = 0;
    BigInteger BIGINT_COL_UNSIGN_MAX = new BigInteger("2").pow(64).subtract(new BigInteger("1"));

    LinkedHashMap<String, String> map = new LinkedHashMap();
    map.put("tinyInt_col_min", String.valueOf(TINYINT_COL_MIN));
    map.put("tinyInt_col_max", String.valueOf(TINYINT_COL_MAX));

    map.put("tinyInt_col_unsign_min", String.valueOf(TINYINT_COL_UNSIGN_MIN));
    map.put("tinyInt_col_unsign_max", String.valueOf(TINYINT_COL_UNSIGN_MAX));

    map.put("smallInt_col_min", String.valueOf(SMALLINT_COL_MIN));
    map.put("smallInt_col_max", String.valueOf(SMALLINT_COL_MAX));

    map.put("smallInt_col_unsign_min", String.valueOf(SMALLINT_COL_UNSIGN_MIN));
    map.put("smallInt_col_unsign_max", String.valueOf(SMALLINT_COL_UNSIGN_MAX));

    map.put("int_col_min", String.valueOf(INT_COL_MIN));
    map.put("int_col_max", String.valueOf(INT_COL_MAX));

    map.put("int_col_unsign_min", String.valueOf(INT_COL_UNSIGN_MIN));
    map.put("int_col_unsign_max", String.valueOf(INT_COL_UNSIGN_MAX));

    map.put("bigInt_col_min", String.valueOf(BIGINT_COL_MIN));
    map.put("bigInt_col_max", String.valueOf(BIGINT_COL_MAX));

    map.put("bigInt_col_unsign_min", String.valueOf(BIGINT_COL_UNSIGN_MIN));
    map.put("bigInt_col_unsign_max", String.valueOf(BIGINT_COL_UNSIGN_MAX));

    String createTable = "CREATE TABLE `maggie.codec_test` (\n"
            + "  `tinyInt_col_min` TinyInt,\n"
            + "  `tinyInt_col_max` TinyInt,\n"
            + "  `tinyInt_col_unsign_min` TinyInt unsigned,\n"
            + "  `tinyInt_col_unsign_max` TinyInt unsigned,\n"
            + "  `smallInt_col_min` SmallInt,\n"
            + "  `smallInt_col_max` SmallInt,\n"
            + "  `smallInt_col_unsign_min` SmallInt unsigned,\n"
            + "  `smallInt_col_unsign_max` SmallInt unsigned,\n"
            + "  `int_col_min` Int,\n"
            + "  `int_col_max` Int,\n"
            + "  `int_col_unsign_min` Int unsigned,\n"
            + "  `int_col_unsign_max` Int unsigned,\n"
            + "  `bigInt_col_min` BigInt,\n"
            + "  `bigInt_col_max` BigInt,\n"
            + "  `bigInt_col_unsign_min` BigInt unsigned,\n"
            + "  `bigInt_col_unsign_max` BigInt unsigned,\n"
            + "  PRIMARY KEY (`%s`)\n"
            + ") COMMENT 'REPLICA=1' ENGINE=MEMORY AUTO_INCREMENT=0";

    String baseInsertSql = "INSERT INTO maggie.codec_test(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            + " VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);";

    String baseSelectSql = "select %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s from maggie.codec_test";

    List<String> insertSqlFormatList = new ArrayList<>();
    List<String> selectSqlFormatList = new ArrayList<>();

    List<String> values = new ArrayList<>();

    map.forEach((k, v) -> {
      insertSqlFormatList.add(k);
      selectSqlFormatList.add(k);
    });
    StringBuffer sb = new StringBuffer();
    map.forEach((k, v) -> {
      insertSqlFormatList.add(v);
      sb.append(k).append("=").append(v).append("; ");
    });
    sb.deleteCharAt(sb.length() - 1);
    sb.deleteCharAt(sb.length() - 1);
    values.add(TestUtil.linuxString(sb.toString()));
    String insertSql = String.format(baseInsertSql, insertSqlFormatList.toArray(new String[0]));
    String selectSql = String.format(baseSelectSql, selectSqlFormatList.toArray(new String[0]));

    map.forEach((k, v) -> {
        execUpdate("drop table IF EXISTS maggie.codec_test", 0, true);

        //create table
        String tableSql = String.format(createTable, k);
        System.out.println("PRIMARY KEY : " + k);
        execUpdate(tableSql, 0, true);

        //insert values
        execUpdate(insertSql, 1, true);
        //select check

        execQuery(selectSql, values);
        //drop table
        execUpdate("drop table IF EXISTS maggie.codec_test", 0, true);
    });
  }


}

