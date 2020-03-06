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
import java.sql.Time;
import java.sql.Timestamp;
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

import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

/**
 * @version V1.0
 */
public class SelectIndexTest extends SqlTestBase {
  private static String DBNAME = "test_index";
  private static String INT_TABLENAME = "baker_int_idx";
  private static String FLOAT_TABLENAME = "baker_float_idx";
  private static String DATETIME_TABLENAME = "baker_datetime_idx";
  private static String COMPOSITE_PRIMARY_KEY_TABLENAME = "baker_composite_primary_key";
  private static final int size = 1000;

  private static TableDataResult INT_TABLE_DATARESULT = new TableDataResult(INT_TABLENAME);
  private static TableDataResult FLOAT_TABLE_DATARESULT = new TableDataResult(FLOAT_TABLENAME);
  private static TableDataResult DATETIME_TABLE_DATARESULT = new TableDataResult(DATETIME_TABLENAME);
  private static TableDataResult COMPOSITE_PRIMARY_KEY_TABLE_DATARESULT = new TableDataResult(COMPOSITE_PRIMARY_KEY_TABLENAME);


//  @BeforeClass
  public static void init() {
    createDB();
    initIntTable();
    initFloatTable();
    initDatetimeTable();
    initCompositePrimaryKeyTable();
    initINT_TABLE_Data();
    initFloat_Table_Data();
    initDatetime_Table_Data();
    initCompositePrimaryKey_Table_Data();
  }

  private static void createDB() {
    createCatalog(DBNAME);
    useCatalog(DBNAME);
  }

  private static void initIntTable() {
    String sql = "CREATE TABLE IF NOT EXISTS `" + INT_TABLENAME + "` ( "
            + "`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT, "
            + "`i1` int(11) NOT NULL, "
            + "`i2` int(11) NOT NULL, "
            + "`i3` int(11) NOT NULL, "
            + "`b1` bigint NOT NULL, "
            + "`b2` bigint DEFAULT NULL, "
            + "`b3` bigint NOT NULL, "
            + "`v1` varchar(100) NOT NULL, "
            + "`v2` varchar(100) NOT NULL, "
            + "`v3` varchar(100) NOT NULL, "
            + "`ti1` tinyint NOT NULL, "
            + "`ti2` tinyint NOT NULL, "
            + "`ti3` tinyint NOT NULL, "
            + "`si1` smallint NOT NULL, "
            + "`si2` smallint NOT NULL, "
            + "`si3` smallint NOT NULL, "
            + "`mi1` mediumint NOT NULL, "
            + "`mi2` mediumint NOT NULL, "
            + "`mi3` mediumint NOT NULL, "
            + "PRIMARY KEY (`id`), "
            + "UNIQUE INDEX int_idx (i1,i2,i3), "
            + "UNIQUE INDEX bigint_idx (b1,b2,b3), "
            + "UNIQUE INDEX varchar_idx (v1,v2,v3), "
            + "UNIQUE INDEX smallint_idx (si1,si2,si3), "
            + "UNIQUE INDEX mediumint_idx (mi1,mi2,mi3), "
            + "UNIQUE INDEX tinyint_idx (ti1,ti2,ti3), "
            + "INDEX ibv_normal_idx (i1,b1,v1), "
            + "UNIQUE INDEX ibv_composite_uniq_idx (i3,b3,v3), "
            + "INDEX mst_normal_idx (mi1,si1,ti1), "
            + "UNIQUE INDEX mst_composite_uniq_idx (mi3,si3,ti3) "
            + ") COMMENT 'REPLICA=1' ENGINE=MEMORY AUTO_INCREMENT=0 ";
    dropAndCreateTable(INT_TABLENAME, sql);
  }

  private static void initCompositePrimaryKeyTable() {
    String sql = "CREATE TABLE IF NOT EXISTS `" + COMPOSITE_PRIMARY_KEY_TABLENAME + "` ( "
            + "`t_int` int(11) NOT NULL, "
            + "`t_varchar` varchar(100) NOT NULL, "
            + "`t_decimal` decimal(10,2) NOT NULL,"
            + "`i1` int(11) NOT NULL DEFAULT 0,"
            + "`v1` varchar(10) NOT NULL DEFAULT 0,"
            + "PRIMARY KEY (`t_int`,`t_varchar`,`t_decimal`),"
            + "INDEX t_idx (i1) "
            + ") COMMENT 'REPLICA=1' ENGINE=MEMORY ";
    dropAndCreateTable(COMPOSITE_PRIMARY_KEY_TABLENAME, sql);
  }

  private static void initCompositePrimaryKey_Table_Data() {
    int idx = 0;
    for (int batch = 0; batch < size/10; batch++) {
      int onesize = 10;

      for (int n = 0; n < onesize; n++) {
        String sql = "INSERT INTO "+ COMPOSITE_PRIMARY_KEY_TABLENAME
                + " (t_int,t_varchar,t_decimal,i1,v1) VALUES";
        sql += "("
                + (idx + 2000)
                + ", 't_varchar-" + idx + "'"
                + "," + reserveBitTwo(idx * 100000 * 0.01)
                + "," + idx
                + ", 'abc-" + batch + "'" + ")";
        idx++;
        execUpdate(sql, 1, true);
      }
    }
    System.out.println(COMPOSITE_PRIMARY_KEY_TABLENAME + " table initData is complete.");
  }

  private static void initFloatTable() {
    String sql = "CREATE TABLE IF NOT EXISTS `" + FLOAT_TABLENAME + "` ( "
            + "`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT, "
            + "`d1` double NOT NULL, "
            + "`d2` double NOT NULL, "
            + "`d3` double NOT NULL, "
            + "`f1` float NOT NULL, "
            + "`f2` float NOT NULL, "
            + "`f3` float NOT NULL, "
            + "`decimal1` decimal(10,2) NOT NULL, "
            + "`decimal2` decimal(10,2) NOT NULL, "
            + "`decimal3` decimal(10,2) NOT NULL, "
            + "PRIMARY KEY (`id`), "
            + "UNIQUE INDEX double_idx (d1,d2,d3), "
            + "UNIQUE INDEX float_idx (f1,f2,f3), "
            + "UNIQUE INDEX decimal_idx (decimal1,decimal2,decimal3), "
            + "INDEX dfd_normal_idx (d1,f1,decimal1), "
            + "UNIQUE INDEX dfd_composite_uniq_idx (d3,f3,decimal3) "
            + ") COMMENT 'REPLICA=1' ENGINE=MEMORY AUTO_INCREMENT=0 ";
    dropAndCreateTable(FLOAT_TABLENAME, sql);
  }

  private static void initDatetimeTable() {
    String sql = "CREATE TABLE IF NOT EXISTS `" + DATETIME_TABLENAME + "` ( "
            + "`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT, "
            + "`date1` date NOT NULL, "
            + "`date2` date NOT NULL, "
            + "`time1` time NOT NULL, "
            + "`time2` time NOT NULL, "
            + "`datetime1` dateTime NOT NULL, "
            + "`datetime2` dateTime NOT NULL, "
            + "`timestamp1` timeStamp NOT NULL, "
            + "`timestamp2` timeStamp NOT NULL, "
            + "`year1` year NOT NULL, "
            + "`year2` year NOT NULL, "
            + "PRIMARY KEY (`id`), "
            + "UNIQUE INDEX date_idx (date1,date2), "
            + "UNIQUE INDEX time_idx (time1,time2), "
            + "UNIQUE INDEX datetime_idx (datetime1,datetime2), "
            + "UNIQUE INDEX timestamp_idx (timestamp1,timestamp2), "
            + "UNIQUE INDEX year_idx (year1,year2), "
            + "INDEX date_normal_idx (date1,datetime1,year1), "
            + "UNIQUE INDEX date_composite_uniq_idx (date2,datetime2,year2), "
            + "INDEX time_normal_idx (time1,timestamp1), "
            + "UNIQUE INDEX time_composite_uniq_idx (time2,timestamp2) "
            + ") COMMENT 'REPLICA=1' ENGINE=MEMORY AUTO_INCREMENT=0 ";
    dropAndCreateTable(DATETIME_TABLENAME, sql);
  }

  private static void initINT_TABLE_Data() {

    int idx = 0;
    int tinyint = -127;
    int smallint = 0;
    Random r = new Random();
    for (int batch = 0; batch < size/10; batch++) {
      int onesize = 10;

      for (int n = 0; n < onesize; n++) {
        String sql = "INSERT INTO "+ INT_TABLENAME
                + " (i1,i2,i3,b1,b2,b3,"
                + "v1,v2,v3,ti1,ti2,ti3,"
                + "si1,si2,si3,mi1,mi2,mi3) VALUES";
        sql += "(" + batch + "," + (n + 2000) + "," + (idx + 3000) + "," + (batch + 10000) + "," + (n + 12000) + "," + (idx + 13000)
                + ",'v1-" + batch + "','v2-" + n + "','v3-" + idx + "'," + tinyint + "," + (r.nextInt(128)) + "," + (r.nextInt(128))
                + "," + smallint + "," + (r.nextInt(32768)) + "," + smallint + "," + batch + "," + (n * 2) + "," + (idx + 10000) + ")";
        idx++;
        tinyint++;
        smallint++;
        if (tinyint >= 125) {
          tinyint = -127;
        }
        if (smallint >= 32765) {
          smallint = -32767;
        }
        execUpdate(sql, 1, true);
      }
    }
    System.out.println(INT_TABLENAME + " table initData is complete.");
  }

  private static void initFloat_Table_Data() {
    int idx = 0;
    for (int batch = 0; batch < size/10; batch++) {
      int onesize = 10;

      for (int n = 0; n < onesize; n++) {
        String sql = "INSERT INTO "+ FLOAT_TABLENAME
                + " (d1,d2,d3,"
                + "f1,f2,f3,"
                + "decimal1,decimal2,decimal3) VALUES";
        sql += "(" + (batch + 1.5) + "," + (n * 3000 + 1.1) + "," + (idx * 5000 + 1.1)
                + "," + (batch + 1.5) + "," + (n * 3000 + 1.1) + "," + (idx * 5000 + 1.1)
                + "," + reserveBitTwo(batch * 10 + 1.01) + "," + reserveBitTwo(n * 300 + 1.01) + "," + reserveBitTwo(idx * 500 + 1.01)+ ")";
        idx++;
        execUpdate(sql, 1, true);
      }
    }
    System.out.println(FLOAT_TABLENAME + " table initData is complete.");
  }

  private static void initDatetime_Table_Data() {
    try {
      int idx = 1;
      Random r = new Random();
      Set<String> yearSet = new HashSet<>();
      DateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
      DateFormat timeFormat = new SimpleDateFormat("HH:mm:ss");
      int year = 1990;
      for (int batch = 0; batch < size / 10; batch++) {
        int onesize = 10;
        Date base = dateFormat.parse(year++ + "-01-01");
        for (int n = 0; n < onesize; n++) {
          Date date = new Date(base.getTime() + (idx * 24 * 3600 * 1000));
          Date date1 = new Date(base.getTime() + (idx * 48 * 3600 * 1000));
          Date time = new Date(base.getTime() + idx * (20 * 1000));
          Date time1 = new Date(base.getTime() + idx * (60 * 1000));
          int year1 = idx % 200 + 1905;
          int year2 = r.nextInt(200) + 1905;
          while (yearSet.contains(year1 + "" + year2)) {
            year2 = r.nextInt(200) + 1905;
          }
          yearSet.add(year1 + "" + year2);
          String sql = "INSERT INTO " + DATETIME_TABLENAME
                  + " (date1,date2,datetime1,datetime2,"
                  + "time1,time2,timestamp1,timestamp2,"
                  + "year1,year2) VALUES";
          sql += "('" + dateFormat.format(date) + "','" + dateFormat.format(date1) + "','" + dateTimeFormat.format(date) + "','" + dateTimeFormat.format(date1) + "','"
                  + timeFormat.format(time) + "','" + timeFormat.format(time1) + "','" + dateTimeFormat.format(date) + "','" + dateTimeFormat.format(date1) + "',"
                  + year1 + "," + year2 + ")";
          idx++;
          execUpdate(sql, 1, true);
        }
      }
      System.out.println(DATETIME_TABLENAME + " table initData is complete.");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static double reserveBitTwo(double d) {
    return new BigDecimal(d).setScale(2, BigDecimal.ROUND_FLOOR).doubleValue();
  }

  @Before
  public void fetchData() {
    useCatalog(DBNAME);

    if (INT_TABLE_DATARESULT.data_result.isEmpty()) {
      selectAllfromTablebaker(INT_TABLE_DATARESULT);
    }
    if (FLOAT_TABLE_DATARESULT.data_result.isEmpty()) {
      selectAllfromTablebaker(FLOAT_TABLE_DATARESULT);
    }
    if (DATETIME_TABLE_DATARESULT.data_result.isEmpty()) {
      selectAllfromTablebaker(DATETIME_TABLE_DATARESULT);
    }
    if (COMPOSITE_PRIMARY_KEY_TABLE_DATARESULT.data_result.isEmpty()) {
      selectAllfromTablebaker(COMPOSITE_PRIMARY_KEY_TABLE_DATARESULT);
    }
  }

  @Test
  public void testSelectDateGroupbyId() {
    execQuery("select  id, date1, datetime1, time1, timestamp1 from " + DATETIME_TABLENAME + "  group by id limit 10");
  }

  @Test
  public void testIndex4Int01() {
    SelectCols select = new SelectCols(INT_TABLE_DATARESULT, new String[] {"id", "i1", "i2", "i3" } );
    List<String> expected = this.filter(select,
            row -> row.get("i1").compareTo(1) == 0
                    && row.get("i2").compareTo(2005) == 1
                    && row.get("i2").compareTo(2007) == -1);
    
    execQueryCheckEmpty("select " + select.select + " from " + select.tableName + " where i1 = 1 and i2 > 2005 and i2 < 2007 ", expected);
  }

  @Test
  public void testIndex4Int02() {
    SelectCols select = new SelectCols(INT_TABLE_DATARESULT, new String[] {"id", "i1", "i2", "i3" } );
    List<String> expected = this.filter(select,
            row -> row.get("i1").compareTo(1) == 0
                    && row.get("i2").compareTo(2005) == 0
                    && row.get("i3").compareTo(3015) == 0);
    
    execQueryCheckEmpty("select "+select.select+" from " + select.tableName + " where i1 = 1 and i2 = 2005 and i3 = 3015 ", expected);
  }
  @Test
  public void testIndex4Int03() {
    SelectCols select = new SelectCols(INT_TABLE_DATARESULT, new String[] { "i1", "i2", "i3" } );
    List<String> expected = this.filter(select,
            row -> row.get("i1").compareTo(1) == 0
                    && row.get("i2").compareTo(2005) == 1
                    && row.get("i2").compareTo(2007) == -1);
    
    execPrepareQueryCheckEmpty("select " + select.select + " from " + select.tableName + " where i1 = ? and i2 > ? and i2 < ? ", expected,1,2005,2007);
  }

  @Test
  public void testIndex4Int04() {
    SelectCols select = new SelectCols(INT_TABLE_DATARESULT, new String[] { "i1", "i2", "i3" } );
    List<String> expected = this.filter(select,
            row -> row.get("i1").compareTo(3) == 0
                    && row.get("i2").compareTo(2000) == 0
                    && row.get("i3").compareTo(3030) == 0);
    
    execPrepareQueryCheckEmpty("select "+select.select+" from " + select.tableName + " where i1 = ? and i2 = ? and i3 = ? ", expected,3,2000,3030);
  }

  @Test
  public void testIndex4Int05() {
    SelectCols select = new SelectCols(INT_TABLE_DATARESULT, new String[] { "i1", "i2", "i3" } );
    List<String> expected = this.filter(select,
            row -> row.get("i1").compareTo(3) == 0);
    
    execQueryCheckEmpty("select "+select.select+" from " + select.tableName + " where i1 = 3", expected);
  }

  @Test
  public void testIndex4BigInt01() {
    SelectCols select = new SelectCols(INT_TABLE_DATARESULT, new String[] {"id", "b1", "b2", "b3" } );
    List<String> expected = this.filter(select,
            row -> row.get("b1").compareTo(10000L) == 0
                    && row.get("b2").compareTo(12006L) == 1
                    && row.get("b2").compareTo(12009L) == -1);
    execQueryCheckEmpty("select " +select.select+ " from " + select.tableName + " where b1 = 10000 and b2 > 12006 and b2 < 12009 ", expected);
  }

  @Test
  public void testIndex4BigInt02() {
    SelectCols select = new SelectCols(INT_TABLE_DATARESULT, new String[] {"id", "b1", "b2", "b3" } );
    List<String> expected = this.filter(select,
            row -> row.get("b1").compareTo(10000L) == 0
                    && row.get("b2").compareTo(12006L) == 0
                    && row.get("b3").compareTo(13006L) == 0);
    execQueryCheckEmpty("select " + select.select + " from " + select.tableName + " where b1 = 10000 and b2 = 12006 and b3 = 13006", expected);
  }

  @Test
  public void testIndex4BigInt03() {
    SelectCols select = new SelectCols(INT_TABLE_DATARESULT, new String[] { "b1", "b2", "b3" } );
    List<String> expected = this.filter(select,
            row -> row.get("b1").compareTo(10000L) == 0
                    && row.get("b2").compareTo(12006L) == 1
                    && row.get("b2").compareTo(12009L) == -1);
    
    execPrepareQueryCheckEmpty("select " +select.select+ " from " + select.tableName + " where b1 = ? and b2 > ?  and b2 < ? ", expected,10000,12006 , 12009);
  }


  @Test
  public void testIndex4BigInt04() {
    SelectCols select = new SelectCols(INT_TABLE_DATARESULT, new String[] { "b1", "b2", "b3" } );
    List<String> expected = this.filter(select,
            row -> row.get("b1").compareTo(10000L) == 0
                    && row.get("b2").compareTo(12006L) == 0
                    && row.get("b3").compareTo(13006L) == 0);
    
    execPrepareQueryCheckEmpty("select " + select.select + " from " + select.tableName + " where b1 = ? and b2 = ? and b3 = ?", expected, 10000, 12006, 13006);
  }

  @Test
  public void testIndex4Varchar01() {
    SelectCols select = new SelectCols(INT_TABLE_DATARESULT, new String[] {"id", "v1", "v2", "v3" } );
    List<String> expected = this.filter(select,
            row -> row.get("v1").compareTo("v1-9") == 0
                    && row.get("v2").compareTo("v2-9") < 0
                    && row.get("v2").compareTo("v2-6") > 0);
    execQueryCheckEmpty("select " + select.select + " from " + select.tableName + " where v1 = 'v1-9' and v2 < 'v2-9' and v2 > 'v2-6' ", expected);
  }

  @Test
  public void testIndex4Varchar02() {
    SelectCols select = new SelectCols(INT_TABLE_DATARESULT, new String[] {"id", "v1", "v2", "v3" } );
    List<String> expected = this.filter(select,
            row -> row.get("v1").compareTo("v1-9") == 0
                    && row.get("v2").compareTo("v2-9") == 0
                    && row.get("v3").compareTo("v3-99") == 0);
    execQueryCheckEmpty("select " + select.select + " from " + select.tableName + " where v1 = 'v1-9' and v2 = 'v2-9' and v3 = 'v3-99' ", expected);
  }


  @Test
  public void testIndex4Varchar03() {
    SelectCols select = new SelectCols(INT_TABLE_DATARESULT, new String[] { "v1", "v2", "v3" } );
    List<String> expected = this.filter(select,
            row -> row.get("v1").compareTo("v1-9") == 0
                    && row.get("v2").compareTo("v2-9") < 0
                    && row.get("v2").compareTo("v2-6") > 0);
    
    execPrepareQueryCheckEmpty("select " + select.select + " from " + select.tableName + " where v1 = ? and v2 < ? and v2 > ? ", expected, "v1-9","v2-9","v2-6");
  }


  @Test
  public void testIndex4Data00() {
    for (Map<String, Comparable> record : INT_TABLE_DATARESULT.data_result) {
      System.out.println(record.get("ti1") + ",      " + record.get("ti2") + ",      " + record.get("ti3"));
    }
  }



  @Test
  public void testIndex4TinyInt01() {
    SelectCols select = new SelectCols(INT_TABLE_DATARESULT, new String[] {"id", "ti1", "ti2", "ti3" } );
    List<String> expected = this.filter(select,
            row -> row.get("ti1").compareTo(8) == 0);
    execQueryCheckEmpty("select " +select.select+ " from " + select.tableName + " where ti1 = 8  ", expected);
  }

  @Test
  public void testIndex4TinyInt02() {
    SelectCols select = new SelectCols(INT_TABLE_DATARESULT, new String[] {"id", "ti1", "ti2", "ti3" } );
    List<String> expected = this.filter(select,
            row -> row.get("ti1").compareTo(8) == 0
                    && row.get("ti2").compareTo(0) > 0
                    && row.get("ti2").compareTo(100) < 0);
    execQueryCheckEmpty("select " +select.select+ " from " + select.tableName + " where ti1 = 8 and ti2 > 0 and ti2 < 100 ", expected);
  }

  @Test
  public void testIndex4TinyInt03() {
    SelectCols select = new SelectCols(INT_TABLE_DATARESULT, new String[] {"id", "ti1", "ti2", "ti3" } );
    Map<String, Comparable> row = INT_TABLE_DATARESULT.data_result.get(0);
    List<String> expected = new ArrayList<>();
    expected.add(getResultString(select.colNames, row));
    execPrepareQueryCheckEmpty("select " + select.select + " from " + select.tableName + " where ti1 = ? and ti2 = ? "
            + "and ti3 = ?", expected, row.get("ti1"), row.get("ti2"), row.get("ti3"));
  }

  @Test
  public void testIndex4SmallInt01() {
    SelectCols select = new SelectCols(INT_TABLE_DATARESULT, new String[] {"id", "si1", "si2", "si3" } );
    List<String> expected = this.filter(select,
            row -> row.get("si1").compareTo(10) == 0);
    execQueryCheckEmpty("select " +select.select+ " from " + select.tableName + " where si1 = 10  ", expected);
  }

  @Test
  public void testIndex4SmallInt02() {
    SelectCols select = new SelectCols(INT_TABLE_DATARESULT, new String[] {"id", "si1", "si2", "si3" } );
    List<String> expected = this.filter(select,
            row -> row.get("si1").compareTo(10) == 0
                    && row.get("si2").compareTo(1) > 0
                    && row.get("si2").compareTo(30000) < 0);
    execQueryCheckEmpty("select " +select.select+ " from " + select.tableName + " where si1 = 10 and si2 > 1 "
            + "and si2 < 30000 ", expected);
  }

  @Test
  public void testIndex4SmallInt03() {
    SelectCols select = new SelectCols(INT_TABLE_DATARESULT, new String[] {"id", "si1", "si2", "si3" } );
    Map<String, Comparable> row = INT_TABLE_DATARESULT.data_result.get(0);
    List<String> expected = new ArrayList<>();
    expected.add(getResultString(select.colNames, row));
    execPrepareQueryCheckEmpty("select " + select.select + " from " + select.tableName + " where si1 = ? and si2 = ? "
            + "and si3 = ?", expected, row.get("si1"), row.get("si2"), row.get("si3"));
  }

  @Test
  public void testIndex4MediumInt01() {
    SelectCols select = new SelectCols(INT_TABLE_DATARESULT, new String[] {"id", "mi1", "mi2", "mi3" } );
    List<String> expected = this.filter(select,
            row -> row.get("mi1").compareTo(10) == 0
                    && row.get("mi2").compareTo(2) == -1);
    execQueryCheckEmpty("select "+select.select+" from " + select.tableName + " where mi1 = 10 and mi2 < 2 ", expected);
  }

  @Test
  public void testIndex4MediumInt02() {
    SelectCols select = new SelectCols(INT_TABLE_DATARESULT, new String[] {"id", "mi1", "mi2", "mi3" } );
    List<String> expected = this.filter(select,
            row -> row.get("mi1").compareTo(10) == 0
                    && row.get("mi2").compareTo(0) == 0
                    && row.get("mi3").compareTo(10100) == 0);
    execQueryCheckEmpty("select "+select.select+" from " + select.tableName + " where mi1 = 10 and mi2 = 0 and mi3 = 10100", expected);
  }

  @Test
  public void testNonUniqIndex4_IBV01() {
    SelectCols select = new SelectCols(INT_TABLE_DATARESULT, new String[] {"id", "i1", "b1", "v1", "mi1" } );
    List<String> expected = this.filter(select,
            row -> row.get("i1").equals(10)
                    && row.get("b1").compareTo(10000L) > 0);
    execQueryCheckEmpty("select " + select.select + " from " + select.tableName + " where i1 = 10 and b1 > 10000", expected);
  }

  @Test
  public void testNonUniqIndex4_IBV02() {
    SelectCols select = new SelectCols(INT_TABLE_DATARESULT, new String[] {"id", "i1", "b1", "v1" } );
    List<String> expected = this.filter(select,
            row -> row.get("i1").compareTo(1) == 0
                    && row.get("b1").compareTo(10001L) == 0
                    && row.get("v1").compareTo("v1-1") == 0);
    execQueryCheckEmpty("select " + select.select + " from " + select.tableName + " where i1 = 1 and b1 = 10001 and v1 = 'v1-1'", expected);
  }

  @Test
  public void testNonUniqIndex4_IBV_UNIQ() {
    SelectCols select = new SelectCols(INT_TABLE_DATARESULT, new String[] {"id", "i3", "b3", "v3" } );
    List<String> expected = this.filter(select,
            row -> row.get("i3").compareTo(3010) == 0
                    && row.get("b3").compareTo(13010L) == 0
                    && row.get("v3").compareTo("v3-10") == 0);
    execQueryCheckEmpty("select " + select.select + " from " + select.tableName + " where i3 = 3010 and b3 = 13010 and v3 = 'v3-10'", expected);
  }

  @Test
  public void testNonUniqIndex4_MST01() {
    SelectCols select = new SelectCols(INT_TABLE_DATARESULT, new String[] {"id", "mi1", "si1", "ti1" } );
    List<String> expected = this.filter(select,
            row -> row.get("mi1").compareTo(1) == 0
                    && row.get("si1").compareTo(16) == 1);
    execQueryCheckEmpty("select " + select.select + " from " + select.tableName + " where mi1 = 1 and si1 > 16", expected);
  }

  @Test
  public void testNonUniqIndex4_MST02() {
    SelectCols select = new SelectCols(INT_TABLE_DATARESULT, new String[] {"id", "mi1", "si1", "ti1" } );
    List<String> expected = this.filter(select,
            row -> row.get("mi1").compareTo(1) == 0
                    && row.get("si1").compareTo(16) == 0
                    && row.get("ti1").compareTo(-111) == 0);
    execQueryCheckEmpty("select " + select.select + " from " + select.tableName
            + " where mi1 = 1 and si1 = 16 and ti1 = -111 ", expected);
  }

  @Test
  public void testNonUniqIndex4_MST_UNIQ() {
    SelectCols select = new SelectCols(INT_TABLE_DATARESULT, new String[] {"id", "mi3", "si3", "ti3" } );
    Map<String, Comparable> row = INT_TABLE_DATARESULT.data_result.get(0);
    List<String> expected = new ArrayList<>();
    expected.add(getResultString(select.colNames, row));
    execPrepareQueryCheckEmpty("select " + select.select + " from " + select.tableName + " where mi3 = ? and si3 = ? and ti3 = ?"
            , expected, row.get("mi3"), row.get("si3"), row.get("ti3"));
  }

  @Test
  public void testIndex4Double01() {
    SelectCols select = new SelectCols(FLOAT_TABLE_DATARESULT, new String[] {"d1", "d2", "d3" } );
    List<String> expected = this.filter(select,
            row -> row.get("d1").compareTo(13.5) == 0
                    && row.get("d2").compareTo(15000.0) > 0
                    && row.get("d2").compareTo(24000.0) < 0);
    execQueryCheckEmpty("select " + select.select + " from " + select.tableName + " where d1 = 13.5 and d2 > 15000.0 and d2 < 24000.0 ", expected);
  }

  @Test
  public void testIndex4Double02() {
    SelectCols select = new SelectCols(FLOAT_TABLE_DATARESULT, new String[] {"id", "d1", "d2", "d3" } );
    List<String> expected1 = this.filter(select,
            row -> row.get("d1").compareTo(13.5) == 0);
    List<String> expected = this.filter(select,
            row -> row.get("d1").compareTo(13.5) == 0
                    && row.get("d2").compareTo(15001.1) == 0
                    && row.get("d3").compareTo(625001.1) == 0);
    execQueryCheckEmpty("select " + select.select + " from " + select.tableName + " where d1 = 13.5 and d2 = 15001.1 and d3 = 625001.1 ", expected);
  }


  @Test
  public void testIndex4Double03() {
    SelectCols select = new SelectCols(FLOAT_TABLE_DATARESULT, new String[] { "d1", "d2", "d3" } );
    List<String> expected = this.filter(select,
            row -> row.get("d1").compareTo(13.5) == 0
                    && row.get("d2").compareTo(15001.1) == 0
                    && row.get("d3").compareTo(625001.1) == 0);
    
    execPrepareQueryCheckEmpty("select " + select.select + " from " + select.tableName + " where d1 = ? and d2 = ? and d3 = ? ",
            expected,13.5, 15001.1, 625001.1);
  }

  @Test
  public void testIndex4Float01() {
    SelectCols select = new SelectCols(FLOAT_TABLE_DATARESULT, new String[] {"id", "f1", "f2", "f3" } );
    List<String> expected = this.filter(select,
            row -> row.get("f1").compareTo(1.5f) == 0
                    && row.get("f2").compareTo(15000.0f) > 0
                    && row.get("f2").compareTo(16000.0f) < 0);
    execQueryCheckEmpty("select " + select.select + " from " + select.tableName + " where f1 = 13.5 and f2 > 15000.0 and f2 < 16000.0 ", expected);
  }

  @Test
  public void testIndex4Float02() {
    SelectCols select = new SelectCols(FLOAT_TABLE_DATARESULT, new String[] {"id", "f1", "f2", "f3" } );
    List<String> expected = this.filter(select,
            row -> row.get("f1").compareTo(10.5f) == 0
                    && row.get("f2").compareTo(15001.1f) == 0
                    && row.get("f3").compareTo(475001.1f) == 0);
    execQueryCheckEmpty("select " + select.select + " from " + select.tableName + " where f1 = 10.5 and f2 = 15001.1 and f3 = 475001.1 ", expected);
  }

  @Test
  public void testIndex4Decimal01() {
    SelectCols select = new SelectCols(FLOAT_TABLE_DATARESULT, new String[] {"id", "decimal1", "decimal2", "decimal3" } );
    List<String> expected = this.filter(select,
            row -> row.get("decimal1").compareTo(new BigDecimal("121.01")) == 0
                    && row.get("decimal2").compareTo(new BigDecimal("300.00")) == -1);
    execPrepareQueryCheckEmpty("select " + select.select + " from " + select.tableName
            + " where decimal1 = ? and decimal1 < ? ", expected, 121.01, 300.00);
  }

  @Test
  public void testIndex4Decimal02() {
    SelectCols select = new SelectCols(FLOAT_TABLE_DATARESULT, new String[] {"id", "decimal1", "decimal2", "decimal3" } );
    List<String> expected = this.filter(select,
            row -> row.get("decimal1").compareTo(new BigDecimal("121.01")) == 0
                    && row.get("decimal2").compareTo(new BigDecimal("1501.00")) == 0
                    && row.get("decimal3").compareTo(new BigDecimal("62501.01")) == 0);
    execQueryCheckEmpty("select " + select.select + " from " + select.tableName
            + " where decimal1 = 121.01 and decimal2 = 1501.00 and decimal3 = 62501.01 ", expected);
  }



  @Test
  public void testIndex4DFD01() {
    SelectCols select = new SelectCols(FLOAT_TABLE_DATARESULT, new String[] {"id", "d1", "f1", "decimal1" } );
    List<String> expected = this.filter(select,
            row -> row.get("d1").compareTo(13.5d) == 0
                    && row.get("f1").compareTo(14.5f) == -1);
    execQueryCheckEmpty("select " + select.select + " from " + select.tableName + " where d1 = 13.5 and f1 < 14.5  ", expected);
  }

  @Test
  public void testIndex4DFD02() {
    SelectCols select = new SelectCols(FLOAT_TABLE_DATARESULT, new String[] {"id", "d1", "f1", "decimal1" } );
    List<String> expected = this.filter(select,
            row -> row.get("d1").compareTo(13.5d) == 0
                    && row.get("f1").compareTo(13.5f) == 0
                    && row.get("decimal1").compareTo(new BigDecimal("121.01")) == 0);
    execQueryCheckEmpty("select " + select.select + " from " + select.tableName + " where d1 = 13.5 and f1 = 13.5 and decimal1 = 121.01 ", expected);
  }

  @Test
  public void testIndex4DFD_UNIQ() {
    SelectCols select = new SelectCols(FLOAT_TABLE_DATARESULT, new String[] {"id", "d3", "f3", "decimal3" } );
    List<String> expected = this.filter(select,
            row -> row.get("d3").compareTo(110001.1d) == 0
                    && row.get("f3").compareTo(110001.1f) == 0
                    && row.get("decimal3").compareTo(new BigDecimal("11001.01")) == 0);
    execQueryCheckEmpty("select " + select.select + " from " + select.tableName
            + " where d3 = 110001.1 and f3 = 110001.1 and decimal3 = 11001.01 ", expected);
  }

  @Test
  public void testIndex4Date01() throws ParseException {
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    Date date = dateFormat.parse("1990-01-02");

    SelectCols select = new SelectCols(DATETIME_TABLE_DATARESULT, new String[] {"id", "date1", "date2" } );
    List<String> expected = this.filter(select,
            row -> row.get("date1").compareTo(date) == 0
                    && row.get("date2").compareTo(date) == 1);
    execQueryCheckEmpty("select " + select.select + " from " + select.tableName + " where date1 = '1990-01-02' and date2 > '1990-01-02' ", expected);
  }

  @Test
  public void testIndex4Date02() throws ParseException {
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    Date date1 = dateFormat.parse("1990-01-02");
    Date date2 = dateFormat.parse("1990-01-03");

    SelectCols select = new SelectCols(DATETIME_TABLE_DATARESULT, new String[] {"id", "date1", "date2" } );
    List<String> expected = this.filter(select,
            row -> row.get("date1").compareTo(date1) == 0
                    && row.get("date2").compareTo(date2) == 0);
    execQueryCheckEmpty("select " + select.select + " from " + select.tableName + " where date1 = '1990-01-02' and date2 = '1990-01-03' ", expected);
  }

  @Test
  public void testIndex4Time01() throws ParseException {
    SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss");
    Date date = format.parse("01:12:20");
    Time time1 = new Time(date.getTime());  //01:12:20

    SelectCols select = new SelectCols(DATETIME_TABLE_DATARESULT, new String[] {"id", "time1", "time2" } );
    List<String> expected = this.filter(select,
            row -> row.get("time1").equals(time1));
    execQueryCheckEmpty("select " + select.select + " from " + select.tableName + " where time1 = '01:12:20' ", expected);
  }

  @Test
  public void testIndex4Time02() throws ParseException {
    SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss");
    Date date1 = format.parse("01:12:20");
    Date date2 = format.parse("03:30:00");
    Time time1 = new Time(date1.getTime());    //01:12:20
    Time time2 = new Time(date2.getTime());    //03:30:00

    SelectCols select = new SelectCols(DATETIME_TABLE_DATARESULT, new String[] {"id", "time1", "time2" } );
    List<String> expected = this.filter(select,
            row -> row.get("time1").equals(time1)
                    && row.get("time2").compareTo(time2) == 1);
    execQueryCheckEmpty("select " + select.select + " from " + select.tableName + " where time1 = '01:12:20' and time2 > '03:30:00' ", expected);
  }

  @Test
  public void testIndex4Time03() throws ParseException {
    SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss");
    Date date1 = format.parse("01:12:20");
    Date date2 = format.parse("03:40:00");
    Time time1 = new Time(date1.getTime());    //01:12:20
    Time time2 = new Time(date2.getTime());    //03:30:00

    SelectCols select = new SelectCols(DATETIME_TABLE_DATARESULT, new String[] {"id", "time1", "time2" } );
    List<String> expected = this.filter(select,
            row -> row.get("time1").equals(time1)
                    && row.get("time2").compareTo(time2) < 0);
    execQueryCheckEmpty("select " + select.select + " from " + select.tableName + " where time1 = '01:12:20' and time2 < '03:40:00' ", expected);
  }

  @Test
  public void testIndex4Time04() throws ParseException {
    SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss");
    Date date1 = format.parse("01:12:20");
    Date date2 = format.parse("03:37:00");
    Time time1 = new Time(date1.getTime());    //01:12:20
    Time time2 = new Time(date2.getTime());    //03:30:00

    SelectCols select = new SelectCols(DATETIME_TABLE_DATARESULT, new String[] {"id", "time1", "time2" } );
    List<String> expected = this.filter(select,
            row -> row.get("time1").compareTo(time1) == 0
                    && row.get("time2").compareTo(time2) == 0);
    execQueryCheckEmpty("select " + select.select + " from " + select.tableName + " where time1 = '01:12:20' and time2 = '03:37:00' ", expected);
  }

  @Test
  public void testIndex4DateTime01() throws ParseException {

    SelectCols select = new SelectCols(DATETIME_TABLE_DATARESULT, new String[] {"id", "datetime1", "datetime2" } );
    DateFormat timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    Date date1 = timeFormat.parse("1991-12-08 06:57:12.0");
    Date date2 = timeFormat.parse("1992-01-03 06:57:00.0");
    Timestamp timestamp1 = new Timestamp(date1.getTime());
    Timestamp timestamp2 = new Timestamp(date2.getTime());

    List<String> expected = this.filter(select,
            row -> row.get("datetime1").compareTo(timestamp1) == 0
                    && row.get("datetime2").compareTo(timestamp2) > 0);
    execQueryCheckEmpty("select " + select.select + " from " + select.tableName + " where datetime1 = '1991-12-08 06:57:12' "
            + "and datetime2 > '1992-01-03 06:57:00' ", expected);
  }

  @Test
  public void testIndex4DateTime02() throws ParseException {
    SelectCols select = new SelectCols(DATETIME_TABLE_DATARESULT, new String[] {"id", "datetime1", "datetime2" } );
    DateFormat timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    Date date1 = timeFormat.parse("1991-12-08 06:57:12.0");
    Date date2 = timeFormat.parse("1992-01-03 06:57:12.0");

    Timestamp timestamp1 = new Timestamp(date1.getTime());
    Timestamp timestamp2 = new Timestamp(date2.getTime());

    List<String> expected = this.filter(select,
            row -> row.get("datetime1").equals(timestamp1)
                    && row.get("datetime2").equals(timestamp2));
    execQueryCheckEmpty("select " + select.select + " from " + select.tableName + " where datetime1 = '1991-12-08 06:57:12.0' and datetime2 = '1992-01-03 06:57:12.0' ", expected);
//    execPrepareQueryCheckEmpty("select " + select.select + " from " + select.tableName + " where datetime1 = ? and datetime2 = ? ",
//            expected, datetime1, datetime2);
  }

  @Test
  public void testIndex4TimeStamp01() throws ParseException {
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    Date timestamp1 = dateFormat.parse("2000-01-02 13:54:25.0");

    SelectCols select = new SelectCols(DATETIME_TABLE_DATARESULT, new String[] {"id", "timestamp1", "timestamp2" } );
    List<String> expected = this.filter(select,
            row -> row.get("timestamp1").compareTo(timestamp1) == 0);
    execQueryCheckEmpty("select " + select.select + " from " + select.tableName +
            " where timestamp1 = '2000-01-02 13:54:25' ", expected);
  }
  @Test
  public void testIndex4TimeStamp02() throws ParseException {
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    Date timestamp1 = dateFormat.parse("2000-01-02 13:54:25.0");
    Date timestamp2 = dateFormat.parse("2000-01-04 00:00:00.0");

    SelectCols select = new SelectCols(DATETIME_TABLE_DATARESULT, new String[] {"id", "timestamp1", "timestamp2" } );
    List<String> expected = this.filter(select,
            row -> row.get("timestamp1").compareTo(timestamp1) == 0
                    && row.get("timestamp2").compareTo(timestamp2) > 0);
    execQueryCheckEmpty("select " + select.select + " from " + select.tableName +
            " where timestamp1 = '2000-01-02 13:54:25' and timestamp2 > '2000-01-04 00:00:00' ", expected);
  }

  @Test
  public void testIndex4TimeStamp03() throws ParseException {
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    Date timestamp1 = dateFormat.parse("2000-01-02 13:54:25.0");
    Date timestamp2 = dateFormat.parse("2000-01-04 03:48:50.0");

    SelectCols select = new SelectCols(DATETIME_TABLE_DATARESULT, new String[] {"id", "timestamp1", "timestamp2" } );
    List<String> expected = this.filter(select,
            row -> row.get("timestamp1").compareTo(timestamp1) == 0
                    && row.get("timestamp2").compareTo(timestamp2) == 0);
    execQueryCheckEmpty("select " + select.select + " from " + select.tableName +
            " where timestamp1 = '2000-01-02 13:54:25' and timestamp2 = '2000-01-04 03:48:50' ", expected);
  }

  @Test
  public void testIndex4Year01() throws ParseException {
    DateFormat dateFormat = new SimpleDateFormat("yyyy");
    Date year1 = dateFormat.parse("1906");
    SelectCols select = new SelectCols(DATETIME_TABLE_DATARESULT, new String[] {"id", "year1", "year2" } );
    List<String> expected = this.filter(select,
            row -> row.get("year1").compareTo(year1) == 0);
    execQueryCheckEmpty("select " + select.select + " from " + select.tableName + " where year1 = 1906  ", expected);
  }

  @Test
  public void testIndex4Year02() throws ParseException {
    DateFormat dateFormat = new SimpleDateFormat("yyyy");
    Date year1 = dateFormat.parse("1906");
    Date year2 = dateFormat.parse("2060");
    SelectCols select = new SelectCols(DATETIME_TABLE_DATARESULT, new String[] {"id", "year1", "year2" } );
    List<String> expected = this.filter(select,
            row -> row.get("year1").compareTo(year1) == 0
                    && row.get("year2").compareTo(year2) == 1);
    execQueryCheckEmpty("select " + select.select + " from " + select.tableName + " where year1 = 1906 and year2 > 2060 ", expected);
  }

  @Test
  public void testIndex4Year03() throws ParseException {
    DateFormat dateFormat = new SimpleDateFormat("yyyy");
    Date year1 = dateFormat.parse("1906");
    Date year2 = dateFormat.parse("2060");
    SelectCols select = new SelectCols(DATETIME_TABLE_DATARESULT, new String[] {"id", "year1", "year2" } );
    List<String> expected = this.filter(select,
            row -> row.get("year1").compareTo(year1) == 0
                    && row.get("year2").compareTo(year2) < 0);
    execQueryCheckEmpty("select " + select.select + " from " + select.tableName + " where year1 = 1906 and year2 < 2060 ", expected);
  }

  @Test
  public void testIndex4Year04() {
    SelectCols select = new SelectCols(DATETIME_TABLE_DATARESULT, new String[] {"id", "year1", "year2" } );
    Map<String, Comparable> row = DATETIME_TABLE_DATARESULT.data_result.get(0);
    List<String> expected = new ArrayList<>();

    int year1 = Integer.parseInt(row.get("year1").toString().substring(0, 4));
    int year2 = Integer.parseInt(row.get("year2").toString().substring(0, 4));

    expected.add(getResultString(select.colNames, row));
    execQueryCheckEmpty("select " + select.select + " from " + select.tableName
            + " where year1 = " + year1 + " and year2 = " + year2, expected);
  }

  @Test
  public void testIndex4_DATE_COMPOSITE01() throws ParseException {
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    DateFormat timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    Date date = dateFormat.parse("1991-12-08");
    Date datetime = timeFormat.parse("1991-12-08 06:57:00.0");

    SelectCols select = new SelectCols(DATETIME_TABLE_DATARESULT, new String[] {"id", "date1", "datetime1" ,"year1" } );
    List<String> expected = this.filter(select,
            row -> row.get("date1").compareTo(date) == 0
                    && row.get("datetime1").compareTo(datetime) == 1);
    execQueryCheckEmpty("select " + select.select + " from " + select.tableName + " where date1 = '1991-12-08' and datetime1 > '1991-12-08 06:57:00.0' ", expected);
  }

  @Test
  public void testIndex4_DATE_COMPOSITE02() throws ParseException {
    DateFormat yearFormat = new SimpleDateFormat("yyyy");
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    DateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    Date year = yearFormat.parse("1931");
    Date date = dateFormat.parse("1991-12-08");
    Date datetime = dateTimeFormat.parse("1991-12-08 06:57:12.0");

    SelectCols select = new SelectCols(DATETIME_TABLE_DATARESULT, new String[] {"id", "date1", "datetime1" ,"year1" } );
    List<String> expected = this.filter(select,
            row -> row.get("date1").compareTo(date) == 0
                    && row.get("datetime1").compareTo(datetime) == 0
                    && row.get("year1").compareTo(year) == 0);
    execQueryCheckEmpty("select " + select.select + " from " + select.tableName + " where date1 = '1991-12-08' and datetime1 = '1991-12-08 06:57:12' and year1 = 1931", expected);
  }

  @Test
  public void testIndex4_DATE_COMPOSITE_UNIQ() throws ParseException {
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    DateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    DateFormat yearFormat = new SimpleDateFormat("yyyy");
    Date date = dateFormat.parse("1995-12-14");
    Date datetime = dateTimeFormat.parse("1995-12-14 20:51:38.0");
    Date year = yearFormat.parse("1947");

    SelectCols select = new SelectCols(DATETIME_TABLE_DATARESULT, new String[] {"id", "date2", "datetime2" ,"year2" } );
    List<String> expected = this.filter(select,
            row -> row.get("date2").compareTo(date) == 0
                    && row.get("datetime2").compareTo(datetime) == 0
                    && row.get("year2").compareTo(year) == 0);
    execQueryCheckEmpty("select " + select.select + " from " + select.tableName
            + " where date2 = '1995-12-14' and datetime2 = '1995-12-14 20:51:38' and year2 = 1947", expected);
  }

  @Test
  public void testIndex4_TIME_COMPOSITE01() throws ParseException {
    DateFormat timeStampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss");
    Time time = new Time(format.parse("00:10:20").getTime());  //00:10:20
    Timestamp timestamp = new Timestamp(timeStampFormat.parse("1992-12-13 06:57:10.0").getTime());

    SelectCols select = new SelectCols(DATETIME_TABLE_DATARESULT, new String[] {"id", "time1", "timestamp1" } );
    List<String> expected = this.filter(select,
            row -> row.get("time1").compareTo(time) == 0
                    && row.get("timestamp1").compareTo(timestamp) > 0);
    execPrepareQueryCheckEmpty("select " + select.select + " from " + select.tableName + " where time1 = ? and timestamp1 > ? "
            , expected, time, timestamp);
  }

  @Test
  public void testIndex4_TIME_COMPOSITE02() throws ParseException {
    DateFormat timeStampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss");
    Time time = new Time(format.parse("00:10:20").getTime());  //00:10:20
    Timestamp timestamp = new Timestamp(timeStampFormat.parse("1992-12-13 06:57:12.0").getTime());

    SelectCols select = new SelectCols(DATETIME_TABLE_DATARESULT, new String[] {"id", "time1", "timestamp1" } );
    List<String> expected = this.filter(select,
            row -> row.get("time1").compareTo(time) == 0
                    && row.get("timestamp1").compareTo(timestamp) == 0);
    execPrepareQueryCheckEmpty("select " + select.select + " from " + select.tableName
            + " where time1 = ? and timestamp1 = ? ", expected, time, timestamp);
  }

  @Test
  public void testIndex4_TIME_COMPOSITE_UNIQ() throws ParseException {
    DateFormat timeFormat = new SimpleDateFormat("HH:mm:ss");
    DateFormat timeStampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    Time time1 = new Time(timeFormat.parse("01:16:00").getTime());
    Date timestamp1 = timeStampFormat.parse("1997-01-03 20:51:38.0");

    SelectCols select = new SelectCols(DATETIME_TABLE_DATARESULT, new String[] {"id", "time2", "timestamp2" } );
    List<String> expected = this.filter(select,
            row -> row.get("time2").compareTo(time1) == 0
                    && row.get("timestamp2").compareTo(timestamp1) == 0);
    execQueryCheckEmpty("select " + select.select + " from " + select.tableName
            + " where time2 = '01:16:00' and timestamp2 = '1997-01-03 20:51:38.0' ", expected);
  }
  //region  composite  primary key test set

  @Test
  public void testCompositePrimaryKey01() {
    SelectCols select = new SelectCols(COMPOSITE_PRIMARY_KEY_TABLE_DATARESULT, new String[]{ "t_int",
            "t_varchar", "t_decimal" });
    List<String> expected = this.filter(select,
            row -> row.get("t_int").compareTo(1) == 1
    );
    System.out.println(expected);

    execQueryCheckEmpty("select " + select.select + " from " + select.tableName + " where t_int > 1 ", expected);
  }

  @Test
  public void testCompositePrimaryKey02() {
    SelectCols select = new SelectCols(COMPOSITE_PRIMARY_KEY_TABLE_DATARESULT, new String[]{ "t_int",
            "t_varchar", "t_decimal" });
    List<String> expected = this.filter(select,
            row -> row.get("t_int").compareTo(2988) == 0
                    && row.get("t_varchar").equals("t_varchar-988")
                    && row.get("t_decimal").compareTo(new BigDecimal("1.00")) > 0);
    System.out.println(expected);

    execPrepareQueryCheckEmpty("select " + select.select + " from " + select.tableName + " where t_int = ? and t_varchar = ?"
            + " and t_decimal > ?  ", expected, 2988, "t_varchar-988", reserveBitTwo(1.00));
  }

  @Test
  public void testCompositePrimaryKey03() {
    SelectCols select = new SelectCols(COMPOSITE_PRIMARY_KEY_TABLE_DATARESULT, new String[]{ "t_int",
            "t_varchar", "t_decimal" });
    List<String> expected = this.filter(select,
            row -> row.get("t_int").compareTo(2988) == 0
                    && row.get("t_varchar").equals("t_varchar-988")
    );
    System.out.println(expected);

    execQueryCheckEmpty("select " + select.select + " from " + select.tableName + " where t_int = 2988 and t_varchar = "
            + "'t_varchar-988'  ", expected);
  }

  @Test
  public void testCompositePrimaryKey04() {
    SelectCols select = new SelectCols(COMPOSITE_PRIMARY_KEY_TABLE_DATARESULT, new String[]{ "t_int",
            "t_varchar", "t_decimal" });
    List<String> expected = this.filter(select,
            row -> row.get("t_int").compareTo(2000) == 0
    );

    execQueryCheckEmpty("select " + select.select + " from " + select.tableName + " where t_int = 2000 ", expected);
  }

  @Test
  public void testCompositePrimaryKey05() {
    SelectCols select = new SelectCols(COMPOSITE_PRIMARY_KEY_TABLE_DATARESULT, new String[]{ "t_int",
            "t_varchar", "t_decimal" });
    List<String> expected = this.filter(select,
            row -> row.get("t_int").compareTo(2988) == 0
                    && row.get("t_varchar").equals("t_varchar-988"));
    System.out.println(expected);

    execPrepareQueryCheckEmpty("select " + select.select + " from " + select.tableName + " where t_int = ? and t_varchar = ?"
            + "  ", expected, 2988, "t_varchar-988");
  }

  @Test
  public void testCompositePrimaryKey06() {
    SelectCols select = new SelectCols(COMPOSITE_PRIMARY_KEY_TABLE_DATARESULT, new String[]{ "v1", "i1" });
    List<String> expected = this.filter(select,
            row -> row.get("i1").equals(25) || row.get("i1").equals(26)|| row.get("i1").equals(27));

    execPrepareQueryCheckEmpty("select " + select.select + " from " + select.tableName + " where i1 = ? or i1 = ? or i1 = ?"
            , expected, 25, 26, 27);
  }

  @Test
  public void testCompositePrimaryKey01222() {

    SelectCols select = new SelectCols(DATETIME_TABLE_DATARESULT, new String[] {"id", "time2", "timestamp2" } );
    List<String> expected = new ArrayList<>();

//    execQuery("select count(id) from " + select.tableName + " where id =  6 group by time2", expected);
//    execQuery("select count(id) from " + select.tableName + " group by time2", expected);
//    execQuery("select count(distinct time2 ) from " + select.tableName + "", expected);
    execQuery("select count( time2) from " + select.tableName + " ", expected);
  }


  //endregion

  private void execPrepareQueryCheckEmpty(String sql, List<String> expected, Object... args) {
    Assert.assertTrue("expected is empty !", !expected.isEmpty());
    execPrepareQuery(sql, expected, args);
  }

  private void execQueryCheckEmpty(String sql, List<String> expected) {
    Assert.assertTrue("expected is empty !", !expected.isEmpty());
    execQuery(sql, expected, false);
  }


  protected List<String> filter(SelectCols selectCols, Predicate<Map<String, Comparable>> predicate) {
    return selectCols.getResult().stream().filter(predicate).map(r -> getResultString(selectCols.colNames, r)).collect(Collectors.toList());
  }

  private String getResultString(String[] columnNames, Map<String, Comparable> r) {
    String result = "";
    int colsSize = columnNames.length;
    for (int i = 0; i < colsSize; i++) {
      String colName = columnNames[i];
      result += (colName + "=" + r.get(colName).toString());
      if (i < colsSize - 1) {
        result += "; ";
      }
    }
    return result;
  }

  private void selectAllfromTablebaker(TableDataResult tableResult) {
    String tableName = tableResult.tableName;
    List<Map<String, Comparable>> data_result = tableResult.data_result;
    List<Tuple2<String, Integer>> meta = tableResult.table_meta;
    execQuery("select * from " + tableName, resultSet -> {
      try {
        for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
          meta.add(Tuples.of(resultSet.getMetaData().getColumnName(i), resultSet.getMetaData().getColumnType(i)));
        }
        while (resultSet.next()) {
          Map<String, Comparable> record = new HashMap<>();
          int count = resultSet.getMetaData().getColumnCount();
          for (int i = 1; i <= count; i++) {
            try {
              record.put(resultSet.getMetaData().getColumnLabel(i), (Comparable) resultSet.getObject(i));
            } catch (Exception e) {}

          }
          data_result.add(record);
        }
      } catch (Exception e) {
        e.printStackTrace();
        TestUtil.rethrow(e);
      }
    });
  }

  class SelectCols {
    String tableName;
    String[] colNames;
    String select;
    TableDataResult dataResult;


    public SelectCols(TableDataResult dataResult, String[] colNames) {
      this.dataResult = dataResult;
      this.tableName = dataResult.tableName;
      this.colNames = colNames;
      this.select = StringUtils.join(colNames, ",");
    }

    public List<Map<String, Comparable>> getResult() {
      return dataResult.data_result;
    }
  }

  static class TableDataResult {
    String tableName;
    List<Map<String, Comparable>> data_result = new ArrayList<>();
    List<Tuple2<String, Integer>> table_meta = new ArrayList<>();

    public TableDataResult(String tableName) {
      this.tableName = tableName;
    }
  }
}
