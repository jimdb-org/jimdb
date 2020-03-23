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
import java.sql.Types;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import io.jimdb.test.TestUtil;
import io.jimdb.test.mysql.SqlTestBase;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Lists;

import joptsimple.internal.Strings;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

/**
 * @version V1.0
 */
public class SelectAggTest extends SqlTestBase {
  private static String DBNAME = "test_agg";

  @BeforeClass
  public static void init() {
    createDB();
    createTable();
    initData();
  }

  private static void createDB() {
    createCatalog(DBNAME);
    useCatalog(DBNAME);
  }

  private static void createTable() {

    String sql = "CREATE TABLE IF NOT EXISTS `baker_agg` ( "
            + "`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT, "
            + "`name` varchar(255) DEFAULT NULL, "
            + "`age` int(11) DEFAULT NULL, "
            + "`country` varchar(100) NOT NULL, "
            + "`num_float` float NOT NULL, "
            + "`num_double` double NOT NULL, "
            + "`num_smallint` smallint NOT NULL, "
            + "`num_bigint` bigint NOT NULL, "
            + "`gender` tinyint NOT NULL, "
            + "`company` varchar(100) NOT NULL, "
            + "`card_id` bigint NOT NULL, "
            + "`cost_decimal` decimal(10,2) NOT NULL, "
            + "`crt_timestamp` timestamp NOT NULL, "
            + "`crt_date` date NOT NULL, "
            + "`crt_datetime` datetime NOT NULL, "
            + "`crt_time` time NOT NULL, "
            + "`crt_year` year NOT NULL, "
            + "PRIMARY KEY (`id`),"
            + "INDEX age_idx (age), "
            + "INDEX gender_idx (gender), "
            + "UNIQUE INDEX cardid_idx (card_id) "
            + ") COMMENT 'REPLICA=1' ENGINE=MEMORY AUTO_INCREMENT=0 ";
    dropAndCreateTable("baker_agg", sql);
  }

  private static void initData() {

    int size = 1000;
    String[] name = new String[]{ "Tom", "Jack", "Mary", "Suzy", "Kate", "Luke", "Bob", "Polly", "Alex", "Barke",
            "Mara", "Martin" };
    String[] country = new String[]{ "china", "japan", "usa", "korea", "england", "french", "brazil", "india" };
    String[] company = new String[]{ "jingdong", "ibm", "oracle", "alibaba", "tencent", "sap", "facebook",
            "huawei", "baidu", "mi" };
    DateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    DateFormat timeFormat = new SimpleDateFormat("hh:mm:ss");
    int idx = 0;
    for (int batch = 0; batch < size / 10; batch++) {
      int onesize = 10;
      String sql = "INSERT INTO baker_agg("
              + "name, age, country, num_float, num_double, "
              + "num_smallint, num_bigint, gender, company, card_id,cost_decimal, "
              + "crt_timestamp, crt_date, crt_datetime, crt_time, crt_year) VALUES";
      for (int n = 0; n < onesize; n++) {
        Date base = new Date();
        Date date = new Date(base.getTime() + idx * 24 * 3600 * 1000);
        int year = idx % 200 + 1905;
        sql += "('" + name[idx % name.length] + "-" + (idx % size) + "' , " + (idx % 100) + ", '" + country[idx %
                country.length] + "', " + (idx % 90) + ", " + (idx % 70) + ", "
                + (idx % 16) + ", " + (idx % 100) + "00000000, " + (idx % 2) + ", '" + company[idx % company
                .length] + "', " + idx + " , " + reserveBitTwo(batch * 10 + 1.01) +
                ", '" + dateTimeFormat.format(date) + "', '" + dateFormat.format(date) + "','" + dateTimeFormat
                .format(date) + "','" + timeFormat.format(date) + "', " + year + ")";
        if (n + 1 < 10) {
          sql += ",";
        }
        idx++;
      }
      execUpdate(sql, onesize, true);
    }
  }

  private static double reserveBitTwo(double d) {
    return new BigDecimal(d).setScale(2, BigDecimal.ROUND_FLOOR).doubleValue();
  }

  @Before
  public void load() {
    useCatalog(DBNAME);
  }

  @Test
  public void testSelectAll01() {
    List<String> expected = expectedStr(new String[]{
            "COUNT(1)=1000" });
    execQuery("select COUNT(1) from baker_agg", expected);
  }

  @Test
  public void testSelectDoubleGroupby() {
    List<String> expected = expectedStr(new String[]{
            "sum=190.0; avg=38.0; cnt=5; max=58.0; min=18.0; country=china; gender=0; company=baidu",
            "sum=230.0; avg=46.0; cnt=5; max=66.0; min=26.0; country=china; gender=0; company=facebook",
            "sum=100.0; avg=20.0; cnt=5; max=40.0; min=0.0; country=china; gender=0; company=jingdong",
    });
    execQuery("select sum(num_double) as sum, avg(num_double) as avg, count(1) as cnt, max(num_double) as max,"
            + "min(num_double) as min, country, gender, company "
            + "from baker_agg "
            + "where age between 25 and 45 and (country = 'china' or country = 'usa') "
            + "group by country,gender,company order by country,gender,company limit 3", expected);
  }

  @Test
  public void testSelectFloatGroupby() {
    List<String> expected = expectedStr(new String[]{
            "sum=210.0; avg=42.0; cnt=5; max=78.0; min=8.0; country=china; gender=0; company=baidu",
            "sum=250.0; avg=50.0; cnt=5; max=86.0; min=16.0; country=china; gender=0; company=facebook",
            "sum=220.0; avg=44.0; cnt=5; max=80.0; min=10.0; country=china; gender=0; company=jingdong",
    });
    execQuery("select sum(num_float) as sum,avg(num_float) as avg,count(1) as cnt,max(num_float) as max,min"
            + "(num_float) as min,country,gender,company "
            + "from baker_agg "
            + "where age between 25 and 45 and (country = 'china' or country = 'usa') "
            + "group by country,gender,company order by country,gender,company limit 3", expected);
  }

  @Test
  public void testSelectSmallintGroupby() {
    List<String> expected = expectedStr(new String[]{
            "sum=104; avg=4.1600; cnt=25; max=8; min=0; country=china; gender=0; company=baidu",
            "sum=96; avg=3.8400; cnt=25; max=8; min=0; country=china; gender=0; company=facebook",
            "sum=96; avg=3.8400; cnt=25; max=8; min=0; country=china; gender=0; company=jingdong"
    });
    execQuery("select sum(num_smallint) as sum,avg(num_smallint) as avg,count(1) as cnt,max(num_smallint) as max,min"
            + "(num_smallint) as min,country,gender,company "
            + "from baker_agg "
            + "where country = 'china' or country = 'usa' "
            + "group by country,gender,company order by country,gender,company limit 3", expected);
  }

  @Test
  public void testSelectTinyintGroupby() {
    List<String> expected = expectedStr(new String[]{
            "sum=0; avg=0.0000; cnt=25; max=0; min=0; country=china; gender=0; company=baidu",
            "sum=0; avg=0.0000; cnt=25; max=0; min=0; country=china; gender=0; company=facebook",
            "sum=0; avg=0.0000; cnt=25; max=0; min=0; country=china; gender=0; company=jingdong"
    });
    execQuery("select sum(gender) as sum, avg(gender) as avg, count(1) as cnt, max(gender) as max, min(gender) as min, "
            + "country, gender, company "
            + "from baker_agg "
            + "where  (country = 'china' or country = 'japan') "
            + "group by country,gender,company "
            + "order by country,gender,company "
            + "limit 3", expected);
  }

  @Test
  public void testSelectIntGroupby() {
    List<String> expected = expectedStr(new String[]{
            "sum=6250; avg=50.0000; cnt=125; max=98; min=2; country=brazil",
            "sum=6000; avg=48.0000; cnt=125; max=96; min=0; country=china",
            "sum=6000; avg=48.0000; cnt=125; max=96; min=0; country=england"
    });
    execQuery("select sum(age) as sum,avg(age) as avg,count(1) as cnt,max(age) as max,min(age) as min,country "
            + "from baker_agg "
            + "group by country order by country limit 3", expected);
  }

  @Test
  public void testSelectBigIntGroupby() {
    List<String> expected = expectedStr(new String[]{
            "company=baidu; sum=120000000000; avg=4800000000.0000; cnt=25; max=8800000000; min=800000000; "
                    + "country=china; "
                    + "gender=0; company=baidu",
            "company=facebook; sum=140000000000; avg=5600000000.0000; cnt=25; max=9600000000; min=1600000000; "
                    + "country=china; gender=0; company=facebook",
            "company=jingdong; sum=100000000000; avg=4000000000.0000; cnt=25; max=8000000000; min=0; country=china; "
                    + "gender=0; company=jingdong"
    });
    execQuery("select company,sum(num_bigint) as sum,avg(num_bigint) as avg,count(1) as cnt,max(num_bigint) as max,"
            + "min(num_bigint) as min,country,gender,company "
            + "from baker_agg "
            + "where  (country = 'china' or country = 'japan') "
            + "group by country,gender,company "
            + "order by country,gender,company limit 3", expected);
  }

  @Test
  public void testSelectVarcharMaxMinGroupbyCol() {
    List<String> expected = expectedStr(new String[]{
            "MAX(company)=tencent; MIN(company)=baidu; country=brazil",
            "MAX(company)=tencent; MIN(company)=baidu; country=china",
            "MAX(company)=tencent; MIN(company)=baidu; country=england",
            "MAX(company)=sap; MIN(company)=alibaba; country=french",
            "MAX(company)=sap; MIN(company)=alibaba; country=india",
            "MAX(company)=sap; MIN(company)=alibaba; country=japan",
            "MAX(company)=sap; MIN(company)=alibaba; country=korea",
            "MAX(company)=tencent; MIN(company)=baidu; country=usa"
    });
    execQuery("select max(company),min(company),country "
            + "from baker_agg "
            + "group by country "
            + "order by country", expected);
  }

  @Test
  public void testSelectIndexGroupbyCol() {
    List<String> expected = expectedStr(new String[]{
            "age=20; COUNT(1)=10; AVG(age)=20.0000; MIN(age)=20; SUM(age)=200; MAX(age)=20; age=20",
            "age=21; COUNT(1)=10; AVG(age)=21.0000; MIN(age)=21; SUM(age)=210; MAX(age)=21; age=21",
            "age=22; COUNT(1)=10; AVG(age)=22.0000; MIN(age)=22; SUM(age)=220; MAX(age)=22; age=22"
    });
    execQuery("select age, count(1),avg(age),min(age),sum(age),max(age),age  "
            + "from baker_agg  where age >= 20 and age < 60 "
            + "group by age order by age limit 3", expected);
  }

  @Test
  public void testSelectIndexLookUpGroupbyCol() {
    List<String> expected = expectedStr(new String[]{
            "company=jingdong; country=england; age=20; COUNT(1)=10; AVG(age)=20.0000; MIN(age)=20; SUM(age)=200; MAX"
                    + "(age)"
                    + "=20; age=20",
            "company=ibm; country=french; age=21; COUNT(1)=10; AVG(age)=21.0000; MIN(age)=21; SUM(age)=210; MAX(age)"
                    + "=21; "
                    + "age=21",
            "company=oracle; country=brazil; age=22; COUNT(1)=10; AVG(age)=22.0000; MIN(age)=22; SUM(age)=220; MAX(age)"
                    + "=22; age=22"
    });
    execQuery("select company, country , age, count(1),avg(age),min(age),sum(age),max(age),age  "
            + "from baker_agg  where age >= 20 and age < 60 "
            + "group by age order by age limit 3", expected);
  }

  @Test
  public void testSelectPKGroupbyCol() {
    List<String> expected = expectedStr(new String[]{
            "age=99; COUNT(1)=10; AVG(age)=99.0000; MIN(age)=99; SUM(age)=990; MAX(age)=99; age=99",
            "age=98; COUNT(1)=10; AVG(age)=98.0000; MIN(age)=98; SUM(age)=980; MAX(age)=98; age=98",
            "age=97; COUNT(1)=10; AVG(age)=97.0000; MIN(age)=97; SUM(age)=970; MAX(age)=97; age=97"
    });
    execQuery("select age, count(1),avg(age),min(age),sum(age),max(age),age  "
            + "from baker_agg  where id  > 10 "
            + "group by age order by age desc limit 3", expected);
  }

  @Test
  public void testSelectCountDistinctGroupby1() {
    List<String> expected = expectedStr(new String[]{
            "country=korea; company=alibaba",
            "country=china; company=baidu",
            "country=brazil; company=facebook",
            "country=india; company=huawei",
            "country=japan; company=ibm",
            "country=china; company=jingdong",
            "country=japan; company=mi",
            "country=usa; company=oracle",
            "country=french; company=sap",
            "country=england; company=tencent"
    });
    execQuery("select distinct country , company from baker_agg group by company order by company", expected);
  }

  @Test
  public void testSelectGroupbyHaving() {
    List<String> expected = expectedStr(new String[]{
            "company=baidu; country=china; aa=710.0; cc=35.5; COUNT(1)=20; MAX(age)=88; MIN(age)=28",
            "company=baidu; country=england; aa=780.0; cc=39.0; COUNT(1)=20; MAX(age)=88; MIN(age)=28",
            "company=facebook; country=usa; aa=740.0; cc=37.0; COUNT(1)=20; MAX(age)=86; MIN(age)=26"
    });
    execQuery("select company, country, sum(num_double) as aa, avg(num_double) as cc, count(1), max(age), min(age)  "
            + "from baker_agg "
            + "where age > 10 and (country ='china' or country = 'england' or country= 'usa') "
            + "group by country,company "
            + "having aa > 700 and aa <= 800 "
            + "order by company, cc desc limit 3 ", expected);
  }

  @Test
  public void testSelectGroupbyHaving1() {
    List<String> expected = expectedStr(new String[]{
            "age=40; num_double=40.0; company=jingdong; country=china",
            "age=40; num_double=30.0; company=jingdong; country=china",
            "age=40; num_double=20.0; company=jingdong; country=china"
    });
    execQuery("select age, num_double, company, country  "
            + "from baker_agg where (age > 30 and age <= 40) having country = 'china'"
            + " order by age desc limit 3 ", expected);
  }

  @Test
  public void testMax() {
    List<String> expected = expectedStr(new String[]{
            "MAX(age)=1; COUNT(1)=10"
    });
    execQuery("select max(age),count(1) from baker_agg where age = 1 ", expected);
  }

  @Test
  public void testSelectGroupbyHaving2() {
    List<String> expected = expectedStr(new String[]{
            "SUM(age)=4500; company=jingdong",
            "SUM(age)=4600; company=ibm",
            "SUM(age)=4700; company=oracle"
    });
    execQuery("select sum(age),company from baker_agg group by company having min(age) < 3 ", expected);
  }

  @Test
  public void testSelectGroupbyHavingOrderByInt() {
    List<String> expected = expectedStr(new String[]{
            "age=32; num_double=32.0; company=oracle; country=china"
    });
    execQuery("select age, num_double, company, country  "
            + "from baker_agg where (age > 30 and age <= 40) having country = 'china'"
            + " order by age limit 1  ", expected);
  }

  @Test
  public void testSelectGroupbyHavingOrderByStringAndInt() {
    List<String> expected = expectedStr(new String[]{
            "age=32; num_double=32.0; company=oracle; country=china",
            "age=32; num_double=22.0; company=oracle; country=china",
            "age=32; num_double=12.0; company=oracle; country=china"
    });
    execQuery("select age, num_double, company, country  "
            + "from baker_agg where (age > 30 and age <= 40) having country = 'china'"
            + " order by company desc,id asc limit 3", expected);
  }

  @Test
  public void testSelectGroupbyHavingOrderByIntAndDouble() {
    List<String> expected = expectedStr(new String[]{
            "age=40; num_double=40.0; company=jingdong; country=china",
            "age=40; num_double=30.0; company=jingdong; country=china",
            "age=40; num_double=20.0; company=jingdong; country=china"
    });
    execQuery("select age, num_double, company, country  "
            + "from baker_agg where (age > 30 and age <= 40) having country = 'china'"
            + " order by age desc,num_double desc limit 3 ", expected);
  }

  @Test
  public void testSelectIndexHaving() {
    List<String> expected = expectedStr(new String[]{
            "id=98; age=57",
            "id=97; age=56",
            "id=96; age=55"
    });
    execQuery("select id, age  "
            + "from baker_agg where (age > 50 and age <= 60) having id < 100 and id > 50 "
            + " order by id desc  limit 1, 3 ", expected);
  }

  @Test
  public void testDistinct() {

    List<Map<String, Comparable>> allResult = new ArrayList<>();
    List<Tuple2<String, Integer>> meta = new ArrayList<>();

    selectAllfromTablebaker02(allResult, meta);

    for (int i = 0; i < meta.size(); i++) {
      final String metaName = meta.get(i).getT1();

      List<String> values = allResult.stream().map(r -> r.get(metaName)).distinct().sorted()
              .map(v -> metaName + "=" + v.toString()).collect(Collectors.toList());

      //all
      execQuery("select DISTINCT(" + metaName + ") from baker_agg order by " + metaName, values);

      //top 10-asc
      values = allResult.stream().map(r -> r.get(metaName)).distinct().sorted()
              .limit(10).map(v -> metaName + "=" + v.toString()).collect(Collectors.toList());
      execQuery("select DISTINCT(" + metaName + ") from baker_agg order by " + metaName + " asc limit 10",
              values);

      //top 10-desc
      values = allResult.stream().map(r -> r.get(metaName)).distinct()
              .sorted().map(v -> metaName + "=" + v.toString()).collect(Collectors.toList());
      int offset = values.size() - 10 < 0 ? 0 : values.size() - 10;
      values = values.subList(offset, values.size());
      execQuery("select DISTINCT(" + metaName + ") from baker_agg order by " + metaName + " desc limit 10", values);
    }
  }

  @Test
  public void testDistinctWildcard() {

    List<Map<String, Comparable>> allResult = new ArrayList<>();
    List<Tuple2<String, Integer>> meta = new ArrayList<>();

    selectAllfromTablebaker02(allResult, meta);

    List<String> values = allResult.stream().sorted((m1, m2) -> m2.get("id").compareTo(m2.get("id"))).map(m -> {
      List<String> list = new ArrayList<>();
      for (Tuple2<String, Integer> tu : meta) {
        list.add(tu.getT1() + "=" + m.get(tu.getT1()).toString());
      }
      return Strings.join(list, "; ");
    }).collect(Collectors.toList());

    //all
    execQuery("select DISTINCT(*) from baker_agg order by id", values);
  }

  @Test
  public void testDistinctWithSum() {

    List<Map<String, Comparable>> allResult = new ArrayList<>();
    List<Tuple2<String, Integer>> meta = new ArrayList<>();

    selectAllfromTablebaker02(allResult, meta);

    for (int i = 0; i < meta.size(); i++) {
      Tuple2<String, Integer> metaKV = meta.get(i);
      final String metaName = metaKV.getT1();
      if (!isNumberAndDateTime(meta.get(i))) {
        continue;
      }

      System.out.println("execute by cloum : " + metaName);

      if (metaName.equals("crt_year") || metaName.equals("id")) { //TODO: FIX ME , the year value is  2038-01-01
        continue;
      }

      BigDecimal result = new BigDecimal(0);

      List<BigDecimal> collect =
              allResult.stream().map(r -> r.get(metaName)).distinct().map(v -> new BigDecimal(castNumValue(v.toString(), metaKV.getT2()))).collect(Collectors.toList());

      for (BigDecimal bigDecimal : collect) {
        result = result.add(bigDecimal);
      }

      List<String> actual = execQuery("select SUM(DISTINCT(" + metaName + ")) from baker_agg");
      List<String> expected = Lists.newArrayList("SUM(DISTINCT " + metaName + ")=" + result.toString());

      String a = actual.get(0);
      String e = expected.get(0);
      BigDecimal ba = new BigDecimal(a.split("=")[1]);
      BigDecimal be = new BigDecimal(e.split("=")[1]);

      if (ba.doubleValue() != be.doubleValue()) {
        if (be.doubleValue() == 0) {
          throw new RuntimeException(metaName + " is zero");
        }
        System.out.println("meta:" + metaName + " actual:" + actual + " expected:" + expected);
        Assert.assertTrue(Math.abs(ba.divide(be, 4, BigDecimal.ROUND_HALF_UP).floatValue() - 1) < 0.001);
      }
    }
  }

  @Test
  public void testDistinctWithCount() {

    List<Map<String, Comparable>> allResult = new ArrayList<>();
    List<Tuple2<String, Integer>> meta = new ArrayList<>();

    selectAllfromTablebaker02(allResult, meta);

    for (int i = 0; i < meta.size(); i++) {
      final String metaName = meta.get(i).getT1();
      long value = allResult.stream().map(r -> r.get(metaName)).distinct().count();
      execQuery("select COUNT(DISTINCT(" + metaName + ")) from baker_agg",
              Lists.newArrayList("COUNT(DISTINCT " + metaName + ")=" + value));
    }
  }

  @Test
  public void testWithMaxMin() {

    List<Map<String, Comparable>> allResult = new ArrayList<>();
    List<Tuple2<String, Integer>> meta = new ArrayList<>();
    selectAllfromTablebaker02(allResult, meta);

    for (int i = 0; i < meta.size(); i++) {
      final String metaName = meta.get(i).getT1();

      Comparable max = allResult.stream().map(r -> r.get(metaName)).max(Comparable::compareTo).get();
      execQuery("select MAX(" + metaName + ") from baker_agg", Lists.newArrayList("MAX(" + metaName + ")=" + max));

      Comparable min = allResult.stream().map(r -> r.get(metaName)).min(Comparable::compareTo).get();
      execQuery("select MIN(" + metaName + ") from baker_agg", Lists.newArrayList("MIN(" + metaName + ")=" + min));
    }
  }

  @Test
  public void testDistinctWithMaxMin() {

    List<Map<String, Comparable>> allResult = new ArrayList<>();
    List<Tuple2<String, Integer>> meta = new ArrayList<>();
    selectAllfromTablebaker02(allResult, meta);

    for (int i = 0; i < meta.size(); i++) {
      final String metaName = meta.get(i).getT1();

      Comparable max = allResult.stream().map(r -> r.get(metaName)).max(Comparable::compareTo).get();
      execQuery("select MAX(DISTINCT(" + metaName + ")) from baker_agg",
              Lists.newArrayList("MAX(DISTINCT " + metaName + ")=" + max));

      Comparable min = allResult.stream().map(r -> r.get(metaName)).min(Comparable::compareTo).get();
      execQuery("select MIN(DISTINCT(" + metaName + ")) from baker_agg",
              Lists.newArrayList("MIN(DISTINCT " + metaName + ")=" + min));
    }
  }

  @Test
  public void testDistinctWithAvg() {

    List<Map<String, Comparable>> allResult = new ArrayList<>();
    List<Tuple2<String, Integer>> meta = new ArrayList<>();
    selectAllfromTablebaker02(allResult, meta);

    for (int i = 0; i < meta.size(); i++) {
      Tuple2<String, Integer> metaKV = meta.get(i);
      final String metaName = metaKV.getT1();

      if (!isNumber(metaKV)) {
        continue;
      }

      BigDecimal count = new BigDecimal(allResult.stream().map(r -> r.get(metaName)).distinct().count());
      List<BigDecimal> sumList =
              allResult.stream().map(r -> r.get(metaName)).distinct().map(m -> new BigDecimal(m.toString())).collect(Collectors.toList());
      BigDecimal sum = new BigDecimal(0);
      if (metaName.equals("cost_decimal")) {
        // cost_decimal's scale is 2 , mysql avg scale = decimal scale + 4
        sum = BigDecimal.valueOf(0, 2 + 4);
      }
      for (BigDecimal bigDecimal : sumList) {
        sum = sum.add(bigDecimal);
      }

      List<String> actual = execQuery("select AVG(DISTINCT(" + metaName + ")) from baker_agg");
      List<String> expected = Lists.newArrayList("AVG(DISTINCT " + metaName + ")=" + sum.divide(count));

      String a = actual.get(0);
      String e = expected.get(0);
      if (metaKV.getT2().equals(Types.BIGINT)
              || metaKV.getT2().equals(Types.INTEGER) || metaKV.getT2().equals(Types.SMALLINT)
              || metaKV.getT2().equals(Types.TINYINT)) {
        BigDecimal ba = new BigDecimal(a.split("=")[1]);
        ba.setScale(4);
        BigDecimal be = new BigDecimal(e.split("=")[1]);
        be.setScale(4);
        Assert.assertTrue(Math.abs(ba.subtract(be).doubleValue()) == 0.0000);
      } else {
        Assert.assertEquals(e, a);
      }
    }
  }

  @Test
  public void testWithAvg() {

    List<Map<String, Comparable>> allResult = new ArrayList<>();
    List<Tuple2<String, Integer>> meta = new ArrayList<>();
    selectAllfromTablebaker02(allResult, meta);

    for (int i = 0; i < meta.size(); i++) {
      Tuple2<String, Integer> metaKV = meta.get(i);
      final String metaName = metaKV.getT1();

      if (!isNumber(metaKV) || metaName.equals("id")) {
        continue;
      }

      BigDecimal count = new BigDecimal(allResult.stream().map(r -> r.get(metaName)).count());
      List<BigDecimal> sumList =
              allResult.stream().map(r -> r.get(metaName)).map(m -> new BigDecimal(m.toString())).collect(Collectors.toList());
      BigDecimal sum = new BigDecimal(0);
      if (metaName.equals("cost_decimal")) {
        // cost_decimal's scale is 2 , mysql avg scale = decimal scale + 4
        sum = BigDecimal.valueOf(0, 2 + 4);
      }
      for (BigDecimal bigDecimal : sumList) {
        sum = sum.add(bigDecimal);
      }

      List<String> actual = execQuery("select AVG(" + metaName + ") from baker_agg");
      List<String> expected = Lists.newArrayList("AVG(" + metaName + ")=" + (sum.divide(count)));

      String a = actual.get(0);
      String e = expected.get(0);
      if (metaKV.getT2().equals(Types.BIGINT)
              || metaKV.getT2().equals(Types.INTEGER) || metaKV.getT2().equals(Types.SMALLINT)
              || metaKV.getT2().equals(Types.TINYINT)) {
        BigDecimal ba = new BigDecimal(a.split("=")[1]);
        BigDecimal be = new BigDecimal(e.split("=")[1]);
        Assert.assertTrue(Math.abs(ba.subtract(be).doubleValue()) == 0.0000);
      } else {
        Assert.assertEquals(e, a);
      }
    }
  }

  private String castNumValue(String v, Integer nameType) {
    try {
      switch (nameType) {
        case Types.TIMESTAMP:
          return new SimpleDateFormat("yyyyMMddHHmmss").format(new SimpleDateFormat("yyyy-MM-dd "
                  + "HH:mm:ss.S").parse(v));
        case Types.DATE:
          return new SimpleDateFormat("yyyyMMddHHmmss").format(new SimpleDateFormat("yyyy-MM-dd").parse(v));
        case Types.TIME:
          return new SimpleDateFormat("HHmmss").format(new SimpleDateFormat("HH:mm:ss").parse(v));
        default:
          return v;
      }
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage() + ":[" + v + "] type:" + nameType);
    }
  }

  private boolean isNumberAndDateTime(Tuple2<String, Integer> nameType) {
    switch (nameType.getT2()) {
      case Types.TINYINT:
      case Types.SMALLINT:
      case Types.INTEGER:
      case Types.BIGINT:
      case Types.FLOAT:
      case Types.REAL:
      case Types.DOUBLE:
      case Types.NUMERIC:
      case Types.DECIMAL:
      case Types.TIME:
      case Types.DATE:
      case Types.TIMESTAMP:
        return true;
      default:
        return false;
    }
  }

  private boolean isNumber(Tuple2<String, Integer> nameType) {
    switch (nameType.getT2()) {
      case Types.TINYINT:
      case Types.SMALLINT:
      case Types.INTEGER:
      case Types.BIGINT:
      case Types.FLOAT:
      case Types.REAL:
      case Types.DOUBLE:
      case Types.NUMERIC:
      case Types.DECIMAL:
        return true;
      default:
        return false;
    }
  }

  private void selectAllfromTablebaker02(List<Map<String, Comparable>> allResult, List<Tuple2<String, Integer>> meta) {

    execQuery("select * from baker_agg", resultSet -> {
      try {
        for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
          meta.add(Tuples.of(resultSet.getMetaData().getColumnName(i), resultSet.getMetaData().getColumnType(i)));
        }

        while (resultSet.next()) {
          int count = resultSet.getMetaData().getColumnCount();
          Map<String, Comparable> record = new HashMap<>();
          for (int i = 1; i <= count; i++) {
            record.put(resultSet.getMetaData().getColumnLabel(i), (Comparable) resultSet.getObject(i));
          }
          allResult.add(record);
        }
      } catch (Exception e) {
        TestUtil.rethrow(e);
      }
    });
  }

  @Test
  public void testCountInMultiThreads() {
    int size = 200;
    CountDownLatch latch = new CountDownLatch(size);
    ExecutorService service = Executors.newFixedThreadPool(4);

    for (int i = 0; i < size; i++) {
      service.submit(() -> {
        try {
          execQuery("select COUNT(1) from " + DBNAME + ".baker_agg  where name = 'aaa' group by age");
        } catch (Exception e) {
          e.printStackTrace();
        } finally {
          latch.countDown();
        }
      });
    }

    try {
      latch.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testSelectCountDistinct() {
    List<Map<String, Comparable>> allResult = new ArrayList<>();
    List<Tuple2<String, Integer>> meta = new ArrayList<>();

    selectAllfromTablebaker02(allResult, meta);

    List<String> esp = new ArrayList<>();
    allResult.stream().collect(Collectors.groupingBy((r) -> r.get("company"))).forEach((k, v) -> {
      long count = v.stream().map((row) -> row.get("country")).distinct().count();
      String value = "COUNT(DISTINCT country)=" + count + "; company=" + k;
      esp.add(value);
    });
    execQuery("select count(distinct country), company from baker_agg group by company", esp);
  }

  @Test
  public void testSelectSumDistinct() {
    List<Map<String, Comparable>> allResult = new ArrayList<>();
    List<Tuple2<String, Integer>> meta = new ArrayList<>();

    selectAllfromTablebaker02(allResult, meta);

    for (Tuple2<String, Integer> metaKV : meta) {
      final String metaName = metaKV.getT1();

      if (!isNumberAndDateTime(metaKV) || metaName.equals("id") || metaName.equals("crt_timestamp")
              || metaName.equals("crt_date") || metaName.equals("crt_datetime") || metaName.equals("crt_time")
              || metaName.equals("crt_year")) {
        continue;
      }
      List<String> esp = new ArrayList<>();

      allResult.stream().collect(Collectors.groupingBy((r) -> r.get("company"))).forEach((k, v) -> {
        List<BigDecimal> sumList = v.stream().map((row) -> row.get(metaName)).distinct()
                .map(m -> new BigDecimal(castNumValue(m.toString(), metaKV.getT2()))).collect(Collectors.toList());
        BigDecimal result = new BigDecimal(0);
        for (BigDecimal bigDecimal : sumList) {
          result = result.add(bigDecimal);
        }
        String value = "SUM(DISTINCT " + metaName + ")=" + result.toString() + "; company=" + k;
        esp.add(value);
      });
      execQuery("select sum(distinct " + metaName + "), company from baker_agg group by company", esp);
    }
  }

  @Test
  public void testSelectAvgDistinct() {
    List<Map<String, Comparable>> allResult = new ArrayList<>();
    List<Tuple2<String, Integer>> meta = new ArrayList<>();

    selectAllfromTablebaker02(allResult, meta);

    for (Tuple2<String, Integer> metaKV : meta) {
      final String metaName = metaKV.getT1();

      if (!isNumberAndDateTime(metaKV) || metaName.equals("id") || metaName.equals("crt_timestamp")
              || metaName.equals("crt_date") || metaName.equals("crt_datetime") || metaName.equals("crt_time")
              || metaName.equals("crt_year")) {
        continue;
      }
      List<String> esp = new ArrayList<>();

      allResult.stream().collect(Collectors.groupingBy((r) -> r.get("company"))).forEach((k, v) -> {
        List<BigDecimal> sumList = v.stream().map((row) -> row.get(metaName)).distinct()
                .map(m -> new BigDecimal(castNumValue(m.toString(), metaKV.getT2()))).collect(Collectors.toList());

        String result;
        if (metaKV.getT2().equals(Types.BIGINT)
                || metaKV.getT2().equals(Types.INTEGER) || metaKV.getT2().equals(Types.SMALLINT)
                || metaKV.getT2().equals(Types.TINYINT)) {
          BigDecimal avg = new BigDecimal("0.0000");
          if (metaName.equals("cost_decimal")) {
            // cost_decimal's scale is 2 , mysql avg scale = decimal scale + 4
            avg = BigDecimal.valueOf(0, 2 + 4);
          }
          for (BigDecimal bigDecimal : sumList) {
            avg = avg.add(bigDecimal);
          }
          avg = avg.divide(new BigDecimal(sumList.size()));
          result = avg.toString();
        } else {
          BigDecimal avg = new BigDecimal("0.0");
          if (metaName.equals("cost_decimal")) {
            // cost_decimal's scale is 2 , mysql avg scale = decimal scale + 4
            avg = BigDecimal.valueOf(0, 2 + 4);
          }
          for (BigDecimal bigDecimal : sumList) {
            avg = avg.add(bigDecimal);
          }
          avg = avg.divide(new BigDecimal(sumList.size()));
          result = avg.toString();
        }

        String value = "AVG(DISTINCT " + metaName + ")=" + result.toString() + "; company=" + k;
        esp.add(value);
      });
      execQuery("select avg(distinct " + metaName + "), company from baker_agg group by company", esp);
    }
  }

  @Test
  public void testSelectEliminateAgg() {
    List<Map<String, Comparable>> allResult = new ArrayList<>();
    List<Tuple2<String, Integer>> meta = new ArrayList<>();

    selectAllfromTablebaker02(allResult, meta);

    List<String> esp = new ArrayList<>();
    allResult.stream().collect(Collectors.groupingBy((r) -> r.get("company"))).forEach((k, v) -> {
      int count = v.stream().map(r -> (Integer) r.get("age")).distinct().collect(Collectors.toList()).size();
      List<Integer> sumDistinctList =
              v.stream().map(r -> (Integer) r.get("age")).distinct().collect(Collectors.toList());
      int sum = 0;
      for (Integer i : sumDistinctList) {
        sum += i.intValue();
      }
      int maxAge = v.stream().map(r -> (Integer) r.get("age")).max(Comparable::compareTo).get();
      int minAge = v.stream().map(r -> (Integer) r.get("age")).min(Comparable::compareTo).get();

      String value = String.format("COUNT(DISTINCT age)=%s; SUM(DISTINCT age)=%s; MAX(age)=%s; "
              + "MIN(age)=%s; company=%s", count, sum, maxAge, minAge, k);
      esp.add(value);
    });
    execQuery("select count(distinct age), sum(distinct age), max(age),min(age), company from baker_agg group by "
            + "company", esp);
  }

  @Test
  public void testStreamAgg() {
    List<String> expected = expectedStr(new String[]{
            "SUM(age)=0", "SUM(age)=10"
    });
    execQuery("select sum(age) from baker_agg group by age limit 2", expected);
  }

  //  @Test
  public void testValue() {

    int size = 1000000;
    CountDownLatch latch = new CountDownLatch(size);
    ExecutorService service = Executors.newFixedThreadPool(3);

    for (int i = 0; i < size; i++) {
      service.execute(() -> {
        try {
          execQueryNotCheck("select count(distinct card_id) from baker_agg  where card_id = 10000 ");
        } finally {
          latch.countDown();
        }
      });
    }

    try {
      latch.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testMessageRetry() {

    String TEST_TABLENAME = DBNAME + "." + "message_retry";

    execUpdate(String.format("DROP TABLE IF EXISTS %s ", TEST_TABLENAME), 0, true);

    String sql = "CREATE TABLE IF NOT EXISTS " + TEST_TABLENAME + " ("
            + " `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '自增主键',"
            + " `topic` varchar(100) NOT NULL COMMENT '主题',"
            + " `app` varchar(100) NOT NULL COMMENT '应用',"
            + " `retry_time` datetime NOT NULL COMMENT '重试时间',"
            + " `status` tinyint(4) NOT NULL DEFAULT '1' COMMENT '状态,0:成功,1:失败,-2:过期',"
            + " PRIMARY KEY (`id`),"
            + " KEY `idx_topic_app` (`topic`, `app`, `status`, `retry_time`)"
            + ")COMMENT 'REPLICA=1' ENGINE=memory AUTO_INCREMENT=0 PARTITION BY RANGE(id) PARTITIONS 1 ";
    execUpdate(sql, 0, true);

    sql = String.format("INSERT INTO %s(topic, app, retry_time, status) VALUES('topic-1','app-1','2010-02-10 "
            + "12:21:01',1)", TEST_TABLENAME);
    execUpdate(sql, 1, true);
    execQuery(String.format("select count(1),  topic from %s where status=1 group by topic ", TEST_TABLENAME));

//    int size = 1000000;
//    CountDownLatch latch = new CountDownLatch(size);
//    ExecutorService service = Executors.newFixedThreadPool(3);
//
//    for (int i = 0; i < size; i++) {
//      service.execute(() -> {
//        try {
//          execQuery(String.format("select count(1),  topic, app from %s where status=1 group by topic,app ",
//          TEST_TABLENAME));
//        } finally {
//          latch.countDown();
//        }
//      });
//    }
//
//    try {
//      latch.await();
//    } catch (InterruptedException e) {
//      e.printStackTrace();
//    }
  }

  @Test
  public void testSumDecimal() {
    List<Map<String, Comparable>> allResult = new ArrayList<>();
    List<Tuple2<String, Integer>> meta = new ArrayList<>();
    selectAllfromTablebaker02(allResult, meta);
    final BigDecimal[] cost_decimal = { BigDecimal.ZERO };
    allResult.stream().filter(row -> row.get("age").compareTo(10) == 1).forEach(row -> {
      cost_decimal[0] = ((BigDecimal) row.get("cost_decimal")).add(cost_decimal[0]);
    });
    List<String> expected = new ArrayList<>();
    expected.add(String.format("SUM(cost_decimal)=%s", cost_decimal[0]));
    System.out.println(expected);
    execQuery("select sum(cost_decimal) from baker_agg where age > 10 ", expected);
  }
}
