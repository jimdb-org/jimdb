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

package io.jimdb.test.mysql.dml;

import java.util.List;

import io.jimdb.test.mysql.SqlTestBase;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 table blue_explain_test
 |--------------------------------------------------------------------|
 |column	   	  Type	  	primary-key	unsigned   auto-incrementing    |
 |id	     	    BigInt	  	是	        否	        是                |
 |name	   	    Varchar	  	否	        否	        否                |
 |age	   	      Int	  	    否	        否	        否                |
 |country	   	  Varchar	  	否	        否	        否                |
 |num_float	   	Float	  	  否	        否	        否                |
 |num_double	  Double	  	否	        否	        否                |
 |num_smallint	Smallint    否	        否	        否                |
 |num_bigint	  BigInt	  	否	        否	        否                |
 |gender	   	  Tinyint	  	否	        否	        否                |
 |company	   	  Varchar	  	否	        否	        否                |
 |card_id	     	BigInt	  	否	        否	        否                |
 |crt_timestamp	TimeStamp	  否	        否	        否                |
 |crt_date	   	Date	  	  否	        否	        否                |
 |crt_datetime	DateTime	  否	        否	        否                |
 |crt_time	   	Time	  	  否	        否	        否                |
 |crt_year	   	Year	  	  否	        否	        否                |
 |--------------------------------------------------------------------|

 index
 |------------------------------|
 |id	  列名	  	   索引名		    |
 |928	  age	  	  age_idx		    |
 |929	  gender	 	gender_idx	  |
 |930	  card_id	 	cardid_idx	  |
 |------------------------------|
 * @version V1.0
 */
public class ExplainTest extends SqlTestBase {
  private String expected;

  @Before
  public void initExplain() {
    dropTable();
    createTable();
  }

  @After
  public void end() {
    dropTable();
    System.out.println("drop table success");
  }

  private void dropTable() {
    String sql = "DROP TABLE IF EXISTS blue_explain_test";
    execUpdate(sql, true);
    try {
      Thread.sleep(1000);
    } catch (Exception e) {
    }

  }

  private void createTable() {
    String sql = "CREATE TABLE IF NOT EXISTS `blue_explain_test` ( "
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
    execUpdate(sql, true);

  }

  @Test
  public void explainSelectSingle() {
    expected = "op=TableSource;task=proxy;op=TableSource;task=ds;";
    List<String> result = execQuery("explain select * from test.blue_explain_test");
    assertResult(expected, result);
  }

  @Test
  public void explainIndexUnique() {
    expected = "op=KeyGet;task=proxy;";
    // TODO parse projection
    List<String> result = execQuery("explain select id, age from test.blue_explain_test where id = 2");
    assertResult(expected, result);
  }

  @Test
  public void explainIndexScan() {
    expected = "op=TableSource;task=proxy;op=TableSource;task=ds;";
    // TODO why use tableScan not indexScan
    List<String> result = execQuery("explain select * from test.blue_explain_test where id > 2 and id <10");
    assertResult(expected, result);
  }

  @Test
  public void explainIndexScan1() {
    expected = "op=TableSource;task=proxy;op=TableSource;task=ds;";
    // TODO explain error
    List<String> result = execQuery("explain select id from test.blue_explain_test where (id > 2 and id <10) "
            + "or (id > 11 and id <20) or (id >30) ");
    assertResult(expected, result);
  }

  @Test
  public void explainIndexScan2() {
    expected = "op=IndexSource;task=proxy;op=IndexSource;task=ds;";
    List<String> result = execQuery("explain select age from test.blue_explain_test where (age > 2 and age <10) or (age > 15 and age <20)");
    assertResult(expected, result);
  }

  @Test
  public void explainIndexLookUp() {
    expected = "op=IndexLookup;task=proxy;op=TableSource;task=ds;op=IndexSource;task=ds;";
    List<String> result = execQuery("explain select * from test.blue_explain_test where age > 2 and age <10");
    assertResult(expected, result);
  }

  @Test
  public void explainSelectAgg() {
    expected = "op=Projection;task=proxy;op=TopN;task=proxy;op=Selection;task=proxy;op=Aggregation;task=proxy;"
            + "op=IndexSource;task=proxy;op=Aggregation;task=ds;op=IndexSource;task=ds;";
    // TODO limit can be pushdown
    List<String> result = execQuery("explain select sum(id) from test.blue_explain_test where card_id > 2 "
            + "group by card_id having count(card_id) >1 order by card_id limit 10 ");
    assertResult(expected, result);
  }

  @Test
  public void explainSelectAgg1() {
    expected = "op=Projection;task=proxy;op=Order;task=proxy;op=Selection;task=proxy;op=Aggregation;task=proxy;"
            + "op=IndexLookup;task=proxy;op=Aggregation;task=ds;op=TableSource;task=ds;op=IndexSource;task=ds;";
    List<String> result = execQuery("explain select sum(id), avg(age) from test.blue_explain_test where card_id > 2 "
            + "group by card_id having count(card_id) >1 order by card_id desc ");
    assertResult(expected, result);
  }

  @Test
  public void explainSelectAll() {
    expected = "op=Aggregation;task=proxy;op=TableSource;task=proxy;op=Aggregation;task=ds;op=TableSource;task=ds;";
    List<String> result = execQuery("explain select count(*) from test.blue_explain_test");
    assertResult(expected, result);
  }



  @Test
  public void explainSelect1() {
    expected = "op=Order;task=proxy;op=Aggregation;task=proxy;op=IndexLookup;task=proxy;op=Aggregation;task=ds;"
            + "op=Selection;task=ds;op=TableSource;task=ds;op=IndexSource;task=ds;";
    List<String> result = execQuery("explain select sum(num_double) as sum,avg(num_double) as avg,count(1) as cnt,max(num_double) as max,min(num_double) as min,country,gender,company "
            + "from test.blue_explain_test "
            + "where age between 25 and 45 and (country = 'china' or country = 'usa') "
            + "group by country,gender,company order by country,gender,company");
    assertResult(expected, result);
  }

  @Test
  public void explainDistinctSelect() {
    expected = "op=Aggregation;task=proxy;op=TableSource;task=proxy;op=Aggregation;task=ds;op=TableSource;task=ds;";
    List<String> result = execQuery("explain select distinct age from test.blue_explain_test");
    assertResult(expected, result);
  }

  @Ignore
  @Test
  public void analyzeSelectAll() {
    //TODO NOT support
    List<String> strings = execQuery("select analyze from test.blue_decimal_index");
    strings.forEach(en -> System.out.println(en));
  }


  @Test
  public void explainDel() {
    expected = "op=Delete;task=proxy;op=KeyGet;task=ds;";
    List<String> result = execQuery("explain delete from test.blue_explain_test where id =2");
    assertResult(expected, result);
  }

  @Test
  public void explainUpdate() {
    expected = "op=Update;task=proxy;op=KeyGet;task=ds;";
    List<String> result = execQuery("explain update test.blue_explain_test set id =3  where id =2");
    assertResult(expected, result);
  }

  @Test
  public void explainInsert() {
//    int size = 100;
//    String[] name = new String[]{ "Tom", "Jack", "Mary", "Suzy", "Kate", "Luke", "Bob", "Polly", "Alex", "Barke",
//            "Mara", "Martin" };
//    String[] country = new String[]{ "china", "japan", "usa", "korea", "england", "french", "brazil", "india" };
//    String[] company = new String[]{ "jingdong", "ibm", "oracle", "alibaba", "tencent", "sap", "facebook",
//            "huawei", "baidu", "mi" };
//    DateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
//    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
//    DateFormat timeFormat = new SimpleDateFormat("hh:mm:ss");
//    int idx = 0;
//    for (int batch = 0; batch < size/10; batch++) {
//      int onesize = 10;
//      String sql = "INSERT INTO baker02("
//              + "name, age, country, num_float, num_double, "
//              + "num_smallint, num_bigint, gender, company, card_id, "
//              + "crt_timestamp, crt_date, crt_datetime, crt_time, crt_year) VALUES";
//      for (int n = 0; n < onesize; n++) {
//        Date base = new Date();
//        Date date = new Date(base.getTime() + idx *24 * 3600 * 1000);
//        int year = idx % 200 + 1905;
//        sql += "('" + name[idx % name.length] + "-" + (idx % size) + "' , " + (idx % 100) + ", '" + country[idx %
//                country.length] + "', " + (idx % 90) + ", " + (idx % 70) + ", "
//                + (idx % 16) + ", " + (idx % 100) + "00000000, " + (idx % 2) + ", '" + company[idx % company
//                .length] + "', " + idx +
//                ", '" + dateTimeFormat.format(date) + "', '" + dateFormat.format(date) + "','" + dateTimeFormat
//                .format(date) + "','" + timeFormat.format(date) + "', " + year + ")";
//        if (n + 1 < 10) {
//          sql += ",";
//        }
//        idx++;
//      }
//      System.out.println(sql);
    //init data
    String insertSql = "INSERT INTO test.blue_explain_test (name, age, country, num_float, num_double, num_smallint, num_bigint, gender, "
            + "company, card_id, crt_timestamp, crt_date, crt_datetime, crt_time, crt_year)"
            + " VALUES('Bob-90' , 90, 'usa', 0, 20, 10, 9000000000, 0, 'jingdong', 90, '2019-12-08 12:01:05', "
            + "'2019-12-08','2019-12-08 12:01:05','12:01:05', 1995)" ;
    expected = "op=Insert;task=proxy;";
    List<String> result = execQuery("explain " + insertSql);
    assertResult(expected, result);
  }

  private void assertResult(String expected, List<String> results) {
    String actual = "";
    for (String s : results) {
      System.out.println(s);
      String[] strings = s.split(" ");
      actual += strings[0] + strings[2];
      System.out.println(actual);
    }
    Assert.assertEquals(expected, actual);
  }
}
