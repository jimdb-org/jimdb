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

import java.util.List;

import io.jimdb.test.mysql.SqlTestBase;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @version V1.0
 */
public class SelectTest extends SqlTestBase {

  private static String DBNAME = "test_select";
  private static String TABLENAME = "student";

  /**
   * student col: id(pk),name,age,class,score
   * index: name_idx(unique:true), age_idx(unique:false)
   */

  @BeforeClass
  public static void tearUp() {
    createDB();
    createTable();
    initTableData();
  }

  private static void createDB() {
    createCatalog(DBNAME);
    useCatalog(DBNAME);
  }

  private static void createTable() {
    String sql = "CREATE TABLE IF NOT EXISTS " + TABLENAME + " ("
            + "`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT, "
            + "`name` varchar(255) DEFAULT NULL, "
            + "`age` int(11) DEFAULT NULL, "
            + "`class` varchar(100) DEFAULT NULL, "
            + "`score` int DEFAULT NULL, "
            + "PRIMARY KEY (`id`),"
            + "INDEX age_idx (age), "
            + "INDEX name_idx (name) "
            + ") COMMENT 'REPLICA=1' ENGINE=MEMORY AUTO_INCREMENT=0 ";
    dropAndCreateTable(TABLENAME, sql);
  }

  private static void initTableData() {
    String sql = "insert into " + TABLENAME + " (name, age, class, score) values "
            + "('Tom', 28, 'one', 85), "
            + "('Jack', 32, 'two', 91), "
            + "('Mary', 27, 'one', 89), "
            + "('Suzy', 31, 'three', 92), "
            + "('Kate', 28, 'two', 99), "
            + "('Luke', 31, 'one', 94) ";

    execUpdate(sql, 6, true);

    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testSelectAll() {
    List<String> expected = expectedStr(new String[]{
            "id=1; name=Tom; age=28; class=one; score=85",
            "id=2; name=Jack; age=32; class=two; score=91",
            "id=3; name=Mary; age=27; class=one; score=89",
            "id=4; name=Suzy; age=31; class=three; score=92",
            "id=5; name=Kate; age=28; class=two; score=99",
            "id=6; name=Luke; age=31; class=one; score=94" });
    execQuery(String.format("select * from %s", TABLENAME), expected, false);
  }

  @Test
  public void testFix() {
    List<String> expected = expectedStr(new String[]{ "SUM(age)=31; id=6; name=Luke",
            "SUM(age)=28; id=1; name=Tom",
            "SUM(age)=32; id=2; name=Jack",
            "SUM(age)=27; id=3; name=Mary",
            "SUM(age)=31; id=4; name=Suzy",
            "SUM(age)=28; id=5; name=Kate"
    });
    execQuery(String.format("select sum(age),id,name from %s.%s group by name having min(score) >= 20 ", DBNAME, TABLENAME), expected);
  }

  /******************************************************************************
   Dual, Alias Test
   ******************************************************************************/
  @Test
  public void testSelectDual() {
    List<String> expected = expectedStr(new String[]{ "a=1; b=8; t=test" });
    execQuery("select 1 as a,2+6 as b,'test' as t", expected);
  }

  @Test
  public void testAlias() {
    List<String> expected = expectedStr(new String[]{ "id=1; name=Tom; age=28; class=one; score=85" });
    execQuery(String.format("select s.* from %s s limit 1", TABLENAME), expected);
  }

  @Test
  public void testSelect1() {
    List<String> expected = expectedStr(new String[]{ "name=Tom", "name=Jack", "name=Mary", "name=Suzy", "name=Kate", "name=Luke" });
    execQuery(String.format("select name from %s where 1=1 ", TABLENAME), expected, false);
  }

  /******************************************************************************
   Limit, Order by, Select TopN Test
   ******************************************************************************/

  @Test
  public void testLimit() {
    List<String> expected = expectedStr(new String[]{ "name=Tom", "name=Jack" });
    execQuery(String.format("select name from %s limit 2", TABLENAME), expected, false);
  }

  @Test
  public void testLimitStart() {
    List<String> expected = expectedStr(new String[]{ "id=6; age=31" });
    execQuery(String.format("select id, age from %s limit 5,5 order by id asc", TABLENAME), expected); //TODO ANSJ add order
  }

  @Test
  public void testLimit0() {
    List<String> expected = expectedStr(new String[]{});
    execQuery(String.format("select age from %s limit 2,0", TABLENAME), expected);
  }

  @Test
  public void testOrder() {
    List<String> expected = expectedStr(new String[]{ "name=Jack", "name=Kate", "name=Luke", "name=Mary", "name=Suzy", "name=Tom" });
    execQuery(String.format("select name from %s order by name", TABLENAME), expected);
  }

  @Test
  public void testOrderNum() {
    List<String> expected = expectedStr(new String[]{ "name=Mary; age=27", "name=Kate; age=28", "name=Tom; age=28", "name=Luke; age=31", "name=Suzy; age=31", "name=Jack; age=32" });
    execQuery(String.format("select name, age from %s order by 2,1", TABLENAME), expected);
  }

  @Test
  public void testOrderDesc() {
    List<String> expected = expectedStr(new String[]{ "name=Tom", "name=Suzy", "name=Mary", "name=Luke", "name=Kate", "name=Jack" });
    execQuery(String.format("select name from %s order by name desc", TABLENAME), expected);
  }

  @Test
  public void testOrderMultiple() {
    List<String> expected = expectedStr(new String[]{
            "name=Mary; age=27; score=89",
            "name=Tom; age=28; score=85",
            "name=Kate; age=28; score=99",
            "name=Suzy; age=31; score=92",
            "name=Luke; age=31; score=94",
            "name=Jack; age=32; score=91" });
    execQuery(String.format("select name, age, score  from %s order by age, score", TABLENAME), expected);
  }

  @Test
  public void testOrderMultipleHybrid() {
    List<String> expected = expectedStr(new String[]{
            "name=Mary; age=27; score=89",
            "name=Kate; age=28; score=99",
            "name=Tom; age=28; score=85",
            "name=Luke; age=31; score=94",
            "name=Suzy; age=31; score=92",
            "name=Jack; age=32; score=91" });
    execQuery(String.format("select name, age, score  from %s order by age, score desc", TABLENAME), expected);
  }

  @Test
  public void testTopN() {
    List<String> expected = expectedStr(new String[]{
            "name=Kate; age=28; score=99", "name=Tom; age=28; score=85" });
    execQuery(String.format("select name, age, score  from %s order by age, score desc limit 1,2", TABLENAME), expected);
  }

  @Test
  public void testSelectByName() {
    List<String> expected = expectedStr(new String[]{
            "name=Tom; age=28; score=85" });
    execQuery(String.format("select name, age, score from %s where name = 'Tom' and score=85", TABLENAME), expected);
  }

  @Test
  public void testSelectByNameAndScore() {
    List<String> expected = expectedStr(new String[]{
            "name=Tom; age=28; score=85" });
    execQuery(String.format("select name, age, score from %s where name = 'Tom' and score=85", TABLENAME), expected);
  }

  @Test
  public void testSelectByID() {
    List<String> expected = expectedStr(new String[]{
            "name=Jack; age=32; score=91" });
    execQuery(String.format("select name, age, score  from %s where id = 2", TABLENAME), expected);
  }

  @Test
  public void testSelectByID1() {
    List<String> expected = expectedStr(new String[]{
            "COUNT(1)=0; name=; SUM(age)=0" });
    execQuery(String.format("select count(1) , name, sum(age) from %s where id = 123 ", TABLENAME), expected);
  }

  @Test
  public void testSelectByAge() {
    List<String> expected = expectedStr(new String[]{
            "name=Tom; age=28", "name=Kate; age=28" });
    execQuery(String.format("select name, age from %s where age = 28", TABLENAME), expected);
  }

  @Test
  public void testSelectByOther() {
    List<String> expected = expectedStr(new String[]{
            "name=Kate; age=28; score=99" });
    execQuery(String.format("select name, age, score  from %s where score = 99", TABLENAME), expected);
  }

  @Test
  public void testProjection() {
    List<String> expected = expectedStr(new String[]{ "age + 1=28", "age + 1=29" });
    execQuery(String.format("select age + 1 from %s order by age limit 2", TABLENAME), expected, false);
  }

  @Test
  public void testProjectionAlias() {
    List<String> expected = expectedStr(new String[]{ "n=Mary; a=27" });
    execQuery(String.format("select name as n,age a from %s where age=27", TABLENAME), expected);
  }

  @Test
  public void testProjectionAddCol() {
    List<String> expected = expectedStr(new String[]{ "id=1; name=Tom; age=28; class=one; score=85; 1=1" });
    execQuery(String.format("select *,1 from %s limit 1", TABLENAME), expected);
  }

  /******************************************************************************
   +, -, *, /, %  Test
   ******************************************************************************/

  @Test
  public void testAdd() {
    List<String> expected = expectedStr(new String[]{ "1 + 2=3" });
    execQuery("select 1+2", expected);
  }

  @Test
  public void testSubtract() {
    List<String> expected = expectedStr(new String[]{ "2 - 1=1" });
    execQuery("select 2-1", expected);
  }

  @Test
  public void testMultiply() {
    List<String> expected = expectedStr(new String[]{ "2 * 3=6" });
    execQuery("select 2*3", expected);
  }

  @Test
  public void testDiv() {
    List<String> expected = expectedStr(new String[]{ "6 / 3=2.0000" });
    execQuery("select 6/3", expected);
  }

  @Test
  public void testMod() {
    List<String> expected = expectedStr(new String[]{ "15 % 7=1" });
    execQuery("select 15%7; ", expected);
  }

  /******************************************************************************
   =, >, <, >=, <=, !=, <>  Test
   ******************************************************************************/

  @Test
  public void testEqualString() {
    List<String> expected = expectedStr(new String[]{ "name=Suzy; age=31; class=three" });
    execQuery(String.format("select name,age,class from %s where class = 'three'", TABLENAME), expected);
  }

  @Test
  public void testEqualInt() {
    List<String> expected = expectedStr(new String[]{ "name=Mary; age=27" });
    execQuery(String.format("select name,age from %s where age = 27", TABLENAME), expected);
  }

  @Test
  //Abnormal results (range)   Assertion `result.first <= result.second' failed.
  public void testGreaterAge() {
    List<String> expected = expectedStr(new String[]{ "name=Suzy; age=31", "name=Luke; age=31", "name=Jack; age=32" });
    execQuery(String.format("select name,age from %s where age > 30 ", TABLENAME), expected, false);
  }

  @Test
  public void testGreaterScore() {
    List<String> expected = expectedStr(new String[]{
            "name=Jack; age=32; score=91",
            "name=Suzy; age=31; score=92",
            "name=Kate; age=28; score=99",
            "name=Luke; age=31; score=94" });
    execQuery(String.format("select name,age,score from %s where score>90 ", TABLENAME), expected, false);
  }

  @Test
  //Abnormal   Assertion `result.first <= result.second' failed.
  public void testGreaterEqualAge() {
    List<String> expected = expectedStr(new String[]{ "name=Suzy; age=31", "name=Luke; age=31", "name=Jack; age=32" });
    execQuery(String.format("select name,age from %s where age >= 31 ", TABLENAME), expected, false);
  }

  @Test
  public void testGreaterEqualScore() {
    List<String> expected = expectedStr(new String[]{ "name=Kate; age=28; score=99", "name=Luke; age=31; score=94" });
    execQuery(String.format("select name,age,score from %s where score >= 94 ", TABLENAME), expected, false);
  }

  @Test
  public void testLessAge() {
    List<String> expected = expectedStr(new String[]{ "name=Mary; age=27", "name=Tom; age=28", "name=Kate; age=28" });
    execQuery(String.format("select name,age from %s where age < 31 ", TABLENAME), expected);
  }

  @Test
  public void testLessScore() {
    List<String> expected = expectedStr(new String[]{ "name=Tom; age=28; score=85", "name=Mary; age=27; score=89" });
    execQuery(String.format("select name,age,score from %s where score < 90 ", TABLENAME), expected);
  }

  @Test
  public void testLessEqualAge() {
    List<String> expected = expectedStr(new String[]{ "name=Tom; age=28", "name=Mary; age=27" });
    execQuery(String.format("select name,age from %s where age <= 31 order by id limit 2 ", TABLENAME), expected);
  }

  @Test
  public void testLessEqualScore() {
    List<String> expected = expectedStr(new String[]{ "name=Tom; age=28; score=85", "name=Mary; age=27; score=89" });
    execQuery(String.format("select name,age,score from %s where score <= 89 ", TABLENAME), expected);
  }

  @Test
  public void testUnequal() {
    List<String> expected = expectedStr(new String[]{ "name=Jack; class=two", "name=Suzy; class=three", "name=Kate; class=two" });
    execQuery(String.format("select name,class from %s where class != 'one'", TABLENAME), expected);
  }

  /******************************************************************************
   And, Or, NOT, IN, NOT IN, IS NULL, IS NOT NULL Test
   ******************************************************************************/

  @Test
  //Abnormal  (ds result error)
  public void testAnd() {
    List<String> expected = expectedStr(new String[]{ "name=Tom; age=28" });
    execQuery(String.format("select name,age from %s where name = 'Tom' and age = 28 ", TABLENAME), expected);
  }

  @Test
  //Abnormal
  public void testAnd2() {
    List<String> expected = expectedStr(new String[]{ "name=Tom; age=28" });
    execQuery(String.format("select name,age from %s where name = 'Tom' && age = 28 ", TABLENAME), expected);
  }

  @Test
  //Abnormal   Assertion `result.first <= result.second' failed.
  public void testAndDiff() {
    List<String> expected = expectedStr(new String[]{ "name=Mary; age=27; score=89" });
    execQuery(String.format("select name,age,score from %s where age <=28 and score = 89 ", TABLENAME), expected);
  }

  @Test
  public void testOr() {
    List<String> expected = expectedStr(new String[]{ "name=Tom; age=28", "name=Mary; age=27" });
    execQuery(String.format("select name,age from %s where name = 'Tom' or age = 27 ", TABLENAME), expected);
  }

  @Test
  public void testOr2() {
    List<String> expected = expectedStr(new String[]{ "name=Tom; age=28", "name=Mary; age=27" });
    execQuery(String.format("select name,age from %s where name = 'Tom' || age = 27 ", TABLENAME), expected);
  }

  @Test
  public void testOr3() {
    List<String> expected = expectedStr(new String[]{ "name=Mary; age=27", "name=Tom; age=28", "name=Kate; age=28" });
    execQuery(String.format("select name,age from %s where age = 28 or age = 27 ", TABLENAME), expected);
  }

  @Test
  public void testOrUniq() {
    List<String> expected = expectedStr(new String[]{ "name=Tom; age=28; score=85", "name=Kate; age=28; score=99" });
    execQuery(String.format("select name,age,score from %s where name = 'Tom' or score > 95 ", TABLENAME), expected);
  }

  @Test
  public void testOrIndex() {
    List<String> expected = expectedStr(new String[]{ "name=Tom; age=28; score=85", "name=Mary; age=27; score=89", "name=Kate; age=28; score=99" });
    execQuery(String.format("select name,age,score from %s where age <= 28 or score > 95 ", TABLENAME), expected);
  }

  @Test
  //Abnormal
  public void testAndOrPar() {
    List<String> expected = expectedStr(new String[]{ "name=Suzy; age=31; score=92" });
    execQuery(String.format("SELECT name,age,score FROM %s WHERE (age=31 OR score=89) AND NAME='Suzy'", TABLENAME), expected);
  }

  @Test
  public void testAndOr() {
    List<String> expected = expectedStr(new String[]{ "name=Tom; age=28", "name=Jack; age=32", "name=Suzy; age=31", "name=Luke; age=31" });
    execQuery(String.format("select name,age from %s where age>=31 and age <= 35 or name = 'Tom' ", TABLENAME), expected, false);
  }

  @Test
  public void testRangeAnd() {
    List<String> expected = expectedStr(new String[]{ "name=Suzy; age=31", "name=Luke; age=31", "name=Jack; age=32" });
    execQuery(String.format("select name,age from %s where age>=31 and age <= 35 ", TABLENAME), expected, false);
  }

  @Test
  public void testBetweenAnd() {
    List<String> expected = expectedStr(new String[]{ "name=Jack; age=32", "name=Mary; age=27", "name=Suzy; age=31" });
    execQuery(String.format("select name,age from %s where id between 2 and 4 ", TABLENAME), expected);
  }

  @Test
  public void testRangeOr() {
    List<String> expected = expectedStr(new String[]{ "name=Mary; age=27", "name=Tom; age=28", "name=Kate; age=28" });
    execQuery(String.format("select name,age from %s where age<30 or age>35 ", TABLENAME), expected);
  }

  @Test
  public void testSelectXor() {
    List<String> expected = expectedStr(new String[]{ "1 ^ 0=1" });
    execQuery("SELECT 1 ^ 0;", expected);
  }

  /******************************************************************************
   count, sum  Agg Test
   ******************************************************************************/

  @Test
  public void testSelectCount1() {
    List<String> expected = expectedStr(new String[]{ "COUNT(1)=6" });
    execQuery(String.format("select count(1) from %s ", TABLENAME), expected);
  }

  @Test
  public void testSelectCountXing() {
    List<String> expected = expectedStr(new String[]{ "COUNT(*)=6" });
    execQuery(String.format("select count(*) from %s ", TABLENAME), expected);
  }

  @Test
  public void testSelectCountCol() {
    List<String> expected = expectedStr(new String[]{ "COUNT(name)=6" });
    execQuery(String.format("select count(name) from %s ", TABLENAME), expected);
  }

  @Test
  public void testSelectCountP() {
    List<String> expected = expectedStr(new String[]{ "COUNT(1 + 1)=6" });
    execQuery(String.format("select count(1+1) from %s ", TABLENAME), expected);
  }

  @Test
  public void testSum() {
    List<String> expected = expectedStr(new String[]{ "'sum_score'=550" });
    execQuery(String.format("select sum(score) as 'sum_score' from %s ", TABLENAME), expected);
  }

  @Test
  public void testDistinct() {
    List<String> expected = expectedStr(new String[]{ "age=31", "age=27", "age=32", "age=28" });
    execQuery(String.format("select distinct age from %s ", TABLENAME), expected, false);
  }

  /******************************************************************************
   PK, index, KeyGet  Test
   ******************************************************************************/

  @Test
  public void testSelectPK() {
    List<String> expected = expectedStr(new String[]{ "id=2; name=Jack" });
    execQuery(String.format("select id,name from %s where id = 2 ", TABLENAME), expected);
  }

  @Test
  public void testPKGtLtOr() {
    List<String> expected = expectedStr(new String[]{ "id=1; name=Tom", "id=6; name=Luke" });
    execQuery(String.format("select id,name from %s where id < 2 or id > 5 ", TABLENAME), expected);
  }

  @Test
  public void testPKGeLeOr() {
    List<String> expected = expectedStr(new String[]{ "id=1; name=Tom", "id=6; name=Luke" });
    execQuery(String.format("select id,name from %s where id <= 1 or id >= 6 ", TABLENAME), expected);
  }

  @Test
  public void testPKGtLtAnd() {
    List<String> expected = expectedStr(new String[]{ "id=4; name=Suzy" });
    execQuery(String.format("select id,name from %s where id < 5 and id > 3 ", TABLENAME), expected);
  }

  @Test
  public void testPKGeLeAnd() {
    List<String> expected = expectedStr(new String[]{ "id=2; name=Jack", "id=3; name=Mary", "id=4; name=Suzy" });
    execQuery(String.format("select id,name from %s where id <= 4 and id >= 2 ", TABLENAME), expected);
  }

  @Test
  public void testKeyGet() {
    List<String> expected = expectedStr(new String[]{ "id=2; name=Jack" });
    execQuery(String.format("select id,name from %s where name = 'Jack' ", TABLENAME), expected);
  }

  @Test
  public void testIndexAgeGtLtOr() {
    List<String> expected = expectedStr(new String[]{ "name=Mary; age=27", "name=Jack; age=32" });
    execQuery(String.format("select name,age from %s where age > 31 or age < 28 ", TABLENAME), expected);
  }

  @Test
  public void testIndexAgeGeLeOr() {
    List<String> expected = expectedStr(new String[]{ "name=Mary; age=27", "name=Jack; age=32" });
    execQuery(String.format("select name,age from %s where age >= 32 or age <= 27 ", TABLENAME), expected);
  }

  @Test
  public void testIndexAgeGtLtAnd() {
    List<String> expected = expectedStr(new String[]{ "name=Tom; age=28", "name=Kate; age=28" });
    execQuery(String.format("select name,age from %s where age < 31 and age > 27 ", TABLENAME), expected);
  }

  @Test
  public void testIndexAgeGeLeAnd() {
    List<String> expected = expectedStr(new String[]{ "name=Tom; age=28", "name=Kate; age=28", "name=Suzy; age=31", "name=Luke; age=31" });
    execQuery(String.format("select name,age from %s where age <= 31 and age >= 28 ", TABLENAME), expected, false);
  }

  @Test
  public void testIndexAge() {
    List<String> expected = expectedStr(new String[]{ "name=Suzy; age=31", "name=Luke; age=31" });
    execQuery(String.format("select name,age from %s where age = 31 ", TABLENAME), expected, false);
  }

  /******************************************************************************
   Temporarily unsupported
   ******************************************************************************/

  @Test
  @Ignore //Temporarily unsupported
  public void testCastVarcherToInt() {
    List<String> expected = expectedStr(new String[]{ "name=Jack" });
    execQuery(String.format("select name from %s where order by CAST(name as SIGNED);", TABLENAME), expected);
  }

  @Test
  @Ignore //Temporarily unsupported
  public void testCastToInt() {
    List<String> expected = expectedStr(new String[]{ "id=1" });
    execQuery(String.format("select cast(id as signed) from %s limit 1 ", TABLENAME), expected);
  }

  @Test
  @Ignore  //Temporarily unsupported
  public void testISNULL() {
    List<String> expected = expectedStr(new String[]{ "=1" });
    execQuery("select null is NULL;", expected);
  }

  @Test
  @Ignore  //Temporarily unsupported
  public void testISNOTNULL() {
    List<String> expected = expectedStr(new String[]{ "=0" });
    execQuery("select null IS NOT NULL", expected);
  }

  @Test
  @Ignore  //Temporarily unsupported
  public void testISNULL2() {
    List<String> expected = expectedStr(new String[]{ "=1" });
    execQuery(String.format("select name from %s where score is null", TABLENAME), expected);
  }

  @Test
  @Ignore //Temporarily unsupported
  public void testISNOTNULL2() {
    List<String> expected = expectedStr(new String[]{ "=0" });
    execQuery(String.format("select name from %s where score is not null", TABLENAME), expected);
  }

  @Test
  @Ignore  //Temporarily unsupported
  public void testBetweenOr() {
    List<String> expected = expectedStr(new String[]{ "name=Jack; age=32", "name=Mary; age=27", "name=Suzy; age=31" });
    execQuery(String.format("select name,age from %s where id not between 2 and 5 ", TABLENAME), expected);
  }

  @Test
  @Ignore //Temporarily unsupported
  public void testNot() {
    List<String> expected = expectedStr(new String[]{ "name=Tom; age=28", "name=Jack; age=32", "name=Suzy; age=31", "name=Luke; age=31" });
    execQuery(String.format("select name,age from %s where not name='Tom' ", TABLENAME), expected);
  }

  @Test
  @Ignore //Temporarily unsupported
  public void testSelectIn() {
    List<String> expected = expectedStr(new String[]{ "name=Suzy; score=92", "name=Luke; score=94" });
    execQuery(String.format("select name,score from %s where score IN (91,99)", TABLENAME), expected);
  }

  @Test
  @Ignore //Temporarily unsupported
  public void testSelectNotIn() {
    List<String> expected = expectedStr(new String[]{ "name=Tom; score=85", "name=Mary; score=89" });
    execQuery(String.format("select name,score from %s where score NOT IN (90,100)", TABLENAME), expected);
  }

  @Test
  @Ignore //Temporarily unsupported
  public void testUnequal2() {
    List<String> expected = expectedStr(new String[]{ "name=Jack; class=two", "name=Suzy; class=three", "name=Kate; class=two" });
    execQuery(String.format("select name,class from %s where class <> 'one'", TABLENAME), expected);
  }

  @Test
  @Ignore //Temporarily unsupported
  public void testLimitEnd() {
    List<String> expected = expectedStr(new String[]{ "age=28", "age=31" });
    execQuery(String.format("select age from %s limit 4,-1", TABLENAME), expected);
  }
}
