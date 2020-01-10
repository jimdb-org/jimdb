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

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @version V1.0
 */
public class SelectTest extends SqlTestBase {

  private static String DBNAME = "test";
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
    execQuery("select * from student", expected, false);
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
    execQuery("select sum(age),id,name from test.student group by name having min(score) >= 20 ", expected);
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
    execQuery("select s.* from student s limit 1", expected);
  }

  @Test
  public void testSelect1() {
    List<String> expected = expectedStr(new String[]{ "name=Tom", "name=Jack", "name=Mary", "name=Suzy", "name=Kate", "name=Luke" });
    execQuery("select name from student where 1=1 ", expected, false);
  }

  /******************************************************************************
   Limit, Order by, Select TopN Test
   ******************************************************************************/

  @Test
  public void testLimit() {
    List<String> expected = expectedStr(new String[]{ "name=Tom", "name=Jack" });
    execQuery("select name from student limit 2", expected, false);
  }

  @Test
  public void testLimitStart() {
    List<String> expected = expectedStr(new String[]{ "id=6; age=31" });
    execQuery("select id, age from student limit 5,5 order by id asc", expected); //TODO ANSJ add order
  }

  @Test
  public void testLimit0() {
    List<String> expected = expectedStr(new String[]{});
    execQuery("select age from student limit 2,0", expected);
  }

  @Test
  public void testOrder() {
    List<String> expected = expectedStr(new String[]{ "name=Jack", "name=Kate", "name=Luke", "name=Mary", "name=Suzy", "name=Tom" });
    execQuery("select name from student order by name", expected);
  }

  @Test
  public void testOrderNum() {
    List<String> expected = expectedStr(new String[]{ "name=Mary; age=27", "name=Kate; age=28", "name=Tom; age=28", "name=Luke; age=31", "name=Suzy; age=31", "name=Jack; age=32" });
    execQuery("select name, age from student order by 2,1", expected);
  }

  @Test
  public void testOrderDesc() {
    List<String> expected = expectedStr(new String[]{ "name=Tom", "name=Suzy", "name=Mary", "name=Luke", "name=Kate", "name=Jack" });
    execQuery("select name from student order by name desc", expected);
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
    execQuery("select name, age, score  from student order by age, score", expected);
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
    execQuery("select name, age, score  from student order by age, score desc", expected);
  }

  @Test
  public void testTopN() {
    List<String> expected = expectedStr(new String[]{
            "name=Kate; age=28; score=99", "name=Tom; age=28; score=85" });
    execQuery("select name, age, score  from student order by age, score desc limit 1,2", expected);
  }

  @Test
  public void testSelectByName() {
    List<String> expected = expectedStr(new String[]{
            "name=Tom; age=28; score=85" });
    execQuery("select name, age, score from student where name = 'Tom' and score=85", expected);
  }

  @Test
  public void testSelectByNameAndScore() {
    List<String> expected = expectedStr(new String[]{
            "name=Tom; age=28; score=85" });
    execQuery("select name, age, score from student where name = 'Tom' and score=85", expected);
  }

  @Test
  public void testSelectByID() {
    List<String> expected = expectedStr(new String[]{
            "name=Jack; age=32; score=91" });
    execQuery("select name, age, score  from student where id = 2", expected);
  }


  @Test
  public void testSelectByID1() {
    List<String> expected = expectedStr(new String[]{
            "COUNT(1)=0; name=; SUM(age)=0" });
    execQuery("select count(1) , name, sum(age) from student where id = 123 ", expected);
  }



  @Test
  public void testSelectByAge() {
    List<String> expected = expectedStr(new String[]{
            "name=Tom; age=28", "name=Kate; age=28" });
    execQuery("select name, age from student where age = 28", expected);
  }

  @Test
  public void testSelectByOther() {
    List<String> expected = expectedStr(new String[]{
            "name=Kate; age=28; score=99" });
    execQuery("select name, age, score  from student where score = 99", expected);
  }

  @Test
  public void testProjection() {
    List<String> expected = expectedStr(new String[]{ "age + 1=28", "age + 1=29" });
    execQuery("select age + 1 from student order by age limit 2", expected, false);
  }

  @Test
  public void testProjectionAlias() {
    List<String> expected = expectedStr(new String[]{ "n=Mary; a=27" });
    execQuery("select name as n,age a from student where age=27", expected);
  }

  @Test
  public void testProjectionAddCol() {
    List<String> expected = expectedStr(new String[]{ "id=1; name=Tom; age=28; class=one; score=85; 1=1" });
    execQuery("select *,1 from student limit 1", expected);
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
    List<String> expected = expectedStr(new String[]{ "6 / 3=2" });
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
    execQuery("select name,age,class from student where class = 'three'", expected);
  }

  @Test
  public void testEqualInt() {
    List<String> expected = expectedStr(new String[]{ "name=Mary; age=27" });
    execQuery("select name,age from student where age = 27", expected);
  }

  @Test
  //Abnormal results (range)   Assertion `result.first <= result.second' failed.
  public void testGreaterAge() {
    List<String> expected = expectedStr(new String[]{ "name=Suzy; age=31", "name=Luke; age=31", "name=Jack; age=32" });
    execQuery("select name,age from student where age > 30 ", expected, false);
  }

  @Test
  public void testGreaterScore() {
    List<String> expected = expectedStr(new String[]{
            "name=Jack; age=32; score=91",
            "name=Suzy; age=31; score=92",
            "name=Kate; age=28; score=99",
            "name=Luke; age=31; score=94" });
    execQuery("select name,age,score from student where score>90 ", expected, false);
  }

  @Test
  //Abnormal   Assertion `result.first <= result.second' failed.
  public void testGreaterEqualAge() {
    List<String> expected = expectedStr(new String[]{ "name=Suzy; age=31", "name=Luke; age=31", "name=Jack; age=32" });
    execQuery("select name,age from student where age >= 31 ", expected, false);
  }

  @Test
  public void testGreaterEqualScore() {
    List<String> expected = expectedStr(new String[]{ "name=Kate; age=28; score=99", "name=Luke; age=31; score=94" });
    execQuery("select name,age,score from student where score >= 94 ", expected, false);
  }

  @Test
  public void testLessAge() {
    List<String> expected = expectedStr(new String[]{ "name=Mary; age=27", "name=Tom; age=28", "name=Kate; age=28" });
    execQuery("select name,age from student where age < 31 ", expected);
  }

  @Test
  public void testLessScore() {
    List<String> expected = expectedStr(new String[]{ "name=Tom; age=28; score=85", "name=Mary; age=27; score=89" });
    execQuery("select name,age,score from student where score < 90 ", expected);
  }

  @Test
  public void testLessEqualAge() {
    List<String> expected = expectedStr(new String[]{ "name=Tom; age=28", "name=Mary; age=27" });
    execQuery("select name,age from student where age <= 31 order by id limit 2 ", expected);
  }

  @Test
  public void testLessEqualScore() {
    List<String> expected = expectedStr(new String[]{ "name=Tom; age=28; score=85", "name=Mary; age=27; score=89" });
    execQuery("select name,age,score from student where score <= 89 ", expected);
  }

  @Test
  public void testUnequal() {
    List<String> expected = expectedStr(new String[]{ "name=Jack; class=two", "name=Suzy; class=three", "name=Kate; class=two" });
    execQuery("select name,class from student where class != 'one'", expected);
  }

  /******************************************************************************
   And, Or, NOT, IN, NOT IN, IS NULL, IS NOT NULL Test
   ******************************************************************************/

  @Test
  //Abnormal  (ds result error)
  public void testAnd() {
    List<String> expected = expectedStr(new String[]{ "name=Tom; age=28" });
    execQuery("select name,age from student where name = 'Tom' and age = 28 ", expected);
  }

  @Test
  //Abnormal
  public void testAnd2() {
    List<String> expected = expectedStr(new String[]{ "name=Tom; age=28" });
    execQuery("select name,age from student where name = 'Tom' && age = 28 ", expected);
  }

  @Test
  //Abnormal   Assertion `result.first <= result.second' failed.
  public void testAndDiff() {
    List<String> expected = expectedStr(new String[]{ "name=Mary; age=27; score=89" });
    execQuery("select name,age,score from student where age <=28 and score = 89 ", expected);
  }

  @Test
  public void testOr() {
    List<String> expected = expectedStr(new String[]{ "name=Tom; age=28", "name=Mary; age=27" });
    execQuery("select name,age from student where name = 'Tom' or age = 27 ", expected);
  }

  @Test
  public void testOr2() {
    List<String> expected = expectedStr(new String[]{ "name=Tom; age=28", "name=Mary; age=27" });
    execQuery("select name,age from student where name = 'Tom' || age = 27 ", expected);
  }

  @Test
  public void testOr3() {
    List<String> expected = expectedStr(new String[]{ "name=Mary; age=27", "name=Tom; age=28", "name=Kate; age=28" });
    execQuery("select name,age from student where age = 28 or age = 27 ", expected);
  }

  @Test
  public void testOrUniq() {
    List<String> expected = expectedStr(new String[]{ "name=Tom; age=28; score=85", "name=Kate; age=28; score=99" });
    execQuery("select name,age,score from student where name = 'Tom' or score > 95 ", expected);
  }

  @Test
  public void testOrIndex() {
    List<String> expected = expectedStr(new String[]{ "name=Tom; age=28; score=85", "name=Mary; age=27; score=89", "name=Kate; age=28; score=99" });
    execQuery("select name,age,score from student where age <= 28 or score > 95 ", expected);
  }

  @Test
  //Abnormal
  public void testAndOrPar() {
    List<String> expected = expectedStr(new String[]{ "name=Suzy; age=31; score=92" });
    execQuery("SELECT name,age,score FROM student WHERE (age=31 OR score=89) AND NAME='Suzy'", expected);
  }

  @Test
  public void testAndOr() {
    List<String> expected = expectedStr(new String[]{ "name=Tom; age=28", "name=Jack; age=32", "name=Suzy; age=31", "name=Luke; age=31" });
    execQuery("select name,age from student where age>=31 and age <= 35 or name = 'Tom' ", expected, false);
  }

  @Test
  public void testRangeAnd() {
    List<String> expected = expectedStr(new String[]{ "name=Suzy; age=31", "name=Luke; age=31", "name=Jack; age=32" });
    execQuery("select name,age from student where age>=31 and age <= 35 ", expected, false);
  }

  @Test
  public void testBetweenAnd() {
    List<String> expected = expectedStr(new String[]{ "name=Jack; age=32", "name=Mary; age=27", "name=Suzy; age=31" });
    execQuery("select name,age from student where id between 2 and 4 ", expected);
  }

  @Test
  public void testRangeOr() {
    List<String> expected = expectedStr(new String[]{ "name=Mary; age=27", "name=Tom; age=28", "name=Kate; age=28" });
    execQuery("select name,age from student where age<30 or age>35 ", expected);
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
    execQuery("select count(1) from student ", expected);
  }

  @Test
  public void testSelectCountXing() {
    List<String> expected = expectedStr(new String[]{ "COUNT(*)=6" });
    execQuery("select count(*) from student ", expected);
  }

  @Test
  public void testSelectCountCol() {
    List<String> expected = expectedStr(new String[]{ "COUNT(name)=6" });
    execQuery("select count(name) from student ", expected);
  }

  @Test
  public void testSelectCountP() {
    List<String> expected = expectedStr(new String[]{ "COUNT(1 + 1)=6" });
    execQuery("select count(1+1) from student ", expected);
  }

  @Test
  public void testSum() {
    List<String> expected = expectedStr(new String[]{ "'sum_score'=550" });
    execQuery("select sum(score) as 'sum_score' from student ", expected);
  }

  @Test
  public void testDistinct() {
    List<String> expected = expectedStr(new String[]{ "age=31", "age=27", "age=32", "age=28" });
    execQuery("select distinct age from student ", expected, false);
  }

  /******************************************************************************
   PK, index, KeyGet  Test
   ******************************************************************************/

  @Test
  public void testSelectPK() {
    List<String> expected = expectedStr(new String[]{ "id=2; name=Jack" });
    execQuery("select id,name from student where id = 2 ", expected);
  }

  @Test
  public void testPKGtLtOr() {
    List<String> expected = expectedStr(new String[]{ "id=1; name=Tom", "id=6; name=Luke" });
    execQuery("select id,name from student where id < 2 or id > 5 ", expected);
  }

  @Test
  public void testPKGeLeOr() {
    List<String> expected = expectedStr(new String[]{ "id=1; name=Tom", "id=6; name=Luke" });
    execQuery("select id,name from student where id <= 1 or id >= 6 ", expected);
  }

  @Test
  public void testPKGtLtAnd() {
    List<String> expected = expectedStr(new String[]{ "id=4; name=Suzy" });
    execQuery("select id,name from student where id < 5 and id > 3 ", expected);
  }

  @Test
  public void testPKGeLeAnd() {
    List<String> expected = expectedStr(new String[]{ "id=2; name=Jack", "id=3; name=Mary", "id=4; name=Suzy" });
    execQuery("select id,name from student where id <= 4 and id >= 2 ", expected);
  }

  @Test
  public void testKeyGet() {
    List<String> expected = expectedStr(new String[]{ "id=2; name=Jack" });
    execQuery("select id,name from student where name = 'Jack' ", expected);
  }

  @Test
  public void testIndexAgeGtLtOr() {
    List<String> expected = expectedStr(new String[]{ "name=Mary; age=27", "name=Jack; age=32" });
    execQuery("select name,age from student where age > 31 or age < 28 ", expected);
  }

  @Test
  public void testIndexAgeGeLeOr() {
    List<String> expected = expectedStr(new String[]{ "name=Mary; age=27", "name=Jack; age=32" });
    execQuery("select name,age from student where age >= 32 or age <= 27 ", expected);
  }

  @Test
  public void testIndexAgeGtLtAnd() {
    List<String> expected = expectedStr(new String[]{ "name=Tom; age=28", "name=Kate; age=28" });
    execQuery("select name,age from student where age < 31 and age > 27 ", expected);
  }

  @Test
  public void testIndexAgeGeLeAnd() {
    List<String> expected = expectedStr(new String[]{ "name=Tom; age=28", "name=Kate; age=28", "name=Suzy; age=31", "name=Luke; age=31" });
    execQuery("select name,age from student where age <= 31 and age >= 28 ", expected, false);
  }

  @Test
  public void testIndexAge() {
    List<String> expected = expectedStr(new String[]{ "name=Suzy; age=31", "name=Luke; age=31" });
    execQuery("select name,age from student where age = 31 ", expected, false);
  }

  /******************************************************************************
   Temporarily unsupported
   ******************************************************************************/

  @Test
  @Ignore //Temporarily unsupported
  public void testCastVarcherToInt() {
    List<String> expected = expectedStr(new String[]{ "name=Jack" });
    execQuery("select name from student where order by CAST(name as SIGNED);", expected);
  }

  @Test
  @Ignore //Temporarily unsupported
  public void testCastToInt() {
    List<String> expected = expectedStr(new String[]{ "id=1" });
    execQuery("select cast(id as signed) from student limit 1 ", expected);
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
    execQuery("select name from student where score is null", expected);
  }

  @Test
  @Ignore //Temporarily unsupported
  public void testISNOTNULL2() {
    List<String> expected = expectedStr(new String[]{ "=0" });
    execQuery("select name from student where score is not null", expected);
  }

  @Test
  @Ignore  //Temporarily unsupported
  public void testBetweenOr() {
    List<String> expected = expectedStr(new String[]{ "name=Jack; age=32", "name=Mary; age=27", "name=Suzy; age=31" });
    execQuery("select name,age from student where id not between 2 and 5 ", expected);
  }

  @Test
  @Ignore //Temporarily unsupported
  public void testNot() {
    List<String> expected = expectedStr(new String[]{ "name=Tom; age=28", "name=Jack; age=32", "name=Suzy; age=31", "name=Luke; age=31" });
    execQuery("select name,age from student where not name='Tom' ", expected);
  }

  @Test
  @Ignore //Temporarily unsupported
  public void testSelectIn() {
    List<String> expected = expectedStr(new String[]{ "name=Suzy; score=92", "name=Luke; score=94" });
    execQuery("select name,score from student where score IN (91,99)", expected);
  }

  @Test
  @Ignore //Temporarily unsupported
  public void testSelectNotIn() {
    List<String> expected = expectedStr(new String[]{ "name=Tom; score=85", "name=Mary; score=89" });
    execQuery("select name,score from student where score NOT IN (90,100)", expected);
  }

  @Test
  @Ignore //Temporarily unsupported
  public void testUnequal2() {
    List<String> expected = expectedStr(new String[]{ "name=Jack; class=two", "name=Suzy; class=three", "name=Kate; class=two" });
    execQuery("select name,class from student where class <> 'one'", expected);
  }

  @Test
  @Ignore //Temporarily unsupported
  public void testLimitEnd() {
    List<String> expected = expectedStr(new String[]{ "age=28", "age=31" });
    execQuery("select age from student limit 4,-1", expected);
  }

  @Test
  public void testKeyUpdateAlias() {
    try {
      Thread.sleep(1000);
      execUpdate("update student set age = 28 where age = 28", 2, true);
    } catch (Exception ex) {
    }
  }

}
