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

import java.util.ArrayList;
import java.util.List;

import io.jimdb.test.mysql.SqlTestBase;

import org.junit.BeforeClass;
import org.junit.Test;

/**
 * PreparedStmtTest
 *
 * @since 2019/9/23
 */
public class PreparedStmtTest extends SqlTestBase {

  //////////////////// sample data of table         //////////////////////////////
  //////////////////// PK : id                     //////////////////////////////
  //////////////////// Index : name, unique       //////////////////////////////
  //////////////////// Index : age,  not unique  //////////////////////////////

  ////////    id=1; name=Tom; age=28; class=one; score=85,        ////////
  ////////    id=2; name=Jack; age=32; class=two; score=91,      ////////
  ////////    id=3; name=Mary; age=27; class=one; score=89,     ////////
  ////////    id=4; name=Suzy; age=31; class=three; score=92,  ////////
  ////////    id=5; name=Kate; age=28; class=two; score=99,   ////////
  ////////    id=6; name=Luke; age=31; class=one; score=94   ////////


  private static String DBNAME = "test";
  private static String TABLENAME = "student";
  /**
   * student col: id(pk),name,age,class,score
   * index: name_idx(unique:true), age_idx(unique:false)
   */

//  @Before
//  public void setUp() {
//    execUpdate("INSERT INTO student VALUES(1, 'Tom', 28, 'one', 85),(2, 'Jack', 32, 'two', 91),(3, 'Mary', 27, 'one', 89),(4, 'Suzy', 31, 'three', 92), (5, 'Kate', 28, 'two', 99), (6, 'Luke', 31, 'one', 94)", 6, true);
//  }

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
  public void testPrepareUpdate() {
    execPrepareUpdate("update student set name = ? where id = ? and age = 10", 0, "Toomm", 1262);
  }

  @Test
  public void testPrepareSelect() {
    List<String> expected = expectedStr(new String[]{ "class=one; name=Tom" });
//    execPrepareQuery("select class,name from student where age < ? order by id limit 1", expected, 30);
    execQuery("select class,name from student where age < 30 order by id limit 1", expected);
  }

  @Test
  public void testAllPrepareQueryCases() {
    List<PrepareStruct> prepares = new ArrayList<>();

//    prepares.add(new PrepareStruct("select ? from dual",
//            expectedStr(new String[]{"1=1"}),
//            new Object[]{1}));

    prepares.add(new PrepareStruct("select * from student where score = ?",
            expectedStr(new String[]{ "id=5; name=Kate; age=28; class=two; score=99" }),
            new Object[]{ 99 }));

    prepares.add(new PrepareStruct("select * from student where score = age + ?",
            expectedStr(new String[]{ "id=3; name=Mary; age=27; class=one; score=89" }),
            new Object[]{ 62 }));

    prepares.add(new PrepareStruct("select name from student where score > ?",
            expectedStr(new String[]{ "name=Jack", "name=Suzy", "name=Kate", "name=Luke" }),
            new Object[]{ 90 }));

    prepares.add(new PrepareStruct("select name,class from student where age < ? order by id",
            expectedStr(new String[]{ "name=Tom; class=one", "name=Mary; class=one", "name=Kate; class=two" }),
            new Object[]{ 30 }));

    prepares.add(new PrepareStruct("select name,class from student where age < ? and score > ?",
            expectedStr(new String[]{ "name=Kate; class=two" }),
            new Object[]{ 30, 90 }));

    prepares.add(new PrepareStruct("select name from student where age = ? and score = ?",
            expectedStr(new String[]{ "name=Tom" }),
            new Object[]{ 28, 85 }));

    prepares.add(new PrepareStruct("select name from student where age = ? and class = ?",
            expectedStr(new String[]{ "name=Suzy" }),
            new Object[]{ 31, "three" }));

    // currently not supported (30, +∞)
//    prepares.add(new PrepareStruct("select name from student where age > ? and class = ?",
//            expectedStr(new String[]{"name=Luke"}),
//            new Object[]{30, "one"}));

    prepares.add(new PrepareStruct("select name from student where age < ? and score = ?",
            expectedStr(new String[]{ "name=Kate" }),
            new Object[]{ 30, 99 }));

    prepares.add(new PrepareStruct("select name from student where class = 'one' limit ?",
            expectedStr(new String[]{ "name=Tom", "name=Mary", "name=Luke" }),
            new Object[]{ 5 }));

    prepares.add(new PrepareStruct("select name from student where class = 'one' limit ?",
            expectedStr(new String[]{ "name=Tom" }),
            new Object[]{ 1 }));

    prepares.add(new PrepareStruct("select name from student where class = 'one' limit ? offset ?",
            expectedStr(new String[]{ "name=Mary", "name=Luke" }),
            new Object[]{ 2, 1 }));

    prepares.add(new PrepareStruct("select name from student where class = ? limit ?",
            expectedStr(new String[]{ "name=Tom" }),
            new Object[]{ "one", 1 }));

    prepares.add(new PrepareStruct("select name from student where age = ? order by ? desc",
            expectedStr(new String[]{ "name=Suzy", "name=Luke" }),
            new Object[]{ 31, 1 }));

    prepares.add(new PrepareStruct("select id from student where age = ?",
            expectedStr(new String[]{ "id=2" }),
            new Object[]{ 32 }));

    prepares.add(new PrepareStruct("select id,score from student where age = ?",
            expectedStr(new String[]{ "id=2; score=91" }),
            new Object[]{ 32 }));

    execPrepareList(prepares, true, false);
  }

  @Test
  public void testAllPrepareUpdateCases() {
    List<PrepareStruct> prepares = new ArrayList<>();

    prepares.add(new PrepareStruct("update student set age = ? where name='Tom'",
            1,
            new Object[]{ 28 }));

    prepares.add(new PrepareStruct("update student set age = ? where name='Tommm'",
            0,
            new Object[]{ 28 }));

    prepares.add(new PrepareStruct("update student set class = ? where age < 30 and score < 90",
            2,
            new Object[]{ "one" }));

    prepares.add(new PrepareStruct("update student set class = ? where age < ? and score < 90",
            2,
            new Object[]{ "one", 30 }));

    prepares.add(new PrepareStruct("update student set age = 10 + ? where name = ?",
            1,
            new Object[]{ 18, "Tom" }));

    prepares.add(new PrepareStruct("update student set name = ? where score = age + ?",
            1,
            new Object[]{ "Suzy", 61 }));

    execPrepareList(prepares, false, true);
  }

  @Test
  public void testPrepareMultStmt() {




  }



}
