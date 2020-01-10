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

import java.util.Arrays;
import java.util.List;

import io.jimdb.test.mysql.SqlTestBase;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class MyDecimalSqlTest extends SqlTestBase {
  private static String DBNAME = "test";
  private static String normalTable = "blue_decimal2";
  private static String indexTable = "blue_decimal_index";

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @BeforeClass
  public static void createDecimal() {
    createDB();
    createTable(normalTable);
    createTableWithIndex(indexTable);
  }

  private static void createDB() {
    createCatalog(DBNAME);
    useCatalog(DBNAME);
  }

  private static void createTable(String name) {
    String sql = "CREATE TABLE IF NOT EXISTS " + name + " ("
            + "`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT, "
            + "`d1` decimal(10,2) DEFAULT NULL, "
            + "PRIMARY KEY (`id`)"
            + ") COMMENT 'REPLICA=1' ENGINE=MEMORY AUTO_INCREMENT=0 ";
    dropAndCreateTable(name, sql);
  }

  private static void createTableWithIndex(String name) {
    String sql = "CREATE TABLE IF NOT EXISTS " + name + " ("
            + "`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT, "
            + "`d1` decimal(10,2) DEFAULT NULL, "
            + "PRIMARY KEY (`id`),"
            + "INDEX d_idx (d1) "
            + ") COMMENT 'REPLICA=1' ENGINE=MEMORY AUTO_INCREMENT=0 ";
    dropAndCreateTable(name, sql);
  }

  @Test
  public void testNormalColumn() {
    execUpdate("INSERT INTO blue_decimal2 VALUES(1,5.1)", 1, true);
    execUpdate("INSERT INTO blue_decimal2 VALUES(2,5.1)", 1, true);
    execUpdate("INSERT INTO blue_decimal2 VALUES(3,5.2)", 1, true);
    execUpdate("INSERT INTO blue_decimal2 VALUES(4,11111111.2)", 1, true);
    execUpdate("INSERT INTO blue_decimal2 VALUES(5,-11111111.2)", 1, true);

    // test all
    String expectedStr = "id=1; d1=5.10,id=2; d1=5.10,id=3; d1=5.20,id=4; d1=11111111.20,id=5; d1=-11111111.20";
    List<String>expected = Arrays.asList(expectedStr.split(","));
    execQuery("select * from blue_decimal2 order by id", expected);
    // test where const
    expectedStr = "id=3; d1=5.20";
    expected = Arrays.asList(expectedStr.split(","));
    execQuery("select * from blue_decimal2 where d1 = 5.20 ", expected);
    // test where range
    expectedStr = "id=1; d1=5.10,id=2; d1=5.10,id=3; d1=5.20,";
    expected = Arrays.asList(expectedStr.split(","));
    execQuery("select * from blue_decimal2 where d1 >=5 and d1<=6 order by id ", expected);

    expectedStr = "id=5; d1=-11111111.20,id=1; d1=5.10,id=2; d1=5.10";
    expected = Arrays.asList(expectedStr.split(","));
    execQuery("select * from blue_decimal2 where d1 <=5.10 order by d1 ", expected);
  }
  @Ignore
  @Test
  public void testLocal() {
    execUpdate("INSERT INTO blue_decimal_index VALUES(1,5.1)", 1, true);

    // test all
    String expectedStr = "id=2; d1=5.10,id=2; d1=5.10,id=3; d1=5.20,id=4; d1=11111111.20,id=5; d1=-11111111.20";
    List<String> expected = Arrays.asList(expectedStr.split(","));
//    execQuery("select * from blue_decimal_index where id = 7", expected);
//    execQuery("select * from blue_decimal_index where id = 1 ", expected);
//    execQuery("select * from blue_decimal2 where d1 = 5.1 ", expected);
//    execUpdate("delete from blue_decimal2 where d1 = 5.1", 1, true);
  }

  @Test
  public void testUniqueIndex() {
    execUpdate("INSERT INTO blue_decimal_index VALUES(1,5.1)", 1, true);
    execUpdate("INSERT INTO blue_decimal_index VALUES(2,5.3)", 1, true);
    execUpdate("INSERT INTO blue_decimal_index VALUES(3,5.2)", 1, true);
    execUpdate("INSERT INTO blue_decimal_index VALUES(4,11111111.2)", 1, true);
    execUpdate("INSERT INTO blue_decimal_index VALUES(5,-11111111.2)", 1, true);
    // test all
    String expectedStr = "id=1; d1=5.10,id=2; d1=5.30,id=3; d1=5.20,id=4; d1=11111111.20,id=5; d1=-11111111.20";
    List<String>expected = Arrays.asList(expectedStr.split(","));
    execQuery("select * from blue_decimal_index order by id", expected);
    // test where const
    expectedStr = "id=3; d1=5.20";
    expected = Arrays.asList(expectedStr.split(","));
    execQuery("select * from blue_decimal_index where d1 = 5.20 ", expected);
    // test where range
    expectedStr = "id=1; d1=5.10,id=2; d1=5.30,id=3; d1=5.20,";
    expected = Arrays.asList(expectedStr.split(","));
    execQuery("select * from blue_decimal_index where d1 >=5 and d1<=6 order by id ", expected);

    expectedStr = "id=5; d1=-11111111.20,id=1; d1=5.10,id=3; d1=5.20,id=2; d1=5.30";
    expected = Arrays.asList(expectedStr.split(","));
    execQuery("select * from blue_decimal_index where d1 <=5.30 order by d1 ", expected);
    // use index column
    expectedStr = "d1=-11111111.20,d1=5.10,d1=5.20,d1=5.30";
    expected = Arrays.asList(expectedStr.split(","));
    execQuery("select d1 from blue_decimal_index where d1 <=5.30 order by d1 ", expected);
  }

//  @Test
//  public void testDouble() {
//    defaultTable = "blue_double_test";
//    for (int i = 1; i <= 1; i++) {
//      deleteIDS.add(String.valueOf(i));
//    }
//    List<String> expected = new ArrayList<>();
//    expected.add("id=1; d1=5.2");
//    execUpdate("INSERT INTO blue_double_test VALUES(1,5.1)", 1, true);
//    execUpdate("update blue_double_test set d1 = 5.2 where id =1", 1, true);
//    execQuery("select * from blue_double_test", expected);
//  }
//
//  @Test
//  public void testTinyInt() {
//    defaultTable = "blue_tinyint_test";
//    for (int i = 1; i <= 1; i++) {
//      deleteIDS.add(String.valueOf(i));
//    }
//    List<String> expected = new ArrayList<>();
//    expected.add("id=1; t1=111");
//    execUpdate("INSERT INTO blue_tinyint_test ( id ,t1) VALUES ( 1,111 );", 1, true);
//    execQuery("select * from blue_tinyint_test", expected);
//  }
//
//  @Test
//  public void testTinyIntOutRange() {
//    exception.expect(RuntimeException.class);
//    exception.expectMessage("TinyInt value is out of range");
//    execUpdate("INSERT INTO blue_tinyint_test ( id ,t1) VALUES ( 1,130 );", 0, true);
//  }

}
