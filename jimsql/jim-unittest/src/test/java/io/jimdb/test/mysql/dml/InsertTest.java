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

import java.util.ArrayList;
import java.util.List;

import io.jimdb.test.mysql.SqlTestBase;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @version V1.0
 */
public final class InsertTest extends SqlTestBase {
  private static String DBNAME = "test_insert";
  private static String TABLENAME = "sqltest";
  private List<Integer> deleteIDS = new ArrayList<>();

  @BeforeClass
  public static void createSqlTest() {
    createDB();
    createTable();
  }

  private static void createDB() {
    createCatalog(DBNAME);
    useCatalog(DBNAME);
  }

  private static void createTable() {
    String sql = "CREATE TABLE IF NOT EXISTS " + TABLENAME + " ("
            + "`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT, "
            + "`name` varchar(255) NOT NULL, "
            + "`age` bigint(11) NULL, "
            + "PRIMARY KEY (`id`),"
            + "INDEX age_idx (age), "
            + "UNIQUE INDEX name_idx (name) "
            + ") COMMENT 'REPLICA=1' ENGINE=MEMORY AUTO_INCREMENT=0 ";
    dropAndCreateTable(TABLENAME, sql);
  }

  @After
  public void tearDown() {
    for (Integer id : deleteIDS) {
      String SQL_CLEAR = "delete from %s where id = %d";
      execUpdate(String.format(SQL_CLEAR, TABLENAME, id), true);
    }
    this.deleteIDS.clear();
  }

  @Test
  public void testInsertAll() {
    this.deleteIDS.add(111);
    String sql = String.format("INSERT INTO %s VALUES(111, 'testInsertAll', 28)", TABLENAME);
    execUpdate(sql, 1, true);

    List<String> expected = new ArrayList<>();
    expected.add("id=111; name=testInsertAll; age=28");
    execQuery(String.format("select * from %s where id=111", TABLENAME), expected);
  }

  @Test
  public void testInsertPartial() {
    this.deleteIDS.add(111);
    String sql = String.format("INSERT INTO %s (id,name) VALUES(111, 'testInsertPartial')", TABLENAME);
    execUpdate(sql, 1, true);

    List<String> expected = new ArrayList<>();
    expected.add("id=111; name=testInsertPartial; age=null");
    execQuery(String.format("select * from %s where id=111", TABLENAME), expected);
  }

  @Test
  public void testInsertNull() {
    this.deleteIDS.add(111);
    String sql = String.format("INSERT INTO %s (id,name,age) VALUES(111, '中国', null)", TABLENAME);
    execUpdate(sql, 1, true);

    List<String> expected = new ArrayList<>();
    expected.add("id=111; age=null; name=中国");
    execQuery(String.format("select id,age,name from %s where id=111", TABLENAME), expected);
  }

  @Test
  public void testInsertMultiValue() {
    this.deleteIDS.add(111);
    this.deleteIDS.add(222);
    this.deleteIDS.add(333);
    this.deleteIDS.add(444);
    this.deleteIDS.add(555);
    this.deleteIDS.add(666);
    String sql = String.format("INSERT INTO %s VALUES (111, 'testInsertMultiValue1', 28)"
            + ",(222, 'testInsertMultiValue2', 28),(333, 'testInsertMultiValue3', 28)", TABLENAME);
    execUpdate(sql, 3, true);
    sql = String.format("INSERT INTO %s (id,name) VALUES (444, 'testInsertMultiValue4')"
            + ",(555, 'testInsertMultiValue5'),(666, 'testInsertMultiValue6')", TABLENAME);
    execUpdate(sql, 3, true);

    List<String> expected = new ArrayList<>();
    expected.add("id=111; name=testInsertMultiValue1; age=28");
    expected.add("id=222; name=testInsertMultiValue2; age=28");
    expected.add("id=333; name=testInsertMultiValue3; age=28");
    expected.add("id=444; name=testInsertMultiValue4; age=null");
    expected.add("id=555; name=testInsertMultiValue5; age=null");
    expected.add("id=666; name=testInsertMultiValue6; age=null");
    execQuery(String.format("select * from %s where id=111 or id=222 or id=333 or id=444 or id=555 or id=666", TABLENAME), expected);
  }

  @Test
  public void testInsertSet() {
    this.deleteIDS.add(11111);
    String sql = String.format("INSERT INTO %s set id=11111, name='testInsertPartial', age=18", TABLENAME);
    execUpdate(sql, 1, true);

    List<String> expected = new ArrayList<>();
    expected.add("id=11111; name=testInsertPartial; age=18");
    execQuery(String.format("select * from %s where id=11111", TABLENAME), expected);
  }

}
