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

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @version V1.0
 */
public final class InsertTest extends SqlTestBase {
  private static String DBNAME = "test";
  private static String TABLENAME = "sqltest";
  private List<String> deleteIDS = new ArrayList<>();

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

  @Before
  public void tearUp() {
    String SQL_CLEAR = "delete from %s where id > 0";
    execUpdate(String.format(SQL_CLEAR, TABLENAME), true);
  }

  @Test
  public void testInsertAll() {
    execUpdate("INSERT INTO sqltest VALUES(111, 'testInsertAll', 28)", 1, true);

    List<String> expected = new ArrayList<>();
    expected.add("id=111; name=testInsertAll; age=28");
    execQuery("select * from sqltest where id=111", expected);
  }

  @Test
  public void testInsertPartial() {
    execUpdate("INSERT INTO sqltest(id,name) VALUES(111, 'testInsertPartial')", 1, true);

    List<String> expected = new ArrayList<>();
    expected.add("id=111; name=testInsertPartial; age=null");
    execQuery("select * from sqltest where id=111", expected);
  }

  @Test
  public void testInsertNull() {
    execUpdate("INSERT INTO sqltest(id,name,age) VALUES(111, '中国', null)", 1, true);

    List<String> expected = new ArrayList<>();
    expected.add("id=111; age=null; name=中国");
    execQuery("select id,age,name from sqltest where id=111", expected);
  }

  @Test
  public void testInsertMultiValue() {
    execUpdate("INSERT INTO sqltest VALUES(111, 'testInsertMultiValue1', 28),(222, 'testInsertMultiValue2', 28),(333, 'testInsertMultiValue3', 28)", 3, true);
    execUpdate("INSERT INTO sqltest(id,name) VALUES(444, 'testInsertMultiValue4'),(555, 'testInsertMultiValue5'),(666, 'testInsertMultiValue6')", 3, true);

    List<String> expected = new ArrayList<>();
    expected.add("id=111; name=testInsertMultiValue1; age=28");
    expected.add("id=222; name=testInsertMultiValue2; age=28");
    expected.add("id=333; name=testInsertMultiValue3; age=28");
    expected.add("id=444; name=testInsertMultiValue4; age=null");
    expected.add("id=555; name=testInsertMultiValue5; age=null");
    expected.add("id=666; name=testInsertMultiValue6; age=null");
    execQuery("select * from sqltest where id=111 or id=222 or id=333 or id=444 or id=555 or id=666", expected);
  }

  @Test
  public void testInsertSet() {
    execUpdate("INSERT INTO sqltest set id=11111, name='testInsertPartial', age=18", 1, true);

    List<String> expected = new ArrayList<>();
    expected.add("id=11111; name=testInsertPartial; age=18");
    execQuery("select * from sqltest where id=11111", expected);
  }
}
