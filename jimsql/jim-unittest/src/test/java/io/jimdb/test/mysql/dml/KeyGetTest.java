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

import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @version V1.0
 */
public final class KeyGetTest extends SqlTestBase {
  private static String DBNAME = "test_get";

  private static String TABLENAME = "sqltest";

  @BeforeClass
  public static void createSqlTest() {
    createDB();
    createTable();
    initData();
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

  private static void initData() {
    execUpdate("INSERT INTO sqltest(id,name,age) VALUES(3333, 'testSelect1', 18),(4444, 'testSelect2', 28)", 2, true);
  }

  @Test
  public void testKeyGetAll() {
    List<String> expected = new ArrayList<>();
    expected.add("id=3333; name=testSelect1; age=18");
    execQuery("select * from sqltest where id=3333", expected);
  }

  @Test
  public void testKeyGetPartial() {
    List<String> expected = new ArrayList<>();
    expected.add("id=3333; name=testSelect1");
    execQuery("select id,name from sqltest where id=3333", expected);
  }

  @Test
  public void testKeyGetAlias() {
    List<String> expected = new ArrayList<>();
    expected.add("id=3333; nm=testSelect1");
    execQuery("select t1.id,t1.name as nm from sqltest as t1 where t1.id=3333", expected);
  }

  @Test
  public void testKeyGetMulti() {
    List<String> expected = new ArrayList<>();
    expected.add("id=3333; nm=testSelect1");
    expected.add("id=4444; nm=testSelect2");
    execQuery("select t1.id,t1.name as nm from sqltest as t1 where t1.id=3333 or t1.id=4444", expected);
  }

  @Test
  public void testKeyGetMultiIndex() {
    List<String> expected = new ArrayList<>();
    expected.add("id=3333; nm=testSelect1");
    expected.add("id=4444; nm=testSelect2");
    execQuery("select t1.id,t1.name as nm from sqltest as t1 where name='testSelect1' or t1.name='testSelect2'", expected);
  }
}
