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

import io.jimdb.test.mysql.SqlTestBase;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @version V1.0
 */
public final class KeyDeleteTest extends SqlTestBase {
  private static String DBNAME = "test";
  private static String TABLENAME = "sqltest";

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
  public void setUp() {
    execUpdate("INSERT INTO sqltest(id,name,age) VALUES(2222, 'testDelete', 18)", 1, true);
  }

  @Test
  public void testKeyDelete() {
    try {
      Thread.sleep(1000);
    } catch (Exception ex) {
    }
    execUpdate("DELETE from sqltest where id=2222", 1, true);
    try {
      Thread.sleep(1000);
    } catch (Exception ex) {
    }
  }

  @Test
  public void testKeyDeleteAlias() {
    try {
      Thread.sleep(1000);
    } catch (Exception ex) {
    }
    execUpdate("DELETE from sqltest as t1 where t1.id=2222", 1, true);
    try {
      Thread.sleep(1000);
    } catch (Exception ex) {
    }
  }

  @Test
  public void testKeyDeleteIndex() {
    try {
      Thread.sleep(1000);
    } catch (Exception ex) {
    }
    execUpdate("DELETE from sqltest where name='testDelete'", 1, true);
    try {
      Thread.sleep(1000);
    } catch (Exception ex) {
    }
  }
}
