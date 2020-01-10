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

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;

import io.jimdb.test.TestUtil;
import io.jimdb.test.mysql.SqlTestBase;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @version V1.0
 */
public final class AutoCommitTest extends SqlTestBase {

  private static final String AUTOCOMMIT_TEST_DB_NAME = "auto_commit_db";
  private static final String AUTOCOMMIT_TEST_TABLE_NAME = "auto_commit_table";

  @Before
  public void tearUp() {
    createCatalog(AUTOCOMMIT_TEST_DB_NAME);

    String sql = String.format("use %s", AUTOCOMMIT_TEST_DB_NAME);
    execUpdate(sql, 0, true);

    sql = String.format("CREATE TABLE IF NOT EXISTS %s(id BIGINT, name varchar(32), num BIGINT, PRIMARY KEY(id)) COMMENT 'REPLICA=1' ENGINE=MEMORY",
            AUTOCOMMIT_TEST_TABLE_NAME);
    execUpdate(sql, 0, true);
  }

  @After
  public void tearDown() {
    String sql = String.format("DROP TABLE IF EXISTS %s", AUTOCOMMIT_TEST_TABLE_NAME);
    execUpdate(sql, 0, true);

    deleteCatalog(AUTOCOMMIT_TEST_DB_NAME);
  }

  @Test
  public void testAutoCommit() {
    List<String> expected = expectedStr(new String[]{});
    execQueryInDB(AUTOCOMMIT_TEST_DB_NAME, String.format("select * from %s where id=115 limit 1", AUTOCOMMIT_TEST_TABLE_NAME), expected);

    execUpdate(String.format("INSERT INTO %s VALUES(115, 'testInsert115', 30)", AUTOCOMMIT_TEST_TABLE_NAME), 1, true);

    expected = expectedStr(new String[]{ "id=115; name=testInsert115; num=30" });
    execQueryInDB(AUTOCOMMIT_TEST_DB_NAME, String.format("select * from %s where id=115 limit 1", AUTOCOMMIT_TEST_TABLE_NAME), expected);
  }

  @Test
  public void testNotAutoCommit() {
    List<String> expected = expectedStr(new String[]{});
    execQueryInDB(AUTOCOMMIT_TEST_DB_NAME, String.format("select * from %s where id=115 limit 1", AUTOCOMMIT_TEST_TABLE_NAME), expected);

    Connection conn = null;
    Statement stmt = null;
    try {
      conn = dataSource.getConnection();
      // set autocommit=0
      conn.setAutoCommit(false);
      stmt = conn.createStatement();
      int rs = stmt.executeUpdate(String.format("INSERT INTO %s VALUES(115, 'testInsert115', 30)", AUTOCOMMIT_TEST_TABLE_NAME));

      // select again before committing by handle
      expected = expectedStr(new String[]{});
      execQueryInDB(AUTOCOMMIT_TEST_DB_NAME, String.format("select * from %s where id=115 limit 1", AUTOCOMMIT_TEST_TABLE_NAME), expected);

      conn.commit();
      Assert.assertEquals(1, rs);

      expected = expectedStr(new String[]{ "id=115; name=testInsert115; num=30" });
      execQueryInDB(AUTOCOMMIT_TEST_DB_NAME, String.format("select * from %s where id=115 limit 1", AUTOCOMMIT_TEST_TABLE_NAME), expected);
    } catch (Exception e) {
      try {
        if (conn != null && !conn.getAutoCommit()) {
          conn.rollback();
        }
      } catch (Exception e1) {
        TestUtil.rethrow(e1);
      }
      TestUtil.rethrow(e);
    } finally {
      closeQuit(conn, stmt, null);
    }
  }
}
