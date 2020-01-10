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

package io.jimdb.test.planner.statistics.integration;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import io.jimdb.test.TestUtil;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zaxxer.hikari.HikariDataSource;

public class SQLHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(SQLHandler.class);

  // FIXME we need to use a proxy to avoid the duplicated code from SqlTestBase.
  //  Just put an instance of SqlTestBase in this test class instead of c & p the following functions.
  protected static List<String> execQuery(HikariDataSource dataSource, String sql) {
    List<String> result = new ArrayList<>();
    execQuery(dataSource, sql, resultSet -> {
      try {
        result.addAll(collect(resultSet));
        //Assert.assertEquals(expected, actual);
      } catch (Exception e) {
        TestUtil.rethrow(e);
      }
    });

    return result;
  }

  private static List<String> collect(ResultSet resultSet) throws SQLException {
    final List<String> result = new ArrayList<>();
    final StringBuilder buf = new StringBuilder();
    while (resultSet.next()) {
      buf.setLength(0);
      int count = resultSet.getMetaData().getColumnCount();
      String sep = "";
      for (int i = 1; i <= count; i++) {
        buf.append(sep)
                .append(resultSet.getMetaData().getColumnLabel(i))
                .append("=")
                .append(resultSet.getString(i));
        sep = "; ";
      }
      result.add(TestUtil.linuxString(buf.toString()));
    }
    return result;
  }

  private static void execQuery(HikariDataSource dataSource, String sql, Consumer<ResultSet> expect) {
    Connection conn = null;
    Statement stmt = null;
    ResultSet resultSet = null;
    try {
      conn = dataSource.getConnection();
      LOGGER.debug("successfully established connections from proxy ... ");

      conn.setAutoCommit(true);
      stmt = conn.createStatement();
      LOGGER.debug("successfully created query statement and start executing query ... ");

      resultSet = stmt.executeQuery(sql);
      LOGGER.debug("successfully received query results from proxy ... ");

      if (expect != null) {
        expect.accept(resultSet);
        LOGGER.debug("received results from proxy is not null ... ");
      }
    } catch (Exception e) {
      TestUtil.rethrow(e);
    } finally {
      closeQuit(conn, stmt, resultSet);
    }
  }

  protected static void execUpdate(HikariDataSource dataSource, String sql, int result, boolean autocommit) {
    execUpdate(dataSource, new String[]{ sql }, new int[]{ result }, autocommit);
  }

  private static void execUpdate(HikariDataSource dataSource, String[] sqls, int[] result, boolean autocommit) {
    Connection conn = null;
    Statement stmt = null;
    try {
      conn = dataSource.getConnection();
      conn.setAutoCommit(autocommit);
      stmt = conn.createStatement();
      for (String sql : sqls) {
        stmt.addBatch(sql);
      }
      int[] rs = stmt.executeBatch();
      if (!autocommit) {
        conn.commit();
      }
      Assert.assertArrayEquals(result, rs);
    } catch (Exception e) {
      if (!autocommit) {
        try {
          conn.rollback();
        } catch (Exception e1) {
          TestUtil.rethrow(e1);
        }
      }
      TestUtil.rethrow(e);
    } finally {
      closeQuit(conn, stmt, null);
    }
  }

  private static void closeQuit(final Connection conn, final Statement stmt, final ResultSet rs) {
    try {
      if (rs != null) {
        rs.close();
      }
      if (stmt != null) {
        stmt.close();
      }
      if (conn != null) {
        conn.close();
      }
    } catch (SQLException e) {
    }
  }
}
