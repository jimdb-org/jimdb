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
package io.jimdb.test.mysql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.jimdb.sql.server.netty.JimService;
import io.jimdb.test.TestUtil;

import com.zaxxer.hikari.HikariDataSource;

/**
 * @version V1.0
 */
public abstract class SqlTransTestBase extends SqlTestBase {
  protected static JimService service;
  protected static HikariDataSource dataSource;
  private String DQL = "select";
  protected static Pattern pattern = Pattern.compile("^(select|explian)",Pattern.CASE_INSENSITIVE);


  protected static void createCatalog(String name) {
    deleteCatalog(name);

    String sql = String.format("Create database %s", name);
    execUpdate(sql, 0, true);
  }

  protected static void deleteCatalog(String name) {
    String sql = String.format("Drop database IF EXISTS %s", name);
    execUpdate(sql, 0, true);
  }

  protected static void useCatalog(String name) {
    String sql = String.format("use %s", name);
    execUpdate(sql, 0, true);
  }

  protected static void dropAndCreateTable(String tableName, String createSql) {
    String sql = String.format("DROP TABLE IF EXISTS %s ", tableName);
    execUpdate(sql, true);
    try {
      Thread.sleep(1000);
    } catch (Exception e) {
    }

    execUpdate(createSql, true);
    try {
      Thread.sleep(1000);
    } catch (Exception e) {
    }
  }

  protected static void execUpdate(String sql[], boolean autocommit) {
    Connection conn = null;
    Statement stmt = null;
    try {
      conn = dataSource.getConnection();
      conn.setAutoCommit(autocommit);
      stmt = conn.createStatement();
      for (String s : sql) {
        Matcher matcher = pattern.matcher(s);
        if (matcher.find()) {
          ResultSet resultSet = stmt.executeQuery(s);
        }
      }
      if (!autocommit) {
        conn.commit();
      }
    } catch (Exception e) {
      if (!autocommit) {
        try {
          if (conn != null) {
            conn.rollback();
          }
        } catch (Exception e1) {
          TestUtil.rethrow(e1);
        }
      }
      TestUtil.rethrow(e);
    } finally {
      closeQuit(conn, stmt, null);
    }
  }
}
