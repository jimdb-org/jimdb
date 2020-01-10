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

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import io.jimdb.core.Bootstraps;
import io.jimdb.core.config.JimConfig;
import io.jimdb.test.TestUtil;
import io.jimdb.sql.server.JimBootstrap;
import io.jimdb.sql.server.netty.JimService;

import org.apache.commons.lang3.StringUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * @version V1.0
 */
public abstract class SqlTestBase {
  protected static JimService service;
  protected static HikariDataSource dataSource;

  @BeforeClass
  public static void beforeClass() throws Exception {
    if (dataSource == null) {
      JimConfig config = Bootstraps.init("jim_test.properties");

      // disable background thread of fetching stats info
      config.setEnableBackgroundStatsCollector(false);

      service = JimBootstrap.createServer(config);
      JimBootstrap.start(service);

      HikariConfig hikariConfig = new HikariConfig();
      String dbName = config.getString("jim.test.db", "");
      if (StringUtils.isNotBlank(dbName)) {
        dbName = "/" + dbName;
      }

      String baseUrl = "jdbc:mysql://localhost:%d%s?useUnicode=true&characterEncoding=utf8&useServerPrepStmts=true&cachePrepStmts=true";
      hikariConfig.setJdbcUrl(String.format(baseUrl, config.getServerConfig().getPort(), dbName));
      hikariConfig.setDriverClassName("com.mysql.jdbc.Driver");
      hikariConfig.setUsername("root");
      hikariConfig.setPassword("root");
      hikariConfig.addDataSourceProperty("cachePrepStmts", "true");
      hikariConfig.addDataSourceProperty("prepStmtCacheSize", "250");
      hikariConfig.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
      dataSource = new HikariDataSource(hikariConfig);
    }
  }

  @AfterClass
  public static void afterClass() {
  }

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

  protected void execUpdate(String sql, SQLException result, boolean autocommit) {
    Connection conn = null;
    Statement stmt = null;
    SQLException rs = null;
    try {
      conn = dataSource.getConnection();
      conn.setAutoCommit(autocommit);
      stmt = conn.createStatement();
      stmt.executeUpdate(sql);
      if (!autocommit) {
        conn.commit();
      }
    } catch (SQLException e) {
      rs = e;
    } finally {
      closeQuit(conn, stmt, null);
    }

    Assert.assertNotNull(rs);
    Assert.assertEquals(result.getErrorCode(), rs.getErrorCode());
    Assert.assertEquals(result.getSQLState(), rs.getSQLState());
    if (StringUtils.isBlank(result.getMessage())) {
      return;
    }
    Assert.assertEquals(result.getMessage(), rs.getMessage());
  }

  protected static void execUpdate(String sql, boolean autocommit) {
    Connection conn = null;
    Statement stmt = null;
    try {
      conn = dataSource.getConnection();
      conn.setAutoCommit(autocommit);
      stmt = conn.createStatement();
      stmt.executeUpdate(sql);
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

  protected static void execUpdate(String sql, int result, boolean autocommit) {
    Connection conn = null;
    Statement stmt = null;
    try {
      conn = dataSource.getConnection();
      conn.setAutoCommit(autocommit);
      stmt = conn.createStatement();
      int rs = stmt.executeUpdate(sql);
      if (!autocommit) {
        conn.commit();
      }
      Assert.assertEquals(result, rs);
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

  protected int[] execUpdate(String[] sqls, int[] result, boolean autocommit) {
    Connection conn = null;
    Statement stmt = null;
    int[] rs = null;
    try {
      conn = dataSource.getConnection();
      conn.setAutoCommit(autocommit);
      stmt = conn.createStatement();
      for (String sql : sqls) {
        stmt.addBatch(sql);
      }
      rs = stmt.executeBatch();
      if (!autocommit) {
        conn.commit();
      }
      Assert.assertArrayEquals(result, rs);
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
    return rs;
  }

  protected List<String> execQuery(String sql) {
    final List<String> result = new ArrayList<>();
    execQuery(sql, resultSet -> {
      try {
        result.addAll(collect(resultSet));
      } catch (SQLException e) {
        TestUtil.rethrow(e);
      }
    });
    return result;
  }

  protected void execQueryInDB(String db, String sql, List<String> expected) {
    execQuery(db, sql, expected, true);
  }

  protected void execQuery(String sql, List<String> expected) {
    execQuery(sql, expected, false);
  }

  protected void execQuery(String sql, List<String> expected, boolean checkSort) {
    execQuery(null, sql, expected, checkSort);
  }

  protected void execQuery(String db, String sql, List<String> expected, boolean checkSort) {
    execQuery(db, sql, resultSet -> {
      try {
        List<String> actual = collect(resultSet);
        if (!checkSort) {
          if (actual.size() == expected.size() && actual.containsAll(expected)) {
            return;
          }
        }
        Assert.assertEquals(sql, expected, actual);
      } catch (Exception e) {
        TestUtil.rethrow(e);
      }
    });
  }

  protected void execQueryNotCheck(String sql) {
    Connection conn = null;
    Statement stmt = null;
    ResultSet resultSet = null;
    try {
      conn = dataSource.getConnection();
      conn.setAutoCommit(true);
      stmt = conn.createStatement();
//      long start = System.currentTimeMillis();
      resultSet = stmt.executeQuery(sql);
//      System.out.println("sql elapse: " + (System.currentTimeMillis() - start) + "ms");
    } catch (Exception e) {
      TestUtil.rethrow(e);
    } finally {
      closeQuit(conn, stmt, resultSet);
    }
  }

  protected void execQuery(String sql, Consumer<ResultSet> expect) {
    execQuery(null, sql, expect);
  }

  protected void execQuery(String db, String sql, Consumer<ResultSet> expect) {
    Connection conn = null;
    Statement stmt = null;
    ResultSet resultSet = null;
    try {
      conn = dataSource.getConnection();
      conn.setAutoCommit(true);
      stmt = conn.createStatement();
      if (StringUtils.isNotBlank(db)) {
        stmt.executeUpdate(String.format("use %s", db));
      }
      long start = System.currentTimeMillis();
      resultSet = stmt.executeQuery(sql);
      System.out.println("sql elapse: " + (System.currentTimeMillis() - start) + "ms");
      if (expect != null) {
        expect.accept(resultSet);
      }
    } catch (Exception e) {
      TestUtil.rethrow(e);
    } finally {
      closeQuit(conn, stmt, resultSet);
    }
  }

  protected void execPrepareUpdate(String sql, int result, Object... args) {
    Connection conn = null;
    PreparedStatement stmt = null;
    int resultSet;
    try {
      conn = dataSource.getConnection();
      conn.setAutoCommit(true);
      stmt = conn.prepareStatement(sql);

      setPrepareStmtArgs(stmt, args);

      resultSet = stmt.executeUpdate();

      assert resultSet == result;
    } catch (Exception e) {
      TestUtil.rethrow(e);
    } finally {
      try {
        if (stmt != null) {
          stmt.close();
        }
        if (conn != null) {
          conn.close();
        }
      } catch (SQLException ignored) {
      }
    }
  }

  protected void execPrepareList(List<PrepareStruct> prepares, boolean isQuery, boolean checkSort) {
    Connection conn = null;
    PreparedStatement stmt = null;

    for (PrepareStruct prepare : prepares) {
      ResultSet resultSet = null;
      try {
        if (conn == null) {
          conn = dataSource.getConnection();
          conn.setAutoCommit(true);
        }
        stmt = conn.prepareStatement(prepare.getSql());

        setPrepareStmtArgs(stmt, prepare.getArgs());

        if (isQuery) {
          resultSet = stmt.executeQuery();
          List<String> actual = collect(resultSet);

          if (!checkSort) {
            Collections.sort(actual);
            Collections.sort(prepare.getQueryExpected());
          }
          Assert.assertEquals(prepare.sql, prepare.getQueryExpected(), actual);
        } else {
          int actual = stmt.executeUpdate();
          Assert.assertEquals(prepare.sql, prepare.getUpdateExpected(), actual);
        }
      } catch (Exception e) {
        TestUtil.rethrow(e);
      } finally {
        try {
          if (resultSet != null) {
            resultSet.close();
          }
        } catch (SQLException ignored) {
        }
      }
    }

    try {
      if (stmt != null) {
        stmt.close();
      }
      if (conn != null) {
        conn.close();
      }
    } catch (SQLException ignored) {
    }
  }

  protected void execPrepareQuery(String sql, List<String> expected, Object... args) {
    execPrepareQuery(sql, resultSet -> {
      try {
        List<String> actual = collect(resultSet);
        Assert.assertEquals(expected, actual);
      } catch (Exception e) {
        TestUtil.rethrow(e);
      }
    }, args);
  }

  protected void execPrepareQuery(String sql, Consumer<ResultSet> expect, Object... args) {
    Connection conn = null;
    PreparedStatement stmt = null;
    ResultSet resultSet = null;
    try {
      conn = dataSource.getConnection();
      conn.setAutoCommit(true);
      stmt = conn.prepareStatement(sql);
      setPrepareStmtArgs(stmt, args);

      for (int i = 0; i < 2; i++) {
        resultSet = stmt.executeQuery();
      }
      if (expect != null) {
        expect.accept(resultSet);
      }
    } catch (Exception e) {
      e.printStackTrace();
      TestUtil.rethrow(e);
    } finally {
      closeQuit(conn, stmt, resultSet);
    }
  }

  private void setPrepareStmtArgs(PreparedStatement stmt, Object... args) throws SQLException {
    if (args != null) {
      for (int i = 0; i < args.length; i++) {
        if (args[i].getClass() == Integer.class) {
          stmt.setInt(i + 1, (Integer) args[i]);
        } else if (args[i].getClass() == Long.class) {
          stmt.setLong(i + 1, (Long) args[i]);
        } else if (args[i].getClass() == String.class) {
          stmt.setString(i + 1, (String) args[i]);
        } else if (args[i].getClass() == Boolean.class) {
          stmt.setBoolean(i + 1, (boolean) args[i]);
        } else if (args[i].getClass() == Double.class) {
          stmt.setDouble(i + 1, (Double) args[i]);
        } else if (args[i].getClass() == Float.class) {
          stmt.setFloat(i + 1, (Float) args[i]);
        } else if (args[i].getClass() == BigDecimal.class) {
          stmt.setBigDecimal(i + 1, (BigDecimal) args[i]);
        } else if (args[i].getClass() == Date.class) {
          stmt.setDate(i + 1, (Date) args[i]);
        } else if (args[i].getClass() == Timestamp.class) {
          stmt.setTimestamp(i + 1, (Timestamp) args[i]);
        }
//        else if (args[i].getClass() == null) {
//          stmt.setNull(i, null);
//        }
      }
    }
  }

  protected static void closeQuit(final Connection conn, final Statement stmt, final ResultSet rs) {
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

  protected List<String> collect(ResultSet resultSet) throws SQLException {
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

  protected static List<String> expectedStr(String[] args) {
    List<String> expected = new ArrayList<>();
    Collections.addAll(expected, args);
    return expected;
  }

  /**
   * Prepare Test Struct
   */
  protected static class PrepareStruct {
    private String sql;
    private List<String> queryExpected;
    private int updateExpected;
    private Object[] args;

    public PrepareStruct() {
    }

    public PrepareStruct(String sql, List<String> queryExpected, Object[] args) {
      this.sql = sql;
      this.queryExpected = queryExpected;
      this.args = args;
    }

    public PrepareStruct(String sql, int updateExpected, Object[] args) {
      this.sql = sql;
      this.updateExpected = updateExpected;
      this.args = args;
    }

    public String getSql() {
      return sql;
    }

    public PrepareStruct setSql(String sql) {
      this.sql = sql;
      return this;
    }

    public List<String> getQueryExpected() {
      return queryExpected;
    }

    public PrepareStruct setQueryExpected(List<String> queryExpected) {
      this.queryExpected = queryExpected;
      return this;
    }

    public int getUpdateExpected() {
      return updateExpected;
    }

    public PrepareStruct setUpdateExpected(int updateExpected) {
      this.updateExpected = updateExpected;
      return this;
    }

    public Object[] getArgs() {
      return args;
    }

    public PrepareStruct setArgs(Object[] args) {
      this.args = args;
      return this;
    }
  }
}
