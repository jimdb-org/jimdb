/*
 * Copyright 2019 The Chubao Authors.
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
package io.jimdb.sql.smith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import io.jimdb.config.JimConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings({ "SQL_INJECTION_JDBC", "UTWR_USE_TRY_WITH_RESOURCES" })
public class ResultChecker {

  private static final Logger LOG = LoggerFactory.getLogger(ResultChecker.class);


  private HikariDataSource chubaoDataSource;
  private HikariDataSource mySQLDataSource;

  public void init(JimConfig config) {

    this.chubaoDataSource = createDataSource(config.getAuthUser(), config.getAuthPassword(), config
            .getString("netty.server.host", "localhost"), config
            .getString("netty.server.port", "3360"), config.getString("jim.test.db", "test"));

    String mySQLHost = config.getString("mysql.server.host", "localhost");
    String mySQLPort = config.getString("mysql.server.port", "3306");
    String mySQLUser = config.getString("mysql.server.user", "root");
    String mySQLPassword = config.getString("mysql.server.password", "root");
    String mySQLdb = config.getString("mysql.server.db", "test");

    this.mySQLDataSource = createDataSource(mySQLUser, mySQLPassword, mySQLHost, mySQLPort, mySQLdb);
  }

  public void checkResultSet(String sql) {
    Connection chubaoConn = null;
    Connection mySQLConn = null;
    try {
      List<String> chubaoCollect;
      List<String> mysqlCollect;

      chubaoConn = chubaoDataSource.getConnection();
      mySQLConn = mySQLDataSource.getConnection();

      ResultSet chubaoResult = executeQuery(sql, chubaoDataSource, DBType.CHUBAO, chubaoConn);
      ResultSet mySQLResult = executeQuery(sql, mySQLDataSource, DBType.MYSQL, mySQLConn);

      if (chubaoResult != null) {
        chubaoCollect = collect(chubaoResult);
      } else {
        LOG.error("[SQLSMITH] Chubao database result set is empty sql : {} ", sql);
        return;
      }
      if (mySQLResult != null) {
        mysqlCollect = collect(mySQLResult);
      } else {
        LOG.error("[SQLSMITH] MySQL database result set is empty sql {} ", sql);
        return;
      }

      if (chubaoResult.getMetaData().getColumnCount() != mySQLResult.getMetaData().getColumnCount()) {
        LOG.error("[SQLSMITH] Check resultSet The number of columns is inconsistent sql : {} ", sql);
        LOG.error("[SQLSMITH] Chubao resultSet : {} ", JSON.toJSONString(chubaoCollect, true));
        LOG.error("[SQLSMITH] MySQL resultSet  : {} ", JSON.toJSONString(mysqlCollect, true));
      }

      while (chubaoResult.next()) {
        if (!mySQLResult.next()) {
          LOG.error("[SQLSMITH] Check ResultSet The number of columns is inconsistent sql : {} ", sql);
          LOG.error("[SQLSMITH] chubao resultSet : {} ", JSON.toJSONString(chubaoCollect, true));
          LOG.error("[SQLSMITH] MySQL resultSet  : {} ", JSON.toJSONString(mysqlCollect, true));
        }

        int count = chubaoResult.getMetaData().getColumnCount();
        for (int i = 1; i <= count; i++) {
          if (!chubaoResult.getMetaData().getColumnLabel(i).equals(mySQLResult.getMetaData().getColumnLabel(i))) {
            LOG.error("[SQLSMITH] Check ResultSet The number of columns is inconsistent sql : {}", sql);
            LOG.error("[SQLSMITH] Chubao resultSet : {} ", JSON.toJSONString(chubaoCollect, true));
            LOG.error("[SQLSMITH] MySQL resultSet : {} ", JSON.toJSONString(mysqlCollect, true));
          }

          if (!chubaoResult.getString(i).equals(mySQLResult.getString(i))) {
            LOG.error("[SQLSMITH] Check ResultSet The number of columns is inconsistent sql : {} ", sql);
            LOG.error("[SQLSMITH] Chubao resultSet : {} ", JSON.toJSONString(chubaoCollect, true));
            LOG.error("[SQLSMITH] MySQL resultSet  : {} ", JSON.toJSONString(mysqlCollect, true));
          }
        }
      }
    } catch (SQLException ex) {
      LOG.error("[SQLSMITH] Check ResultSet error sql : {} ,error : {} ", sql, ex);
    } finally {
      if (chubaoConn != null) {
        try {
          chubaoConn.close();
        } catch (SQLException e) {
          LOG.error("[SQLSMITH] ChubaoConn close error ");
        }
      }

      if (mySQLConn != null) {
        try {
          mySQLConn.close();
        } catch (SQLException e) {
          LOG.error("[SQLSMITH] MySQLConn close error ");
        }
      }
    }
  }

  @SuppressFBWarnings({ "USBR_UNNECESSARY_STORE_BEFORE_RETURN", "OBL_UNSATISFIED_OBLIGATION_EXCEPTION_EDGE" })
  public ResultSet executeQuery(String sql, HikariDataSource dataSource, DBType type, Connection connection) {
    Statement stmt = null;

    try {
      stmt = connection.createStatement();
      ResultSet resultSet = stmt.executeQuery(sql);
      return resultSet;
    } catch (SQLException ex) {
      LOG.error("[SQLSMITH] {} database execution query exception sql : {} ,error : {} ", type, sql, ex);
    }
    return null;
  }

  @SuppressFBWarnings("UCPM_USE_CHARACTER_PARAMETERIZED_METHOD")
  protected List<String> collect(ResultSet resultSet) throws SQLException {
    final List<String> result = new ArrayList<>(10);
    final StringBuilder buf = new StringBuilder();
    while (resultSet.next()) {
      buf.setLength(0);
      int count = resultSet.getMetaData().getColumnCount();
      String sep = "";
      String equalitySign = "=";

      for (int i = 1; i <= count; i++) {
        buf.append(sep)
                .append(resultSet.getMetaData().getColumnLabel(i))
                .append(equalitySign)
                .append(resultSet.getString(i));
        sep = "; ";
      }
      result.add(linuxString(buf.toString()));
    }
    return result;
  }

  public HikariDataSource createDataSource(String user, String password, String host, String port, String database) {
    HikariConfig hikariConfig = new HikariConfig();
    hikariConfig.setJdbcUrl(String.format("jdbc:mysql://%s:%s/%s?useUnicode=true&characterEncoding=gbk",
            host, port, database));
    hikariConfig.setDriverClassName("com.mysql.jdbc.Driver");
    hikariConfig.setUsername(user);
    hikariConfig.setPassword(password);
    hikariConfig.addDataSourceProperty("cachePrepStmts", "true");
    hikariConfig.addDataSourceProperty("prepStmtCacheSize", "250");
    hikariConfig.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
    return new HikariDataSource(hikariConfig);
  }

  public static String linuxString(String str) {
    return str.replaceAll("\r\n", "\n");
  }

  public void close() {
    if (chubaoDataSource != null) {
      chubaoDataSource.close();
    }
    if (mySQLDataSource != null) {
      mySQLDataSource.close();
    }
  }

  /**
   * @version V1.0
   */
  enum DBType {
    CHUBAO, MYSQL
  }
}
