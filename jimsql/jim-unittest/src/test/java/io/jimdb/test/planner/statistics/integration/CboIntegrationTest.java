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

import java.util.Collections;
import java.util.List;

import io.jimdb.core.Bootstraps;
import io.jimdb.core.Session;
import io.jimdb.core.model.meta.MetaData;
import io.jimdb.core.model.meta.Table;
import io.jimdb.core.plugin.PluginFactory;
import io.jimdb.core.plugin.SQLEngine;
import io.jimdb.sql.planner.Planner;
import io.jimdb.sql.operator.RelOperator;
import io.jimdb.sql.optimizer.statistics.TableStats;
import io.jimdb.sql.optimizer.statistics.TableStatsManager;
import io.jimdb.test.mock.store.MockTableData;
import io.jimdb.test.planner.TestBase;
import io.jimdb.sql.server.JimBootstrap;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class CboIntegrationTest extends TestBase {
  private static Table table_user1;
  private static final String CATALOG = "test";
  private static final String dbtable = CATALOG + "." + USER_TABLE;

  @BeforeClass
  public static void setup() throws Exception {
    if (config == null) {
      config = Bootstraps.init("jim_test.properties");
    }
    JimBootstrap.start(JimBootstrap.createServer(config));

    session = new Session(PluginFactory.getSqlEngine(), PluginFactory.getStoreEngine());
    session.getVarContext().setDefaultCatalog(CATALOG);
    session.getVarContext().setAutocommit(true);

    HikariConfig hikariConfig = new HikariConfig();
    hikariConfig.setJdbcUrl(String.format("jdbc:mysql://localhost:%d/%s?useUnicode=true&characterEncoding=gbk",
            config.getServerConfig().getPort(), CATALOG));
    hikariConfig.setDriverClassName("com.mysql.jdbc.Driver");
    hikariConfig.setUsername("root");
    hikariConfig.setPassword("root");
    hikariConfig.addDataSourceProperty("cachePrepStmts", "true");
    hikariConfig.addDataSourceProperty("prepStmtCacheSize", "250");
    hikariConfig.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
    dataSource = new HikariDataSource(hikariConfig);

    // create db and table if not existing yet
    createDbTable();

    // prepare data if not existing yet
    prepareTableData();

    TableStatsManager.init(config);
    planner = new Planner(SQLEngine.DBType.MYSQL);
    table_user1 = MetaData.Holder.get().getTable(CATALOG, USER_TABLE);

    // load all statistics data
    loadStatsData();
  }

  private static void deleteTable(String tableName) {
    try {
      String sql = "DROP TABLE " + tableName;
      SQLHandler.execUpdate(dataSource, sql, 0, true);
      Thread.sleep(1000);
    } catch (Exception e) {
      Assert.fail("Error in deleting table: " + tableName);
    }
  }

  private static void createDbTable() {
    // create database: cbotest
    try {
      String sql = "create database if not exists cbotest";
      //SQLHandler.execUpdate(dataSource, sql, 1, true);
      //Thread.sleep(1000);
    } catch (Exception e) {
      Assert.fail("Error in creating database: cbotest");
    }

    // delete table first
    //deleteTable(USER_TABLE);

    // create user table
    try {
      String sql = "CREATE TABLE IF NOT EXISTS " + USER_TABLE + " ("
              + "`name` varchar(255) NOT NULL, "
              + "`age` int DEFAULT NULL, "
              + "`phone` varchar(11) DEFAULT NULL, "
              + "`score` int DEFAULT NULL, "
              + "`salary` int DEFAULT NULL, "
              + "PRIMARY KEY (`name`), "
              + "INDEX idx_age (age), "
              + "UNIQUE INDEX idx_phone (phone), "
              + "INDEX idx_age_phone (age, phone), "
              + "INDEX idx_salary (salary)"
              + ") COMMENT 'REPLICA=1' ENGINE=MEMORY AUTO_INCREMENT=0 ";
      SQLHandler.execUpdate(dataSource, sql, 0, true);
      Thread.sleep(1000);
    } catch (Exception e) {
      Assert.fail("Error in creating table: user");
    }
  }

  private static void prepareTableData() {
    // prepare data for the user table
    try {
      List<String> result = SQLHandler.execQuery(dataSource, "select name from user where name='Tom0'");
      if (result.size() == 0) {
        String insert = "INSERT INTO user VALUES";
        String values = "(\"V1\", V2, \"V3\", V4, V5),";

        // We need more than 1000 rows to trigger the row count calculation in DS
        int count = 0;
        for (int i = 0; i < 120; i++) {
          String vals = "";
          for (MockTableData.User user : MockTableData.getUserDataList()) {
            String val = values.replace("V1", user.getName() + i + "");
            val = val.replace("V2", user.getAge() + "");
            String phone = user.getPhone().substring(0, 7) + (1000 + i);
            val = val.replace("V3", phone);
            val = val.replace("V4", user.getScore() + "");
            val = val.replace("V5", user.getSalary() + "");
            vals += val;
          }
          String sql = insert + vals.substring(0, vals.length()-1);
          SQLHandler.execUpdate(dataSource, sql, 10, true);
          count += 10;
          if (count % 100 == 0) {
            System.out.println("Finished data insertion, count = " + count);
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static void loadStatsData() {
    TableStatsManager.updateTableStatsCache(Collections.singleton(table_user1));
    try {
      // sleep 3s for all the statistics data to be loaded
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testDataSelect() {
    List<String> result = SQLHandler.execQuery(dataSource,"select name from user");
    Assert.assertEquals(1200, result.size());
  }

  @Test
  public void testUpdateAllTables() {
    loadStatsData();
    TableStats tableStats1 = TableStatsManager.getTableStats(table_user1);
    Assert.assertEquals("Row count in table stats should be equal to the actual row count",
            1200, tableStats1.getEstimatedRowCount());
  }

  private void run(Checker checker) {
    RelOperator physicalPlan = buildPlanAndOptimize(checker.getSql());
    Assert.assertNotNull(physicalPlan);
    checker.doCheck(physicalPlan);
  }

  @Test
  public void testCBOWithTwoIndexFilters() {
    TableStatsManager.setEnableStatsPushDown(false);
    Checker checker = Checker.build()
            .sql("select name, age from user where age > 20 and salary > 7000")
            .addCheckPoint(CheckPoint.PlanTree, "IndexSource(user) -> TableSource(user) -> "
                    + "Selection(GreaterInt(" + CATALOG +".user.age,20)) -> IndexLookUp -> Projection")
            .addCheckPoint(CheckPoint.PushDownIndexPlan, "IndexSource(user.idx_salary)");

    run(checker);
  }

  @Test
  public void testCBOWithSingleIndexFilter() {
    TableStatsManager.setEnableStatsPushDown(false);
    Checker checker = Checker.build()
            .sql("select name, age, score from user where age > 20")
            .addCheckPoint(CheckPoint.PlanTree,
                    "TableSource(user)")
            .addCheckPoint(CheckPoint.TablePlan,
                    "TableSource={columns=[name,age,score], "
                              + "pushDownPredicates=[GreaterInt(" + CATALOG + ".user.age,20)]}");

    run(checker);
  }

  @Test
  public void testCBOWithIndexFilterAndGrby() {
    TableStatsManager.setEnableStatsPushDown(false);
    Checker checker = Checker.build()
            .sql("select count(1) from user where phone = '13010010000' group by score")
            .addCheckPoint(CheckPoint.PlanTree,
                    "IndexSource(user) -> TableSource(user) -> HashAgg(COUNT(1)) -> "
                            + "IndexLookUp -> HashAgg(COUNT(col_0))")
            .addCheckPoint(CheckPoint.PushDownIndexPlan,
                    "IndexSource(user.idx_phone)");

    run(checker);
  }

  @Test
  public void testFastAnalyzeStats() {
    TableStatsManager.setEnableStatsPushDown(false);
    List<String> result = SQLHandler.execQuery(dataSource, "analyze table user");
    Assert.assertEquals("result size should be equal.", 11, result.size());
//    Assert.assertEquals("result of row 0 should be equal.", testAnalyzeRow0, result.get(0));
//    Assert.assertEquals("result of row 1 should be equal.", testAnalyzeRow1, result.get(1));
////    Assert.assertEquals("result of row 2 should be equal.", testAnalyzeRow2, result.get(2));
//    Assert.assertEquals("result of row 3 should be equal.", testAnalyzeRow3, result.get(3));
////    Assert.assertEquals("result of row 4 should be equal.", testAnalyzeRow4, result.get(4));
//    Assert.assertEquals("result of row 5 should be equal.", testAnalyzeRow5, result.get(5));
//    Assert.assertEquals("result of row 6 should be equal.", testAnalyzeRow6, result.get(6));
  }

  @Test
  public void testAnalyzeStatsPushDown() {
    TableStatsManager.setEnableStatsPushDown(true);
    List<String> result = SQLHandler.execQuery(dataSource, "analyze table user");
    Assert.assertEquals("result size should be equal.", 11, result.size());
  }
}

