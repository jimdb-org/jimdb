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
package io.jimdb.test.mysql.ddl;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.jimdb.common.exception.JimException;
import io.jimdb.common.utils.lang.ByteUtil;
import io.jimdb.common.utils.lang.IOUtil;
import io.jimdb.core.config.JimConfig;
import io.jimdb.core.model.meta.Catalog;
import io.jimdb.core.model.meta.Index;
import io.jimdb.core.model.meta.MetaData;
import io.jimdb.core.model.meta.Table;
import io.jimdb.core.plugin.MetaStore;
import io.jimdb.core.plugin.RouterStore;
import io.jimdb.meta.EtcdMetaStore;
import io.jimdb.meta.client.MasterClient;
import io.jimdb.pb.Basepb;
import io.jimdb.pb.Metapb;
import io.jimdb.pb.Mspb;
import io.jimdb.sql.ddl.DDLUtils;
import io.jimdb.test.mysql.SqlTestBase;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.protobuf.ByteString;

/**
 * @version V1.0
 */
public final class TableTest extends SqlTestBase {
  private static final String OVER_NAME;
  private static final String DB_NAME;
  private static final MetaStore metaStore;
  private static final RouterStore routerStore;

  static {
    StringBuilder builder = new StringBuilder(128);
    for (int i = 0; i < 128; i++) {
      builder.append("a");
    }
    OVER_NAME = builder.toString();
    DB_NAME = "ddl_test_db" + System.nanoTime();

    JimConfig config = new JimConfig(IOUtil.loadResource("jim_test.properties"));
    metaStore = new EtcdMetaStore();
    metaStore.init(config);

    routerStore = new MasterClient();
    routerStore.init(config);
  }

  @Before
  public void tearUp() {
    createCatalog(DB_NAME);

    String sql = String.format("use %s", DB_NAME);
    execUpdate(sql, 0, true);
  }

  @After
  public void tearDown() {
    deleteCatalog(DB_NAME);
  }

  @Test
  public void testCreateNameError() {
    String sql = "CREATE TABLE nul999.test(id BIGINT PRIMARY KEY) ENGINE=MEMORY";
    SQLException result = new SQLException("Unknown database 'nul999'", "42000", 1049);
    execUpdate(sql, result, true);

    sql = "CREATE TABLE 'test@'(id BIGINT) ENGINE=MEMORY";
    result = new SQLException("Incorrect table name 'test@'", "42000", 1103);
    execUpdate(sql, result, true);

    sql = String.format("CREATE TABLE %s(id BIGINT) ENGINE=MEMORY", OVER_NAME);
    result = new SQLException(String.format("Identifier name '%s' is too long", OVER_NAME), "42000", 1059);
    execUpdate(sql, result, true);
  }

  @Test
  public void testCreateWithoutKey() {
    String sql = "CREATE TABLE ddl_test_tbl0(user varchar(32), host varchar(32)) COMMENT 'REPLICA=1' ENGINE=MEMORY";
    SQLException result = new SQLException("This table type requires a primary key", "42000", 1173);
    execUpdate(sql, result, true);
  }

  @Test
  public void testCreateDup() {
    String sql = "CREATE TABLE ddl_test_tbl0(id BIGINT PRIMARY KEY) COMMENT 'REPLICA=1' ENGINE=MEMORY";
    execUpdate(sql, 0, true);

    sql = "CREATE TABLE ddl_test_tbl0(id BIGINT PRIMARY KEY) COMMENT 'REPLICA=1' ENGINE=MEMORY";
    SQLException result = new SQLException("Table 'ddl_test_tbl0' already exists", "42S01", 1050);
    execUpdate(sql, result, true);

    sql = "CREATE TABLE IF NOT EXISTS ddl_test_tbl0(id BIGINT PRIMARY KEY) COMMENT 'REPLICA=1' ENGINE=MEMORY";
    execUpdate(sql, 0, true);
  }

  @Test
  public void testCreate() {
    String sql = "CREATE TABLE `message_retry` (" +
            " `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键'," +
            " `message_id` varchar(50) NOT NULL COMMENT '消息编号'," +
            " `business_id` varchar(100) DEFAULT NULL COMMENT '业务编号'," +
            " `topic` varchar(100) NOT NULL COMMENT '主题'," +
            " `app` varchar(100) NOT NULL COMMENT '应用'," +
            " `send_time` datetime NOT NULL COMMENT '发送时间'," +
            " `expire_time` datetime NOT NULL COMMENT '过期时间'," +
            " `retry_time` datetime NOT NULL COMMENT '重试时间'," +
            " `retry_count` int(10) NOT NULL DEFAULT '0' COMMENT '重试次数'," +
            " `data` text NOT NULL COMMENT '消息体'," +
            " `exception` text COMMENT '异常信息'," +
            " `create_time` datetime NOT NULL COMMENT '创建时间'," +
            " `create_by` int(10) NOT NULL DEFAULT '0' COMMENT '创建人'," +
            " `update_time` datetime NOT NULL COMMENT '更新时间'," +
            " `update_by` int(10) NOT NULL DEFAULT '0' COMMENT '更新人'," +
            " `status` tinyint(4) NOT NULL DEFAULT '1' COMMENT '状态,0:成功,1:失败,-2:过期'," +
            " PRIMARY KEY (`id`)," +
            " KEY `idx_topic_app` (`topic`, `app`, `status`, `retry_time`)" +
            ") ENGINE = MEMORY AUTO_INCREMENT = 100  COMMENT 'REPLICA=1;消息重试表'";
    execUpdate(sql, 0, true);

    int found = 0;
    Metapb.TableInfo tableInfo = null;
    Map<Metapb.CatalogInfo, List<Metapb.TableInfo>> metas = metaStore.getCatalogAndTables();
    for (Map.Entry<Metapb.CatalogInfo, List<Metapb.TableInfo>> entry : metas.entrySet()) {
      if (!entry.getKey().getName().equalsIgnoreCase(DB_NAME)) {
        continue;
      }

      for (Metapb.TableInfo table : entry.getValue()) {
        if (table.getName().equalsIgnoreCase("message_retry")) {
          found++;
          tableInfo = table;
        }
      }
    }
    Assert.assertEquals(1, found);
    Assert.assertEquals(2, tableInfo.getIndicesCount());
    Assert.assertEquals(16, tableInfo.getColumnsCount());

    List<Metapb.ColumnInfo> columns = tableInfo.getColumnsList();
    Assert.assertEquals("id", columns.get(0).getName().toLowerCase());
    Assert.assertEquals("message_id", columns.get(1).getName().toLowerCase());
    Assert.assertEquals("business_id", columns.get(2).getName().toLowerCase());
    Assert.assertEquals("topic", columns.get(3).getName().toLowerCase());
    Assert.assertEquals("app", columns.get(4).getName().toLowerCase());
    Assert.assertEquals("send_time", columns.get(5).getName().toLowerCase());
    Assert.assertEquals("expire_time", columns.get(6).getName().toLowerCase());
    Assert.assertEquals("retry_time", columns.get(7).getName().toLowerCase());
    Assert.assertEquals("retry_count", columns.get(8).getName().toLowerCase());
    Assert.assertEquals("data", columns.get(9).getName().toLowerCase());
    Assert.assertEquals("exception", columns.get(10).getName().toLowerCase());
    Assert.assertEquals("create_time", columns.get(11).getName().toLowerCase());
    Assert.assertEquals("create_by", columns.get(12).getName().toLowerCase());
    Assert.assertEquals("update_time", columns.get(13).getName().toLowerCase());
    Assert.assertEquals("update_by", columns.get(14).getName().toLowerCase());
    Assert.assertEquals("status", columns.get(15).getName().toLowerCase());

    Metapb.IndexInfo primaryIndex = tableInfo.getIndices(0);
    Assert.assertEquals("primary", primaryIndex.getName().toLowerCase());
    Assert.assertEquals(1, primaryIndex.getColumnsCount());
    Assert.assertEquals(columns.get(0).getId(), primaryIndex.getColumnsList().get(0).intValue());

    Metapb.IndexInfo secondIndex = tableInfo.getIndices(1);
    Assert.assertEquals("idx_topic_app", secondIndex.getName().toLowerCase());
    Assert.assertEquals(4, secondIndex.getColumnsCount());
    Assert.assertEquals(columns.get(3).getId(), secondIndex.getColumnsList().get(0).intValue());
    Assert.assertEquals(columns.get(4).getId(), secondIndex.getColumnsList().get(1).intValue());
    Assert.assertEquals(columns.get(15).getId(), secondIndex.getColumnsList().get(2).intValue());
    Assert.assertEquals(columns.get(7).getId(), secondIndex.getColumnsList().get(3).intValue());

    verifyRoute(tableInfo);
  }

  @Test
  public void testCreateWithAutoID() {
    String sql = "CREATE TABLE ddl_test_tbl0(id BIGINT AUTO_INCREMENT PRIMARY KEY,  user varchar(32), host varchar(32)) COMMENT 'REPLICA=1' ENGINE=MEMORY";
    SQLException result = new SQLException("This version of MySQL doesn't yet support 'auto_increment signed'", "42000", 1235);
    execUpdate(sql, result, true);

    sql = "CREATE TABLE ddl_test_tbl0(id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,  user varchar(32), host varchar(32)) COMMENT 'REPLICA=1' ENGINE=MEMORY";
    execUpdate(sql, 0, true);

    int found = 0;
    Metapb.TableInfo tableInfo = null;
    Map<Metapb.CatalogInfo, List<Metapb.TableInfo>> metas = metaStore.getCatalogAndTables();
    for (Map.Entry<Metapb.CatalogInfo, List<Metapb.TableInfo>> entry : metas.entrySet()) {
      if (!entry.getKey().getName().equalsIgnoreCase(DB_NAME)) {
        continue;
      }

      for (Metapb.TableInfo table : entry.getValue()) {
        if (table.getName().equalsIgnoreCase("ddl_test_tbl0")) {
          found++;
          tableInfo = table;
        }
      }
    }
    Assert.assertEquals(1, found);
    Assert.assertEquals(1, tableInfo.getIndicesCount());
    Assert.assertEquals(3, tableInfo.getColumnsCount());

    List<Metapb.ColumnInfo> columns = tableInfo.getColumnsList();
    Metapb.IndexInfo primaryIndex = tableInfo.getIndicesList().get(0);
    Assert.assertEquals("id", columns.get(0).getName().toLowerCase());
    Assert.assertEquals("user", columns.get(1).getName().toLowerCase());
    Assert.assertEquals("host", columns.get(2).getName().toLowerCase());
    Assert.assertEquals("primary", primaryIndex.getName().toLowerCase());
    Assert.assertEquals(1, primaryIndex.getColumnsCount());
    Assert.assertEquals(columns.get(0).getId(), primaryIndex.getColumnsList().get(0).intValue());

    verifyRoute(tableInfo);
  }

  @Test
  public void testCreateWithIndex() {
    String sql = "CREATE TABLE ddl_test_tbl0(id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,  user varchar(32) NOT NULL UNIQUE KEY default 'jd', host varchar(32), INDEX idxTest(host)) COMMENT 'REPLICA=1' ENGINE=MEMORY";
    execUpdate(sql, 0, true);

    int found = 0;
    Metapb.TableInfo tableInfo = null;
    Map<Metapb.CatalogInfo, List<Metapb.TableInfo>> metas = metaStore.getCatalogAndTables();
    for (Map.Entry<Metapb.CatalogInfo, List<Metapb.TableInfo>> entry : metas.entrySet()) {
      if (!entry.getKey().getName().equalsIgnoreCase(DB_NAME)) {
        continue;
      }

      for (Metapb.TableInfo table : entry.getValue()) {
        if (table.getName().equalsIgnoreCase("ddl_test_tbl0")) {
          found++;
          tableInfo = table;
        }
      }
    }
    Assert.assertEquals(1, found);
    Assert.assertEquals(3, tableInfo.getIndicesCount());
    Assert.assertEquals(3, tableInfo.getColumnsCount());

    List<Metapb.ColumnInfo> columns = tableInfo.getColumnsList();
    Assert.assertEquals("id", columns.get(0).getName().toLowerCase());
    Assert.assertEquals("user", columns.get(1).getName().toLowerCase());
    Assert.assertEquals("host", columns.get(2).getName().toLowerCase());

    for (Metapb.IndexInfo indexInfo : tableInfo.getIndicesList()) {
      if (indexInfo.getPrimary()) {
        Assert.assertEquals("primary", indexInfo.getName().toLowerCase());
        Assert.assertEquals(1, indexInfo.getColumnsCount());
        Assert.assertEquals(columns.get(0).getId(), indexInfo.getColumnsList().get(0).intValue());
      } else if (indexInfo.getUnique()) {
        Assert.assertEquals("user_2", indexInfo.getName().toLowerCase());
        Assert.assertEquals(1, indexInfo.getColumnsCount());
        Assert.assertEquals(columns.get(1).getId(), indexInfo.getColumnsList().get(0).intValue());
      } else {
        Assert.assertEquals("idxtest", indexInfo.getName().toLowerCase());
        Assert.assertEquals(1, indexInfo.getColumnsCount());
        Assert.assertEquals(columns.get(2).getId(), indexInfo.getColumnsList().get(0).intValue());
      }
    }

    verifyRoute(tableInfo);
  }

  @Test
  public void testCreateWithPartitionError() {
    String sql = "CREATE TABLE ddl_test_tbl0(name varchar(32) PRIMARY KEY,  user varchar(32) NOT NULL UNIQUE KEY default 'jd', host varchar(32), INDEX idxTest(host)) COMMENT 'REPLICA=1' ENGINE=MEMORY partition BY HASH(id) PARTITIONS 3 (" +
            "PARTITION p5 VALUES LESS THAN MAXVALUE)";
    SQLException result = new SQLException("This version of MySQL doesn't yet support 'Non-Range Partition Type'", "42000", 1235);
    execUpdate(sql, result, true);

    sql = "CREATE TABLE ddl_test_tbl0(name varchar(32) PRIMARY KEY,  user varchar(32) NOT NULL UNIQUE KEY default 'jd', host varchar(32), INDEX idxTest(host)) COMMENT 'REPLICA=1' ENGINE=MEMORY partition BY RANGE(id) PARTITIONS 3 (" +
            "PARTITION p1 VALUES IN('a','b','c'))";
    result = new SQLException("Unsupported partition by multi columns and Non-Primary Key", "HY000", 1105);
    execUpdate(sql, result, true);

    sql = "CREATE TABLE ddl_test_tbl0(name varchar(32) PRIMARY KEY,  user varchar(32) NOT NULL UNIQUE KEY default 'jd', host varchar(32), INDEX idxTest(host)) COMMENT 'REPLICA=1' ENGINE=MEMORY partition BY RANGE(name) PARTITIONS 3 (" +
            "PARTITION p1 VALUES IN('a','b','c'))";
    result = new SQLException("Unsupported partition multi values and operator Non-Less Than", "HY000", 1105);
    execUpdate(sql, result, true);

    sql = "CREATE TABLE ddl_test_tbl0(name datetime PRIMARY KEY,  user varchar(32) NOT NULL UNIQUE KEY default 'jd', host varchar(32), INDEX idxTest(host)) COMMENT 'REPLICA=1' ENGINE=MEMORY partition BY RANGE(name) PARTITIONS 3 (" +
            "PARTITION p5 VALUES LESS THAN ('2010-10-12'))";
    result = new SQLException("Unsupported partition data type DateTime", "HY000", 1105);
    execUpdate(sql, result, true);
  }

  @Test
  public void testCreateWithPartition() {
    String sql = "CREATE TABLE ddl_test_tbl0(name varchar(32) PRIMARY KEY,  user varchar(32) NOT NULL UNIQUE KEY default 'jd', host varchar(32), INDEX idxTest(host)) COMMENT 'REPLICA=1' ENGINE=MEMORY partition BY RANGE(name) PARTITIONS 3 (" +
            "PARTITION p1 VALUES LESS THAN ('abc')," +
            "PARTITION p2 VALUES LESS THAN ('jde')," +
            "PARTITION p3 VALUES LESS THAN ('e')," +
            "PARTITION p4 VALUES LESS THAN MAXVALUE)";
    execUpdate(sql, 0, true);

    int found = 0;
    Metapb.TableInfo tableInfo = null;
    Map<Metapb.CatalogInfo, List<Metapb.TableInfo>> metas = metaStore.getCatalogAndTables();
    for (Map.Entry<Metapb.CatalogInfo, List<Metapb.TableInfo>> entry : metas.entrySet()) {
      if (!entry.getKey().getName().equalsIgnoreCase(DB_NAME)) {
        continue;
      }

      for (Metapb.TableInfo table : entry.getValue()) {
        if (table.getName().equalsIgnoreCase("ddl_test_tbl0")) {
          found++;
          tableInfo = table;
        }
      }
    }
    Assert.assertEquals(1, found);
    Assert.assertEquals(3, tableInfo.getIndicesCount());
    Assert.assertEquals(3, tableInfo.getColumnsCount());

    List<Metapb.ColumnInfo> columns = tableInfo.getColumnsList();
    Assert.assertEquals("name", columns.get(0).getName().toLowerCase());
    Assert.assertEquals("user", columns.get(1).getName().toLowerCase());
    Assert.assertEquals("host", columns.get(2).getName().toLowerCase());

    for (Metapb.IndexInfo indexInfo : tableInfo.getIndicesList()) {
      if (indexInfo.getPrimary()) {
        Assert.assertEquals("primary", indexInfo.getName().toLowerCase());
        Assert.assertEquals(1, indexInfo.getColumnsCount());
        Assert.assertEquals(columns.get(0).getId(), indexInfo.getColumnsList().get(0).intValue());
      } else if (indexInfo.getUnique()) {
        Assert.assertEquals("user_2", indexInfo.getName().toLowerCase());
        Assert.assertEquals(1, indexInfo.getColumnsCount());
        Assert.assertEquals(columns.get(1).getId(), indexInfo.getColumnsList().get(0).intValue());
      } else {
        Assert.assertEquals("idxtest", indexInfo.getName().toLowerCase());
        Assert.assertEquals(1, indexInfo.getColumnsCount());
        Assert.assertEquals(columns.get(2).getId(), indexInfo.getColumnsList().get(0).intValue());
      }
    }

    verifyRoute(tableInfo);
  }

  @Test
  public void testDropNameError() {
    String sql = "Drop TABLE nul999.test";
    SQLException result = new SQLException("Unknown database 'nul999'", "42000", 1049);
    execUpdate(sql, result, true);

    sql = "Drop TABLE 'test@'";
    result = new SQLException("Incorrect table name 'test@'", "42000", 1103);
    execUpdate(sql, result, true);

    sql = String.format("Drop TABLE %s", OVER_NAME);
    result = new SQLException(String.format("Identifier name '%s' is too long", OVER_NAME), "42000", 1059);
    execUpdate(sql, result, true);
  }

  @Test
  public void testDropWithExists() {
    String sql = "Drop TABLE nul888";
    SQLException result = new SQLException("Unknown table '[nul888]'", "42S02", 1051);
    execUpdate(sql, result, true);

    sql = "Drop TABLE nul888,nul999,nul666";
    result = new SQLException("Unknown table '[nul888, nul999, nul666]'", "42S02", 1051);
    execUpdate(sql, result, true);

    sql = "CREATE TABLE ddl_test_tbl0(user varchar(32), host varchar(32), PRIMARY KEY(user,host)) COMMENT 'REPLICA=1' ENGINE=MEMORY";
    execUpdate(sql, 0, true);
    sql = "Drop TABLE nul888,nul999,ddl_test_tbl0,nul555,nul666";
    result = new SQLException("Unknown table '[nul888, nul999, nul555, nul666]'", "42S02", 1051);
    execUpdate(sql, result, true);

    sql = "Drop TABLE IF EXISTS nul888,nul999,ddl_test_tbl0,nul555,nul666";
    execUpdate(sql, 0, true);

    sql = "Drop TABLE IF EXISTS dbnull.nul888";
    execUpdate(sql, 0, true);
  }

  @Test
  public void testDrop() {
    createTable("ddl_test_tbl1");
    createTable("ddl_test_tbl2");
    createTable("ddl_test_tbl3");
    createTable("ddl_test_tbl4");

    List<Metapb.TableInfo> dropTables = new ArrayList<>(2);
    Map<Metapb.CatalogInfo, List<Metapb.TableInfo>> metas = metaStore.getCatalogAndTables();
    for (Map.Entry<Metapb.CatalogInfo, List<Metapb.TableInfo>> entry : metas.entrySet()) {
      if (!entry.getKey().getName().equalsIgnoreCase(DB_NAME)) {
        continue;
      }

      for (Metapb.TableInfo table : entry.getValue()) {
        if (table.getName().equalsIgnoreCase("ddl_test_tbl2") || table.getName().equalsIgnoreCase("ddl_test_tbl4")) {
          dropTables.add(table);
        }
      }
    }

    String sql = "Drop TABLE IF EXISTS ddl_test_tbl2,nul999,ddl_test_tbl4";
    execUpdate(sql, 0, true);

    metas = metaStore.getCatalogAndTables();
    for (Map.Entry<Metapb.CatalogInfo, List<Metapb.TableInfo>> entry : metas.entrySet()) {
      if (!entry.getKey().getName().equalsIgnoreCase(DB_NAME)) {
        continue;
      }

      for (Metapb.TableInfo table : entry.getValue()) {
        Assert.assertNotEquals(table.getName().toLowerCase(), "ddl_test_tbl2");
        Assert.assertNotEquals(table.getName().toLowerCase(), "ddl_test_tbl4");
      }
    }

    for (Metapb.TableInfo table : dropTables) {
      try {
        routerStore.getRoute(table.getDbId(), table.getId(), null, 3);
      } catch (JimException ex) {
        Assert.assertEquals(1051, ex.getCode().getCode());
        Assert.assertEquals(String.format("Unknown table '%s'", table.getId()), ex.getMessage());
      }
    }

    dropTables.add(verifyTable("ddl_test_tbl1"));
    dropTables.add(verifyTable("ddl_test_tbl3"));

    deleteCatalog(DB_NAME);

    metas = metaStore.getCatalogAndTables();
    for (Map.Entry<Metapb.CatalogInfo, List<Metapb.TableInfo>> entry : metas.entrySet()) {
      Assert.assertNotEquals(DB_NAME, entry.getKey().getName().toLowerCase());
    }
    for (Metapb.TableInfo table : dropTables) {
      try {
        routerStore.getRoute(table.getDbId(), table.getId(), null, 3);
      } catch (JimException ex) {
        Assert.assertEquals(1049, ex.getCode().getCode());
        Assert.assertEquals(String.format("Unknown database '%s'", table.getDbId()), ex.getMessage());
      }
    }
  }

  @Test
  public void testRename() {
    String sql = "RENAME TABLE ddl_test_tbl1 TO ddl_test_tbl12, ddl_test_tbl2 TO ddl_test_tbl22";
    SQLException result = new SQLException("This version of MySQL doesn't yet support 'schema batch rename table'", "42000", 1235);
    execUpdate(sql, result, true);

    sql = "RENAME TABLE db1.ddl_test_tbl1 TO db2.ddl_test_tbl12";
    result = new SQLException("This version of MySQL doesn't yet support 'schema rename table between different databases'", "42000", 1235);
    execUpdate(sql, result, true);

    sql = "RENAME TABLE ddl_test_tbl1 TO ddl_test_tbl1";
    result = new SQLException("This version of MySQL doesn't yet support 'schema rename table to the same name'", "42000", 1235);
    execUpdate(sql, result, true);

    sql = "RENAME TABLE ddl_test_tbl1 TO ddl_test_tbl2";
    result = new SQLException("Unknown table 'ddl_test_tbl1'", "42S02", 1051);
    execUpdate(sql, result, true);

    createTable("ddl_test_tbl1");
    createTable("ddl_test_tbl2");

    sql = "RENAME TABLE ddl_test_tbl1 TO ddl_test_tbl2";
    result = new SQLException("Table 'ddl_test_tbl2' already exists", "42S01", 1050);
    execUpdate(sql, result, true);

    sql = "RENAME TABLE ddl_test_tbl1 TO ddl_test_tbl11";
    execUpdate(sql, 0, true);

    Catalog catalog = MetaData.Holder.get().getCatalog(DB_NAME);
    Table[] tables = catalog.getTables();
    Assert.assertEquals(2, tables.length);

    int found = 0;
    for (Table table : tables) {
      if (table.getName().equalsIgnoreCase("ddl_test_tbl2") || table.getName().equalsIgnoreCase("ddl_test_tbl11")) {
        found++;
      }
    }
    Assert.assertEquals(2, found);

    sql = "ALTER TABLE ddl_test_tbl11 RENAME TO ddl_test_tbl112";
    execUpdate(sql, 0, true);

    catalog = MetaData.Holder.get().getCatalog(DB_NAME);
    tables = catalog.getTables();
    Assert.assertEquals(2, tables.length);

    found = 0;
    for (Table table : tables) {
      if (table.getName().equalsIgnoreCase("ddl_test_tbl2") || table.getName().equalsIgnoreCase("ddl_test_tbl112")) {
        found++;
      }
    }
    Assert.assertEquals(2, found);
  }

  @Test
  public void testRenameIndex() {
    createTable("ddl_test_tbl1");
    String sql = "ALTER TABLE ddl_test_tbl1 RENAME INDEX idxTest TO idxTest";
    SQLException result = new SQLException("This version of MySQL doesn't yet support 'schema rename index to the same name'", "42000", 1235);
    execUpdate(sql, result, true);

    sql = "ALTER TABLE ddl_test_tbl1 RENAME INDEX idxnull TO idxRename";
    result = new SQLException("Key 'idxnull' doesn't exist in table 'ddl_test_tbl1'", "42000", 1176);
    execUpdate(sql, result, true);

    sql = "ALTER TABLE ddl_test_tbl1 RENAME INDEX idxTest TO user_2";
    result = new SQLException("Duplicate key name 'user_2'", "42000", 1061);
    execUpdate(sql, result, true);

    sql = "ALTER TABLE ddl_test_tbl1 RENAME INDEX idxTest TO idxRename";
    execUpdate(sql, 0, true);
    Table table = MetaData.Holder.get().getTable(DB_NAME, "ddl_test_tbl1");
    for (Index index : table.getReadableIndices()) {
      if (index.isPrimary()) {
        Assert.assertEquals("primary", index.getName().toLowerCase());
        Assert.assertEquals(1, index.getColumns().length);
        Assert.assertEquals("name", index.getColumns()[0].getName());
      } else if (index.isUnique()) {
        Assert.assertEquals("user_2", index.getName().toLowerCase());
        Assert.assertEquals(1, index.getColumns().length);
        Assert.assertEquals("user", index.getColumns()[0].getName());
      } else {
        Assert.assertEquals("idxRename", index.getName());
        Assert.assertEquals(1, index.getColumns().length);
        Assert.assertEquals("host", index.getColumns()[0].getName());
      }
    }
  }

  @Test
  public void testDropIndex() throws Exception {
    createTable("ddl_test_tbl1");
    String sql = "DROP INDEX idxnull ON ddl_test_tbl1";
    SQLException result = new SQLException("Key 'idxnull' doesn't exist in table 'ddl_test_tbl1'", "42000", 1176);
    execUpdate(sql, result, true);

    Table table = MetaData.Holder.get().getTable(DB_NAME, "ddl_test_tbl1");
    Index index = null;
    for (Index idx : table.getReadableIndices()) {
      if ("idxTest".equalsIgnoreCase(idx.getName())) {
        index = idx;
        break;
      }
    }

    sql = "DROP INDEX idxTest ON ddl_test_tbl1";
    execUpdate(sql, 0, true);

    table = MetaData.Holder.get().getTable(DB_NAME, "ddl_test_tbl1");
    Assert.assertEquals(2, table.getReadableIndices().length);
    int found = 0;
    for (Index idx : table.getReadableIndices()) {
      if ("primary".equalsIgnoreCase(idx.getName()) || "user_2".equalsIgnoreCase(idx.getName())) {
        found++;
      }
    }
    Assert.assertEquals(2, found);

    Mspb.GetRouteResponse routers = routerStore.getRoute(table.getCatalogId(), table.getId(), null, 3);
    for (Basepb.Range range : routers.getRoutesList()) {
      Assert.assertNotEquals(index.getId().intValue(), range.getIndexId());
    }

    Field field = table.getClass().getDeclaredField("tableInfo");
    field.setAccessible(true);
    verifyRoute((Metapb.TableInfo) field.get(table));
  }

  private void verifyRoute(Metapb.TableInfo tableInfo) {
    Mspb.GetRouteResponse routes = routerStore.getRoute(tableInfo.getDbId(), tableInfo.getId(), null, 3);
    List<Basepb.Range> ranges = routes.getRoutesList();
    Map<Integer, Metapb.ColumnInfo> colMap = new HashMap<>();
    for (Metapb.ColumnInfo column : tableInfo.getColumnsList()) {
      colMap.put(column.getId(), column);
    }

    verifyTableRoute(tableInfo, colMap, ranges);
    for (Metapb.IndexInfo indexInfo : tableInfo.getIndicesList()) {
      if (indexInfo.getPrimary()) {
        continue;
      }
      verifyIndexRoute(tableInfo, indexInfo, colMap, ranges);
    }
  }

  private void verifyTableRoute(Metapb.TableInfo tableInfo, Map<Integer, Metapb.ColumnInfo> colMap, List<Basepb.Range> ranges) {
    List<Basepb.Range> tableRanges = new ArrayList<>(ranges.size());
    for (Basepb.Range range : ranges) {
      if (range.getRangeType() == Basepb.RangeType.RNG_Data) {
        tableRanges.add(range);
      }
    }
    verifyRange(tableRanges, DDLUtils.buildTableRange(tableInfo, colMap));
  }

  private void verifyIndexRoute(Metapb.TableInfo tableInfo, Metapb.IndexInfo
          indexInfo, Map<Integer, Metapb.ColumnInfo> colMap, List<Basepb.Range> ranges) {
    List<Basepb.Range> indexRanges = new ArrayList<>(ranges.size());
    for (Basepb.Range range : ranges) {
      if (range.getRangeType() == Basepb.RangeType.RNG_Index && range.getIndexId() == indexInfo.getId()) {
        indexRanges.add(range);
      }
    }
    verifyRange(indexRanges, DDLUtils.buildIndexRange(tableInfo, colMap, Collections.singletonList(indexInfo)));
  }

  private void verifyRange(List<Basepb.Range> ranges, List<Basepb.Range> expectRanges) {
    ByteString nextKey = null;
    Assert.assertEquals(expectRanges.size(), ranges.size());
    Collections.sort(ranges, (o1, o2) -> ByteUtil.compare(o1.getStartKey(), o2.getStartKey()));
    for (int i = 0; i < ranges.size(); i++) {
      Basepb.Range range = ranges.get(i);
      Basepb.Range expectRange = expectRanges.get(i);
      Assert.assertEquals(expectRange.getDbId(), range.getDbId());
      Assert.assertEquals(expectRange.getTableId(), range.getTableId());
      Assert.assertEquals(expectRange.getIndexId(), range.getIndexId());
      Assert.assertEquals(expectRange.getRangeType(), range.getRangeType());
      Assert.assertEquals(expectRange.getStoreType(), range.getStoreType());
      Assert.assertEquals(expectRange.getStartKey(), range.getStartKey());
      Assert.assertEquals(expectRange.getEndKey(), range.getEndKey());
      if (nextKey != null) {
        Assert.assertEquals(nextKey, range.getStartKey());
      }
      nextKey = range.getEndKey();
    }
  }

  private void createTable(String name) {
    String sql = "CREATE TABLE " + name + "(name varchar(32) PRIMARY KEY,  user varchar(32) NOT NULL UNIQUE KEY default 'jd', host varchar(32), INDEX idxTest(host)) COMMENT 'REPLICA=1' ENGINE=MEMORY partition BY RANGE(name) PARTITIONS 3 (" +
            "PARTITION p1 VALUES LESS THAN ('abc')," +
            "PARTITION p2 VALUES LESS THAN ('jde')," +
            "PARTITION p3 VALUES LESS THAN ('e')," +
            "PARTITION p4 VALUES LESS THAN MAXVALUE)";
    execUpdate(sql, 0, true);

    verifyTable(name);
  }

  private Metapb.TableInfo verifyTable(String name) {
    int found = 0;
    Metapb.TableInfo tableInfo = null;
    Map<Metapb.CatalogInfo, List<Metapb.TableInfo>> metas = metaStore.getCatalogAndTables();
    for (Map.Entry<Metapb.CatalogInfo, List<Metapb.TableInfo>> entry : metas.entrySet()) {
      if (!entry.getKey().getName().equalsIgnoreCase(DB_NAME)) {
        continue;
      }

      for (Metapb.TableInfo table : entry.getValue()) {
        if (table.getName().equalsIgnoreCase(name)) {
          found++;
          tableInfo = table;
        }
      }
    }
    Assert.assertEquals(1, found);
    Assert.assertEquals(3, tableInfo.getIndicesCount());
    Assert.assertEquals(3, tableInfo.getColumnsCount());

    List<Metapb.ColumnInfo> columns = tableInfo.getColumnsList();
    Assert.assertEquals("name", columns.get(0).getName().toLowerCase());
    Assert.assertEquals("user", columns.get(1).getName().toLowerCase());
    Assert.assertEquals("host", columns.get(2).getName().toLowerCase());

    for (Metapb.IndexInfo indexInfo : tableInfo.getIndicesList()) {
      if (indexInfo.getPrimary()) {
        Assert.assertEquals("primary", indexInfo.getName().toLowerCase());
        Assert.assertEquals(1, indexInfo.getColumnsCount());
        Assert.assertEquals(columns.get(0).getId(), indexInfo.getColumnsList().get(0).intValue());
      } else if (indexInfo.getUnique()) {
        Assert.assertEquals("user_2", indexInfo.getName().toLowerCase());
        Assert.assertEquals(1, indexInfo.getColumnsCount());
        Assert.assertEquals(columns.get(1).getId(), indexInfo.getColumnsList().get(0).intValue());
      } else {
        Assert.assertEquals("idxtest", indexInfo.getName().toLowerCase());
        Assert.assertEquals(1, indexInfo.getColumnsCount());
        Assert.assertEquals(columns.get(2).getId(), indexInfo.getColumnsList().get(0).intValue());
      }
    }

    verifyRoute(tableInfo);
    return tableInfo;
  }
}
