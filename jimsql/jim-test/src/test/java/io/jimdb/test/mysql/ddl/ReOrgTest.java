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
package io.jimdb.test.mysql.ddl;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import io.jimdb.core.codec.Codec;
import io.jimdb.core.codec.ValueCodec;
import io.jimdb.core.model.meta.Column;
import io.jimdb.core.model.meta.MetaData;
import io.jimdb.core.model.meta.Table;
import io.jimdb.pb.Metapb;
import io.jimdb.test.mysql.SqlTestBase;
import io.jimdb.core.values.StringValue;
import io.netty.buffer.ByteBuf;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import com.google.protobuf.ByteString;
import com.google.protobuf.NettyByteString;

/**
 * @version V1.0
 */
public class ReOrgTest extends SqlTestBase {
  private static String catalogName = "maggie";
  private static String tableName = "sqltest2";

  private String tName = String.format("%s.%s", catalogName, tableName);

  private static final String EXTRA = "comment 'REPLICA =1' \nengine = memory";

  public void createCatalogIfNotExist() {
    String sql = String.format("Create database IF NOT EXISTS %s ", catalogName);
    execUpdate(sql, 0, true);
  }

  public void createTable(String sql) {
    execUpdate(sql, 0, true);
  }

  public void dropTableIfExist() {
    //delete table
    execUpdate(String.format("drop table IF EXISTS %s.%s", catalogName, tableName), 0, true);
  }

  public void prepareTable(String sql) {

    createCatalogIfNotExist();

    dropTableIfExist();

    try {
      Thread.sleep(1000);
    } catch (Exception ignored) {
    }

    createTable(sql);

    try {
      Thread.sleep(2000);
    } catch (Exception ignored) {
    }
  }

  @After
  public void tearDown() {
    try {
      dropTableIfExist();

      Thread.sleep(1000);
    } catch (Exception ignored) {
    }
  }

  //reOrg value
  @Test
  public void testAddColumn() {
    String sql = String.format("create table %s.%s (id bigint unsigned primary key auto_increment, " +
                    "num bigint unsigned not null) " +
                    "AUTO_INCREMENT=0  %s",
            catalogName, tableName, EXTRA);

    //create table
    prepareTable(sql);

    //insert data
    testBatchInsert(1, 10, "INSERT INTO " + tName + "(num) VALUES(%d)");

    //add column, and get reOrgValue, reOrgValue should be : varchar '', number 0 and so on
    String addColumnSql = "alter table " + tName + " add column name varchar(20) not null;";
    execUpdate(addColumnSql, 0, true);

    MetaData metaData = MetaData.Holder.getMetaData();
    Table table = metaData.getTable(catalogName, tableName);
    Column commonColumn = table.getWritableColumn("name");
    Metapb.SQLType commonSqlType = commonColumn.getType();
    Assert.assertEquals(commonSqlType.getNotNull(), true);
    Assert.assertNull(commonColumn.getDefaultValue());

    ByteBuf buf = Codec.allocBuffer(20);
    ValueCodec.encodeValue(buf, StringValue.getInstance(""), 0);
    ByteString reorgValue = NettyByteString.wrap(buf);
    Assert.assertEquals(commonColumn.getReorgValue().toStringUtf8(), reorgValue.toStringUtf8());

    //select old data from ds, name should be '', not null
    testSelect(1, 10, "select id, name, num from " + tName + " where num = %d ");

    //insert data,
    testBatchInsert(11, 12, "INSERT INTO " + tName + "(num) VALUES(%d)");
    //Field 'name' doesn't have a default value error

    testBatchInsert(11, 15, "INSERT INTO " + tName + "(name, num) VALUES('w2o23232', %d)");

    testSelect(11, 15, "select id, name, num from " + tName + " where num = %d ");
  }

  //reOrg add index
  //unique index on column
  @Test
  public void testAddIndexForUniqueError() {
//    createCatalog(catalogName);

    String sql = String.format("create table %s.%s (id bigint unsigned primary key auto_increment, " +
                    "name varchar(20) not null default 'a', " +
                    "num bigint unsigned not null) " +
                    "AUTO_INCREMENT=0  %s",
            catalogName, tableName, EXTRA);

    //create table
    prepareTable(sql);

    testBatchInsert(1, 15, "INSERT INTO " + tName + "(num) VALUES(%d)");

    String addIndexSql = "alter table " + tName + " add unique index nameidx using btree(name)";
    SQLException exception = new SQLException(null, "23000", 1062);
    execUpdate(addIndexSql, exception, true);
  }

  //reOrg add index
  //unique index on column
  @Test
  public void testAddIndexForUnique() {
    String sql = String.format("create table %s.%s (id bigint unsigned primary key auto_increment, " +
                    "name varchar(20) not null default 'a', " +
                    "num bigint unsigned) " +
                    "AUTO_INCREMENT=0  %s",
            catalogName, tableName, EXTRA);

    //create table
    prepareTable(sql);

    testBatchInsert(1, 15, "INSERT INTO " + tName + "(name) VALUES('w2o23232%d')");

    String addIndexSql = "alter table " + tName + " add unique ( `name`)";
    execUpdate(addIndexSql, 0, true);

    testSelect(1, 15, "select id, name, num from " + tName + " where name = 'w2o23232%d' ");

    testUpdate(1, 15, "delete from " + tName + " where name = 'w2o23232%d' ");
    testSelect(1, 15, "select id, name, num from " + tName + " where name = 'w2o23232%d' ");
  }

  //reOrg add index
  //not unique on name column
  @Test
  public void testAddIndexForNonUnique() {
//    createCatalog(catalogName);

    String sql = String.format("create table %s.%s (id bigint unsigned primary key auto_increment, " +
                    "name varchar(20) not null default 'a', " +
                    "num bigint unsigned not null) " +
                    "AUTO_INCREMENT=0  %s",
            catalogName, tableName, EXTRA);

    //create table
    prepareTable(sql);

    testBatchInsert(1, 15, "INSERT INTO " + tName + "(id, num) VALUES(%d, 1)");

    String addIndexSql = "alter table " + tName + " add index nameidx using btree(name)";
    execUpdate(addIndexSql, 0, true);

    testSelect(1, 1, "select id, name, num from " + tName + " where name = 'a' ");
    testUpdate(1, 15, "delete from " + tName + " where id = '%d' ");
    testSelect(1, 1, "select id, name, num from " + tName + " where name = 'a' ");
  }

  @Test
  public void testDropIndex() {
    String sql = String.format("create table %s.%s (id bigint unsigned primary key auto_increment, " +
                    "name varchar(20) not null default 'a', " +
                    "num bigint unsigned not null) " +
                    "AUTO_INCREMENT=0  %s",
            catalogName, tableName, EXTRA);

    //create table
    prepareTable(sql);

    sql = "alter table " + tName + " add index nameidx using btree(name)";
    execUpdate(sql, 0, true);

    sql = "alter table " + tName + " drop index nameidx";
    execUpdate(sql, 0, true);
  }

  public void testBatchInsert(int start, int end, String formatSql) {
    int num = end - start + 1;
    CountDownLatch latch = new CountDownLatch(num);
    ThreadPoolExecutor executor = new ThreadPoolExecutor(8,
            8, 1, TimeUnit.MINUTES, new LinkedBlockingDeque<>());
    for (int i = start; i <= end; i++) {
      int temp = i;
      executor.execute(() -> {
        try {
          String sql = String.format(formatSql, temp);
          System.out.println(sql);
          execUpdate(sql, 1, true);
        } catch (Throwable e) {
          e.printStackTrace();
          System.out.println("executor err " + e.getMessage());
          return;
        } finally {
          latch.countDown();
        }
      });
    }
    try {
      latch.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      executor.shutdown();
    }
  }

  public void testSelect(int start, int end, String formatSql) {
    int num = end - start + 1;
    CountDownLatch latch = new CountDownLatch(num);
    ThreadPoolExecutor executor = new ThreadPoolExecutor(8,
            8, 1, TimeUnit.MINUTES, new LinkedBlockingDeque<>(num));
    for (int i = start; i <= end; i++) {
      int temp = i;
      executor.execute(() -> {
        try {
          String sql = String.format(formatSql, temp);
          System.out.println(sql);
          List<String> s = execQuery(sql);
          System.out.println(Arrays.toString(s.toArray()));
        } catch (Throwable e) {
          e.printStackTrace();
          System.out.println("executor err " + e.getMessage());
          return;
        } finally {
          latch.countDown();
        }
      });
    }
    try {
      latch.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      executor.shutdown();
    }
  }

  private void testUpdate(int start, int end, String formatSql) {
    int num = end - start + 1;
    CountDownLatch latch = new CountDownLatch(num);
    ThreadPoolExecutor executor = new ThreadPoolExecutor(8,
            8, 1, TimeUnit.MINUTES, new LinkedBlockingDeque<>(num));
    for (int i = start; i <= end; i++) {
      int temp = i;
      executor.execute(() -> {
        try {
          String sql = String.format(formatSql, temp);
          execUpdate(sql, 1, true);
        } catch (Throwable e) {
          e.printStackTrace();
          System.out.println("executor err " + e.getMessage());
          return;
        } finally {
          latch.countDown();
        }
      });
    }
    try {
      latch.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      executor.shutdown();
    }
  }
}
