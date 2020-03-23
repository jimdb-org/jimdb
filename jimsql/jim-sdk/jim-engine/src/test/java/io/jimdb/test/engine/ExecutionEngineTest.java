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
package io.jimdb.test.engine;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import io.jimdb.core.config.JimConfig;
import io.jimdb.engine.ExecutionEngine;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.ValueExpr;
import io.jimdb.core.model.meta.Catalog;
import io.jimdb.core.model.meta.Table;
import io.jimdb.pb.Basepb;
import io.jimdb.pb.Metapb;
import io.jimdb.pb.Metapb.SQLType;
import io.jimdb.core.plugin.store.Engine;
import io.jimdb.core.plugin.store.Transaction;
import io.jimdb.core.values.LongValue;
import io.jimdb.core.values.NullValue;
import io.jimdb.core.values.StringValue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Execution Engine Test.
 *
 */
public class ExecutionEngineTest {
  private Engine engine;
  private Table table;

  @Before
  public void before() throws Exception {
    Properties properties = new Properties();
    properties.setProperty("jim.meta.address", "http://127.0.0.1:8080");
    properties.setProperty("jim.meta.cluster", "1");
    properties.setProperty("jim.meta.retry", "1");
    JimConfig config = new JimConfig(properties);

    engine = new ExecutionEngine();
    engine.init(config);
  }

  @After
  public void after() throws Exception {
  }

  /**
   * Method: init(ServerConfig c)
   */
  @Test
  public void testInit() throws Exception {
  }

  @Test
  public void testInsertNoAuto() throws Exception {
    table = mockTable();
    Transaction txn = engine.beginTxn(null);
    int rowNum = 3;
    Expression[][] values = new Expression[rowNum][];
    for (int i = 0; i < rowNum; i++) {
      Expression[] row = new Expression[3];
      row[0] = new ValueExpr(LongValue.getInstance(i + 1), SQLType.newBuilder().setType(Basepb.DataType.BigInt).build());
      row[1] = new ValueExpr(StringValue.getInstance("aa" + i), SQLType.newBuilder().setType(Basepb.DataType.Varchar).build());
      row[2] = new ValueExpr(StringValue.getInstance("11"), SQLType.newBuilder().setType(Basepb.DataType.Varchar).build());
//      row[0]  = ByteUtil.intToBytes(i + 1);
//      row[1] = ("aa" + i).getBytes();
//      row[2] = "11".getBytes();
      values[i] = row;
    }
    engine.insert(null, table, table.getWritableColumns(), values, null, false);
    txn.commit();
  }

  @Test
  public void testInsertAuto() throws Exception {
    table = mockTableAuto();
    Transaction txn = engine.beginTxn(null);
    int rowNum = 2;
    Expression[][] values = new Expression[rowNum][];
    for (int i = 0; i < rowNum; i++) {
      Expression[] row = new Expression[3];
      row[0] = new ValueExpr(NullValue.getInstance(), SQLType.newBuilder().setType(Basepb.DataType.BigInt).build());
      row[1] = new ValueExpr(StringValue.getInstance("aa" + i), SQLType.newBuilder().setType(Basepb.DataType.Varchar).build());
      row[2] = new ValueExpr(StringValue.getInstance("11"), SQLType.newBuilder().setType(Basepb.DataType.Varchar).build());
      values[i] = row;
    }
    engine.insert(null, table, table.getWritableColumns(), values, null, false);
    txn.commit();
  }

  @Test
  public void testInsertAuto2() throws Exception {
    table = mockTableAuto();
    Transaction txn = engine.beginTxn(null);
    int rowNum = 2;
    Expression[][] values = new Expression[rowNum][];
    for (int i = 0; i < rowNum; i++) {
      Expression[] row = new Expression[3];
      row[0] = new ValueExpr(LongValue.getInstance(i + 1), SQLType.newBuilder().setType(Basepb.DataType.BigInt).build());
      row[1] = new ValueExpr(StringValue.getInstance("aa" + i), SQLType.newBuilder().setType(Basepb.DataType.Varchar).build());
      row[2] = new ValueExpr(StringValue.getInstance("11"), SQLType.newBuilder().setType(Basepb.DataType.Varchar).build());
      values[i] = row;
    }
    engine.insert(null, table, table.getWritableColumns(), values, null, false);
    txn.commit();
  }

  @Test
  public void testInsertAuto3() throws Exception {
    table = mockTableAuto();
    Transaction txn = engine.beginTxn(null);
    int rowNum = 3;
    Expression[][] values = new Expression[rowNum][];
    for (int i = 0; i < rowNum; i++) {
      Expression[] row = new Expression[3];
      row[0] = new ValueExpr(LongValue.getInstance(0), SQLType.newBuilder().setType(Basepb.DataType.BigInt).build());
      row[1] = new ValueExpr(StringValue.getInstance("aa" + i), SQLType.newBuilder().setType(Basepb.DataType.Varchar).build());
      row[2] = new ValueExpr(StringValue.getInstance("11"), SQLType.newBuilder().setType(Basepb.DataType.Varchar).build());
      values[i] = row;
    }
    engine.insert(null, table, table.getWritableColumns(), values, null, false);
    txn.commit();
  }

  private Table mockTable() {
    Metapb.CatalogInfo catalogInfo = Metapb.CatalogInfo.newBuilder()
            .setId(1)
            .setState(Metapb.MetaState.Public)
            .setName("test").build();

    List<Metapb.ColumnInfo> columnInfos = new ArrayList<>(3);
    Metapb.ColumnInfo columnInfo = Metapb.ColumnInfo.newBuilder()
            .setId(1)
            .setName("h")
            .setSqlType(SQLType.newBuilder().setType(Basepb.DataType.BigInt).setNotNull(true).build())
            .setState(Metapb.MetaState.Public)
            .setPrimary(true).build();
    columnInfos.add(columnInfo);
    Metapb.ColumnInfo columnInfo2 = Metapb.ColumnInfo.newBuilder()
            .setId(2)
            .setName("id")
            .setSqlType(SQLType.newBuilder().setType(Basepb.DataType.Varchar).setNotNull(false).build())
            .setState(Metapb.MetaState.Public).build();
    columnInfos.add(columnInfo2);
    Metapb.ColumnInfo columnInfo3 = Metapb.ColumnInfo.newBuilder()
            .setId(3)
            .setName("v")
            .setSqlType(SQLType.newBuilder().setType(Basepb.DataType.Varchar).setNotNull(false).build())
            .setState(Metapb.MetaState.Public).build();
    columnInfos.add(columnInfo3);

    List<Integer> primaryList = new ArrayList<>();
    primaryList.add(1);

    List<Integer> indexColumns = new ArrayList<>();
    indexColumns.add(1);

    Metapb.IndexInfo indexInfo = Metapb.IndexInfo.newBuilder()
            .setId(3)
            .setName("primary")
            .addAllColumns(indexColumns)
            .setPrimary(true)
            .setState(Metapb.MetaState.Public).build();

    List<Metapb.IndexInfo> indexInfos = new ArrayList<>();
    indexInfos.add(indexInfo);

    Metapb.TableInfo tableInfo = Metapb.TableInfo.newBuilder()
            .setId(2)
            .setDbId(catalogInfo.getId())
            .setName("maggie")
            .setReplicas(3)
            .setState(Metapb.MetaState.Public)
            .addAllPrimarys(primaryList)
            .addAllColumns(columnInfos)
            .addAllIndices(indexInfos).build();

    List<Metapb.TableInfo> tableInfos = new ArrayList<>();
    tableInfos.add(tableInfo);

    Catalog catalog = new Catalog(catalogInfo, tableInfos);
    Table table = new Table(catalog, tableInfos.get(0));
    return table;
  }

  private Table mockTableAuto() {
    Metapb.CatalogInfo catalogInfo = Metapb.CatalogInfo.newBuilder()
            .setId(1)
            .setState(Metapb.MetaState.Public)
            .setName("test").build();

    List<Metapb.ColumnInfo> columnInfos = new ArrayList<>(3);
    Metapb.ColumnInfo columnInfo = Metapb.ColumnInfo.newBuilder()
            .setId(1)
            .setName("h")
            .setSqlType(SQLType.newBuilder().setType(Basepb.DataType.BigInt).setUnsigned(true).setNotNull(true).build())
            .setAutoIncr(true)
            .setState(Metapb.MetaState.Public)
            .setPrimary(true).build();
    columnInfos.add(columnInfo);
    Metapb.ColumnInfo columnInfo2 = Metapb.ColumnInfo.newBuilder()
            .setId(2)
            .setName("id")
            .setSqlType(SQLType.newBuilder().setType(Basepb.DataType.Varchar).setNotNull(false).build())
            .setState(Metapb.MetaState.Public).build();
    columnInfos.add(columnInfo2);
    Metapb.ColumnInfo columnInfo3 = Metapb.ColumnInfo.newBuilder()
            .setId(3)
            .setName("v")
            .setSqlType(SQLType.newBuilder().setType(Basepb.DataType.Varchar).setNotNull(false).build())
            .setState(Metapb.MetaState.Public).build();
    columnInfos.add(columnInfo3);

    List<Integer> primaryList = new ArrayList<>();
    primaryList.add(1);

    List<Integer> indexColumns = new ArrayList<>();
    indexColumns.add(1);

    Metapb.IndexInfo indexInfo = Metapb.IndexInfo.newBuilder()
            .setId(3)
            .setName("primary")
            .addAllColumns(indexColumns)
            .setPrimary(true)
            .setState(Metapb.MetaState.Public).build();

    List<Metapb.IndexInfo> indexInfos = new ArrayList<>();
    indexInfos.add(indexInfo);

    Metapb.TableInfo tableInfo = Metapb.TableInfo.newBuilder()
            .setId(2)
            .setDbId(catalogInfo.getId())
            .setName("maggie")
            .setReplicas(3)
            .setState(Metapb.MetaState.Public)
            .addAllPrimarys(primaryList)
            .addAllColumns(columnInfos)
            .addAllIndices(indexInfos).build();

    List<Metapb.TableInfo> tableInfos = new ArrayList<>();
    tableInfos.add(tableInfo);

    Catalog catalog = new Catalog(catalogInfo, tableInfos);
    Table table = new Table(catalog, tableInfos.get(0));
    return table;
  }
}
