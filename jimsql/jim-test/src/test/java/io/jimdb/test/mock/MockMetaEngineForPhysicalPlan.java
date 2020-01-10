///*
// * Copyright 2019 The Chubao Authors.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// * implied. See the License for the specific language governing
// * permissions and limitations under the License.
// */
//package io.jimdb.test.mock;
//
//import java.util.Collection;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.concurrent.CompletableFuture;
//
//import io.jimdb.config.JimConfig;
//import io.jimdb.meta.model.JimColumn;
//import io.jimdb.meta.model.JimDatabase;
//import io.jimdb.meta.model.JimIndex;
//import io.jimdb.meta.model.JimTable;
//import io.jimdb.model.Catalog;
//import io.jimdb.model.Index;
//import io.jimdb.model.Table;
//import io.jimdb.pb.Basepb;
//import io.jimdb.plugin.MetaEngine;
//
///**
// * @version V1.0
// */
//public final class MockMetaEngineForPhysicalPlan implements MetaEngine {
//  private MetaData metaData;
//
//  @Override
//  public MetaData getMetaData() {
//    return this.metaData;
//  }
//
//  @Override
//  public CompletableFuture<MetaData> sync(int version) {
//    final CompletableFuture<MetaData> completableFuture = new CompletableFuture<>();
//    completableFuture.complete(this.metaData);
//    return completableFuture;
//  }
//
//  @Override
//  public String getName() {
//    return "mockMeta";
//  }
//
//  @Override
//  public void init(JimConfig c) {
//    this.metaData = new MockMetaData();
//  }
//
//  @Override
//  public void close() {
//  }
//
//  static final class MockMetaData implements MetaData {
//    private final Map<String, JimDatabase> catalogs = new HashMap<>();
//
//    MockMetaData() {
//      initCatalogs();
//    }
//
//    @Override
//    public int getVersion() {
//      return 1;
//    }
//
//    @Override
//    public JimDatabase getCatalog(String name) {
//      return catalogs.get(name);
//    }
//
//    @Override
//    public Table getTable(String catalog, String table) {
//      return catalogs.get(catalog).getTable(table);
//    }
//
//    @Override
//    public Collection<? extends Catalog> getAllCatalogs() {
//      return catalogs.values();
//    }
//
//    private void initCatalogs() {
//      JimDatabase database = new JimDatabase();
//      database.setId(1);
//      database.setName("test");
//      database.setVersion(1);
//      database.setJimTables(initTables());
//      catalogs.put("test", database);
//    }
//
//    /**
//     * create table person{
//     * id  bigint,
//     * age int,
//     * phone varchar,
//     * score int,
//     * salary int,
//     * name varchar,
//     * PRIMARY KEY (`id`),
//     * INDEX `idx_age` (`age` ),
//     * UNIQUE KEY `idx_phone` (`phone`),
//     * UNIQUE KEY `idx_age_phone` (`age`,`phone`),
//     * }
//     *
//     * @return
//     */
//    private JimTable[] initTables() {
//      JimTable table = new JimTable();
//      table.setId(1);
//      table.setDbId(1);
//      table.setDbName("test");
//      table.setName("person");
//
//      // table columns: id(Int), age(Int), phone(Varchar), score(Int), salary(Int), name(Varchar)
//      JimColumn[] columns = new JimColumn[6];
//      JimColumn column1 = new JimColumn();
//      column1.setId(1L);
//      column1.setName("id");
//      // key != 0 means primaryKey
//      column1.setPrimaryKey(1);
//      column1.setDataType(Basepb.DataType.BigInt);
//      columns[0] = column1;
//      table.setColumns(columns);
//
//      JimColumn column2 = new JimColumn();
//      column2.setId(2L);
//      column2.setName("age");
//      column2.setDataType(Basepb.DataType.Int);
//      column2.setNullable(false);
//      columns[1] = column2;
//
//      JimColumn column3 = new JimColumn();
//      column3.setId(3L);
//      column3.setName("phone");
//      column3.setUnique(true);
//      column3.setDataType(Basepb.DataType.Varchar);
//      column3.setNullable(false);
//      columns[2] = column3;
//
//      JimColumn column4 = new JimColumn();
//      column4.setId(4L);
//      column4.setName("score");
//      column4.setDataType(Basepb.DataType.Int);
//      columns[3] = column4;
//
//      JimColumn column5 = new JimColumn();
//      column5.setId(5L);
//      column5.setName("salary");
//      column5.setDataType(Basepb.DataType.Int);
//      columns[4] = column5;
//
//      JimColumn column6 = new JimColumn();
//      column6.setId(6L);
//      column6.setName("name");
//      column6.setDataType(Basepb.DataType.Varchar);
//      columns[5] = column6;
//
//      table.setColumns(columns);
//
//      // table indices: idx_age, idx_phone, idx_age_phone
//      JimIndex[] indices = new JimIndex[3];
//      JimIndex jimIndex = new JimIndex();
//      jimIndex.setId(1);
//      jimIndex.setName("idx_age");
//      jimIndex.setColNames(new String[]{ "age" });
//      //  jimIndex.setUnique(true);
//      jimIndex.setIndexType(Index.IndexType.Btree);
//      indices[0] = jimIndex;
//      table.setIndexes(indices);
//
//      JimIndex jimIndex1 = new JimIndex();
//      jimIndex1.setId(2);
//      jimIndex1.setName("idx_phone");
//      jimIndex1.setColNames(new String[]{ "phone" });
//      jimIndex1.setUnique(true);
//      jimIndex1.setIndexType(Index.IndexType.Btree);
//      indices[1] = jimIndex1;
//      table.setIndexes(indices);
//
//      JimIndex jimIndex2 = new JimIndex();
//      jimIndex2.setId(3);
//      jimIndex2.setName("idx_age_phone");
//      jimIndex2.setColNames(new String[]{ "age", "phone" });
//      jimIndex2.setUnique(true);
//      jimIndex2.setIndexType(Index.IndexType.Btree);
//      indices[2] = jimIndex2;
//      table.setIndexes(indices);
//
//      table.initColumns();
//
//      return new JimTable[]{ table };
//    }
//  }
//}
