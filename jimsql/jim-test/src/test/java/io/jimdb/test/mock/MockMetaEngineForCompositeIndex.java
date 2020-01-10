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
//import io.jimdb.config.JimConfig;
//import io.jimdb.meta.model.JimColumn;
//import io.jimdb.meta.model.JimDatabase;
//import io.jimdb.meta.model.JimIndex;
//import io.jimdb.meta.model.JimTable;
//import io.jimdb.model.Catalog;
//import io.jimdb.model.Index;
//import io.jimdb.model.Table;
//import io.jimdb.model.meta.MetaData;
//import io.jimdb.pb.Basepb;
//import io.jimdb.plugin.MetaEngine;
//
//import java.util.Collection;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.concurrent.CompletableFuture;
//
///**
// * @version V1.0
// */
//public final class MockMetaEngineForCompositeIndex implements MetaEngine {
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
//     * create table user{
//     * id  bigint,
//     * name varchar,
//     * age int,
//     * phone varchar,
//     * score int,
//     * salary double,
//     * json json,
//     * date date,
//     * time time,
//     * remark varchar，
//     * PRIMARY KEY (`id`),
//     * INDEX `idx_name_age_phone_salary` (`name`,`age`,`phone`,`salary` ),
//     * UNIQUE KEY `uni_name_age_phone_score_salary` (`name`,`age`,`phone`,`score`,`salary` ),
//     * INDEX `idx_name_age_phone_salary_json_date_time` (`name`,`age`,`phone`,`salary`,`json`,`date`,`time` ),
//     * }
//     *
//     * @return
//     */
//    private JimTable[] initTables() {
//      return new JimTable[]{
//          getTableWithCompositeIdx()
//          ,getTableWithUniqueCompositeIdx()
//          ,getTableWithDateUniqueCompositeIdx()
//          ,getFixTable()
//      };
//    }
//
//
//    private JimTable getTableWithCompositeIdx() {
//      JimTable table = new JimTable();
//      table.setId(1);
//      table.setDbId(1);
//      table.setName("user");
//
//      // table columns: id(Int), name(Varchar), age(Int), phone(Varchar), score(Int), salary(Int), json(Json),date(Date),time(Time),remark(Varchar)
//      JimColumn[] columns = new JimColumn[10];
//      JimColumn column1 = new JimColumn();
//      column1.setId(1L);
//      column1.setName("id");
//      // key != 0 means primaryKey
//      column1.setPrimaryKey(1);
//      column1.setDataType(Basepb.DataType.BigInt);
//      columns[0] = column1;
//
//      JimColumn column2 = new JimColumn();
//      column2.setId(2L);
//      column2.setName("name");
//      column2.setDataType(Basepb.DataType.Varchar);
//      columns[1] = column2;
//
//
//      JimColumn column3 = new JimColumn();
//      column3.setId(3L);
//      column3.setName("age");
//      column3.setDataType(Basepb.DataType.Int);
//      column3.setNullable(false);
//      columns[2] = column3;
//
//      JimColumn column4 = new JimColumn();
//      column4.setId(4L);
//      column4.setName("phone");
//      column4.setUnique(true);
//      column4.setDataType(Basepb.DataType.Varchar);
//      column4.setNullable(false);
//      columns[3] = column4;
//
//      JimColumn column5 = new JimColumn();
//      column5.setId(5L);
//      column5.setName("score");
//      column5.setDataType(Basepb.DataType.Double);
//      columns[4] = column5;
//
//      JimColumn column6 = new JimColumn();
//      column6.setId(6L);
//      column6.setName("salary");
//      column6.setDataType(Basepb.DataType.Decimal);
//      columns[5] = column6;
//
//      JimColumn column7 = new JimColumn();
//      column7.setId(7L);
//      column7.setName("json");
//      column7.setDataType(Basepb.DataType.Json);
//      columns[6] = column7;
//
//      JimColumn column8 = new JimColumn();
//      column8.setId(8L);
//      column8.setName("date");
//      column8.setDataType(Basepb.DataType.Date);
//      columns[7] = column8;
//
//      JimColumn column9 = new JimColumn();
//      column9.setId(9L);
//      column9.setName("time");
//      column9.setDataType(Basepb.DataType.Time);
//      columns[8] = column9;
//
//      JimColumn column10 = new JimColumn();
//      column10.setId(10L);
//      column10.setName("remark");
//      column10.setDataType(Basepb.DataType.Varchar);
//      columns[9] = column10;
//
//      table.setColumns(columns);
//
//
//      /**
//       *  table indices:
//       *       UNIQUE KEY `uni_name_age_phone_score_salary` (`name`,`age`,`phone`,`score`,`salary` ),
//       */
//      JimIndex[] indices = new JimIndex[1];
//      JimIndex jimIndex = new JimIndex();
//      jimIndex.setId(1);
//      jimIndex.setName("uni_name_age_phone_score_salary");
//      jimIndex.setColNames(new String[]{"name","age","phone","score","salary"});
//      //  jimIndex.setUnique(true);
//      jimIndex.setIndexType(Index.IndexType.Btree);
//      indices[0] = jimIndex;
//      table.setIndexes(indices);
//      table.setIndexes(indices);
//
//      table.initColumns();
//
//      return table;
//    }
//
//    private JimTable getTableWithUniqueCompositeIdx() {
//      JimTable table = new JimTable();
//      table.setId(1);
//      table.setDbId(1);
//      table.setName("user_unique_idx");
//
//      // table columns: id(Int), name(Varchar), age(Int), phone(Varchar), score(Int), salary(Int), json(Json),date(Date),time(Time),remark(Varchar)
//      JimColumn[] columns = new JimColumn[10];
//      JimColumn column1 = new JimColumn();
//      column1.setId(1L);
//      column1.setName("id");
//      // key != 0 means primaryKey
//      column1.setPrimaryKey(1);
//      column1.setDataType(Basepb.DataType.BigInt);
//      columns[0] = column1;
//
//      JimColumn column2 = new JimColumn();
//      column2.setId(2L);
//      column2.setName("name");
//      column2.setDataType(Basepb.DataType.Varchar);
//      columns[1] = column2;
//
//
//      JimColumn column3 = new JimColumn();
//      column3.setId(3L);
//      column3.setName("age");
//      column3.setDataType(Basepb.DataType.Int);
//      column3.setNullable(false);
//      columns[2] = column3;
//
//      JimColumn column4 = new JimColumn();
//      column4.setId(4L);
//      column4.setName("phone");
//      column4.setUnique(true);
//      column4.setDataType(Basepb.DataType.Varchar);
//      column4.setNullable(false);
//      columns[3] = column4;
//
//      JimColumn column5 = new JimColumn();
//      column5.setId(5L);
//      column5.setName("score");
//      column5.setDataType(Basepb.DataType.Double);
//      columns[4] = column5;
//
//      JimColumn column6 = new JimColumn();
//      column6.setId(6L);
//      column6.setName("salary");
//      column6.setDataType(Basepb.DataType.Decimal);
//      columns[5] = column6;
//
//      JimColumn column7 = new JimColumn();
//      column7.setId(7L);
//      column7.setName("json");
//      column7.setDataType(Basepb.DataType.Json);
//      columns[6] = column7;
//
//      JimColumn column8 = new JimColumn();
//      column8.setId(8L);
//      column8.setName("date");
//      column8.setDataType(Basepb.DataType.Date);
//      columns[7] = column8;
//
//      JimColumn column9 = new JimColumn();
//      column9.setId(9L);
//      column9.setName("time");
//      column9.setDataType(Basepb.DataType.Time);
//      columns[8] = column9;
//
//      JimColumn column10 = new JimColumn();
//      column10.setId(10L);
//      column10.setName("remark");
//      column10.setDataType(Basepb.DataType.Varchar);
//      columns[9] = column10;
//
//      table.setColumns(columns);
//
//      /**
//       *  table indices:
//       *       UNIQUE KEY `uni_name_age_phone_score_salary` (`name`,`age`,`phone`,`score`,`salary` ),
//       */
//      JimIndex[] indices = new JimIndex[1];
//      JimIndex jimIndex1 = new JimIndex();
//      jimIndex1.setId(1);
//      jimIndex1.setName("uni_name_age_phone_score_salary");
//      jimIndex1.setColNames(new String[]{"name","age","phone","score","salary"});
//      jimIndex1.setUnique(true);
//      jimIndex1.setIndexType(Index.IndexType.Btree);
//      indices[0] = jimIndex1;
//      table.setIndexes(indices);
//
//      table.initColumns();
//
//      return table;
//    }
//
//    private JimTable getTableWithDateUniqueCompositeIdx() {
//      JimTable table = new JimTable();
//      table.setId(1);
//      table.setDbId(1);
//      table.setName("user_date_unique_idx");
//
//      // table columns: id(Int), name(Varchar), age(Int), phone(Varchar), score(Int), salary(Int), json(Json),date(Date),time(Time),remark(Varchar)
//      JimColumn[] columns = new JimColumn[10];
//      JimColumn column1 = new JimColumn();
//      column1.setId(1L);
//      column1.setName("id");
//      // key != 0 means primaryKey
//      column1.setPrimaryKey(1);
//      column1.setDataType(Basepb.DataType.BigInt);
//      columns[0] = column1;
//
//      JimColumn column2 = new JimColumn();
//      column2.setId(2L);
//      column2.setName("name");
//      column2.setDataType(Basepb.DataType.Varchar);
//      columns[1] = column2;
//
//
//      JimColumn column3 = new JimColumn();
//      column3.setId(3L);
//      column3.setName("age");
//      column3.setDataType(Basepb.DataType.Int);
//      column3.setNullable(false);
//      columns[2] = column3;
//
//      JimColumn column4 = new JimColumn();
//      column4.setId(4L);
//      column4.setName("phone");
//      column4.setUnique(true);
//      column4.setDataType(Basepb.DataType.Varchar);
//      column4.setNullable(false);
//      columns[3] = column4;
//
//      JimColumn column5 = new JimColumn();
//      column5.setId(5L);
//      column5.setName("score");
//      column5.setDataType(Basepb.DataType.Double);
//      columns[4] = column5;
//
//      JimColumn column6 = new JimColumn();
//      column6.setId(6L);
//      column6.setName("salary");
//      column6.setDataType(Basepb.DataType.Decimal);
//      columns[5] = column6;
//
//      JimColumn column7 = new JimColumn();
//      column7.setId(7L);
//      column7.setName("json");
//      column7.setDataType(Basepb.DataType.Json);
//      columns[6] = column7;
//
//      JimColumn column8 = new JimColumn();
//      column8.setId(8L);
//      column8.setName("date");
//      column8.setDataType(Basepb.DataType.Date);
//      columns[7] = column8;
//
//      JimColumn column9 = new JimColumn();
//      column9.setId(9L);
//      column9.setName("time");
//      column9.setDataType(Basepb.DataType.Time);
//      columns[8] = column9;
//
//      JimColumn column10 = new JimColumn();
//      column10.setId(10L);
//      column10.setName("remark");
//      column10.setDataType(Basepb.DataType.Varchar);
//      columns[9] = column10;
//
//      table.setColumns(columns);
//
//      /**
//       *  table indices:
//       *       INDEX `idx_name_age_phone_salary_json_date_time` (`name`,`age`,`phone`,`salary`,`json`,`date`,`time` ),
//       */
//      JimIndex[] indices = new JimIndex[1];
//      JimIndex jimIndex2 = new JimIndex();
//      jimIndex2.setId(3);
//      jimIndex2.setName("idx_name_age_phone_salary_date_time");
//      jimIndex2.setColNames(new String[]{"name","age","phone","salary","date","time"});
//      jimIndex2.setUnique(true);
//      jimIndex2.setIndexType(Index.IndexType.Btree);
//      indices[0] = jimIndex2;
//      table.setIndexes(indices);
//
//      table.initColumns();
//
//      return table;
//    }
//
//
//    private JimTable getFixTable() {
//      JimTable table = new JimTable();
//      table.setId(1);
//      table.setDbId(1);
//      table.setName("fix");
//
//      JimColumn[] columns = new JimColumn[10];
//      JimColumn column1 = new JimColumn();
//      column1.setId(1L);
//      column1.setName("id");
//      // key != 0 means primaryKey
//      column1.setPrimaryKey(1);
//      column1.setDataType(Basepb.DataType.BigInt);
//      columns[0] = column1;
//
//      JimColumn column2 = new JimColumn();
//      column2.setId(2L);
//      column2.setName("t_int");
//      column2.setDataType(Basepb.DataType.Int);
//      columns[1] = column2;
//
//
//      JimColumn column3 = new JimColumn();
//      column3.setId(3L);
//      column3.setName("t_unique");
//      column3.setDataType(Basepb.DataType.Varchar);
//      column3.setNullable(false);
//      columns[2] = column3;
//
//      JimColumn column4 = new JimColumn();
//      column4.setId(4L);
//      column4.setName("t_index");
//      column4.setUnique(true);
//      column4.setDataType(Basepb.DataType.Varchar);
//      column4.setNullable(false);
//      columns[3] = column4;
//
//      JimColumn column5 = new JimColumn();
//      column5.setId(5L);
//      column5.setName("t_tinyint");
//      column5.setDataType(Basepb.DataType.TinyInt);
//      columns[4] = column5;
//
//      JimColumn column6 = new JimColumn();
//      column6.setId(6L);
//      column6.setName("t_smallint");
//      column6.setDataType(Basepb.DataType.SmallInt);
//      columns[5] = column6;
//
//      JimColumn column7 = new JimColumn();
//      column7.setId(7L);
//      column7.setName("t_varchar");
//      column7.setDataType(Basepb.DataType.Varchar);
//      columns[6] = column7;
//
//      JimColumn column8 = new JimColumn();
//      column8.setId(8L);
//      column8.setName("t_remark");
//      column8.setDataType(Basepb.DataType.Json);
//      columns[7] = column8;
//
//      JimColumn column9 = new JimColumn();
//      column9.setId(9L);
//      column9.setName("t_float");
//      column9.setDataType(Basepb.DataType.Float);
//      columns[8] = column9;
//
//      JimColumn column10 = new JimColumn();
//      column10.setId(10L);
//      column10.setName("t_double");
//      column10.setDataType(Basepb.DataType.Double);
//      columns[9] = column10;
//
//      table.setColumns(columns);
//
//      JimIndex[] indices = new JimIndex[1];
//      JimIndex jimIndex2 = new JimIndex();
//      jimIndex2.setId(3);
//      jimIndex2.setName("uni_t_unique");
//      jimIndex2.setColNames(new String[]{"t_unique"});
//      jimIndex2.setUnique(true);
//      jimIndex2.setIndexType(Index.IndexType.Btree);
//      indices[0] = jimIndex2;
//      table.setIndexes(indices);
//
//      table.initColumns();
//
//      return table;
//    }
//  }
//}
