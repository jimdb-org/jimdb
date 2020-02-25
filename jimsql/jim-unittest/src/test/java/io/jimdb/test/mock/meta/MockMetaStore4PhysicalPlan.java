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
package io.jimdb.test.mock.meta;

import java.util.ArrayList;
import java.util.List;

import io.jimdb.pb.Basepb;
import io.jimdb.pb.Metapb;

import com.google.common.collect.Lists;

/**
 * @version V1.0
 */
public class MockMetaStore4PhysicalPlan extends MockMeta {

  @Override
  protected List<Metapb.TableInfo> initTables() {
    List<Metapb.TableInfo> tableInfos = Lists.newArrayListWithExpectedSize(10);
    tableInfos.add(initTable_Person());
    return tableInfos;
  }

  /**
   * create table person{
   * id  bigint,
   * age int,
   * phone varchar,
   * score int,
   * salary int,
   * name varchar,
   * PRIMARY KEY (`id`),
   * INDEX `idx_age` (`age` ),
   * UNIQUE KEY `idx_phone` (`phone`),
   * UNIQUE KEY `idx_age_phone` (`age`,`phone`),
   * }
   *
   * @return
   */
  private Metapb.TableInfo initTable_Person() {
    List<Metapb.ColumnInfo> columnInfoList = Lists.newArrayListWithExpectedSize(20);
    // table columns: id(Int), age(Int), phone(Varchar), score(Int), salary(Int), name(Varchar)
    Metapb.ColumnInfo id = Metapb.ColumnInfo.newBuilder()
            .setId(1)
            .setName("id")
            .setSqlType(Metapb.SQLType.newBuilder().setType(Basepb.DataType.Int).setNotNull(true).build())
            .setState(Metapb.MetaState.Public)
            .setPrimary(true).build();
    Metapb.ColumnInfo age = Metapb.ColumnInfo.newBuilder()
            .setId(2)
            .setName("age")
            .setSqlType(Metapb.SQLType.newBuilder().setType(Basepb.DataType.Int).setNotNull(true).build())
            .setState(Metapb.MetaState.Public)
            .setPrimary(false).build();
    Metapb.ColumnInfo phone = Metapb.ColumnInfo.newBuilder()
            .setId(3)
            .setName("phone")
            .setSqlType(Metapb.SQLType.newBuilder().setType(Basepb.DataType.Int).setNotNull(true).build())
            .setState(Metapb.MetaState.Public)
            .setPrimary(false).build();
    Metapb.ColumnInfo score = Metapb.ColumnInfo.newBuilder()
            .setId(4)
            .setName("score")
            .setSqlType(Metapb.SQLType.newBuilder().setType(Basepb.DataType.Int).setNotNull(true).build())
            .setState(Metapb.MetaState.Public)
            .setPrimary(false).build();
    Metapb.ColumnInfo salary = Metapb.ColumnInfo.newBuilder()
            .setId(5)
            .setName("salary")
            .setSqlType(Metapb.SQLType.newBuilder().setType(Basepb.DataType.Int).setNotNull(true).build())
            .setState(Metapb.MetaState.Public)
            .setPrimary(false).build();
    Metapb.ColumnInfo name = Metapb.ColumnInfo.newBuilder()
            .setId(6)
            .setName("name")
            .setSqlType(Metapb.SQLType.newBuilder().setType(Basepb.DataType.Varchar).setNotNull(true).build())
            .setState(Metapb.MetaState.Public)
            .setPrimary(false).build();
    Metapb.ColumnInfo t_char = Metapb.ColumnInfo.newBuilder()
            .setId(7)
            .setName("t_char")
            .setSqlType(Metapb.SQLType.newBuilder().setType(Basepb.DataType.Char).setNotNull(true).build())
            .setState(Metapb.MetaState.Public)
            .setPrimary(false).build();

    columnInfoList.add(id);
    columnInfoList.add(age);
    columnInfoList.add(phone);
    columnInfoList.add(score);
    columnInfoList.add(salary);
    columnInfoList.add(name);
    columnInfoList.add(t_char);

    // table indices: idx_age, idx_phone, idx_age_phone
    List<Integer> primaryIndexColumns = new ArrayList<>();
    primaryIndexColumns.add(1);

    Metapb.IndexInfo primaryIndexInfo = Metapb.IndexInfo.newBuilder()
            .setId(1)
            .setName("primary")
            .addAllColumns(primaryIndexColumns)
            .setPrimary(true)
            .setType(Metapb.IndexType.BTree)
            .setState(Metapb.MetaState.Public).build();

    List<Integer> ageIndexColumns = new ArrayList<>();
    ageIndexColumns.add(2);

    Metapb.IndexInfo ageIndexInfo = Metapb.IndexInfo.newBuilder()
            .setId(2)
            .setName("idx_age")
            .addAllColumns(ageIndexColumns)
            .setPrimary(false)
            .setType(Metapb.IndexType.BTree)
            .setState(Metapb.MetaState.Public).build();

    List<Integer> phoneIndexColumns = new ArrayList<>();
    phoneIndexColumns.add(3);

    Metapb.IndexInfo phoneIndexInfo = Metapb.IndexInfo.newBuilder()
            .setId(3)
            .setName("idx_phone")
            .addAllColumns(phoneIndexColumns)
            .setPrimary(false)
            .setType(Metapb.IndexType.BTree)
            .setUnique(true)
            .setState(Metapb.MetaState.Public).build();

    List<Integer> agePhoneIndexColumns = new ArrayList<>();
    agePhoneIndexColumns.add(2);
    agePhoneIndexColumns.add(3);

    Metapb.IndexInfo agePhoneIndexInfo = Metapb.IndexInfo.newBuilder()
            .setId(4)
            .setName("idx_age_phone")
            .addAllColumns(agePhoneIndexColumns)
            .setPrimary(false)
            .setUnique(false)
            .setType(Metapb.IndexType.BTree)
            .setState(Metapb.MetaState.Public).build();

    List<Metapb.IndexInfo> indexInfos = new ArrayList<>();
    indexInfos.add(primaryIndexInfo);
    indexInfos.add(ageIndexInfo);
    indexInfos.add(phoneIndexInfo);
    indexInfos.add(agePhoneIndexInfo);

    Metapb.TableInfo tableInfo =
            Metapb.TableInfo.newBuilder()
                    .setId(1)
                    .setName("person")
                    .addPrimarys(1)
                    .setState(Metapb.MetaState.Public)
                    .addAllColumns(columnInfoList)
                    .addAllIndices(indexInfos)
                    .build();

    return tableInfo;
  }
}