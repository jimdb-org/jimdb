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
public class MockMetaStore4CompositeIndex extends MockMeta {
  @Override
  protected List<Metapb.TableInfo> initTables() {
    List<Metapb.TableInfo> tableInfos = Lists.newArrayListWithExpectedSize(10);
    tableInfos.add(getTableWithCompositeIdx());
    tableInfos.add(getTableWithUniqueCompositeIdx());
    tableInfos.add(getTableWithDateUniqueCompositeIdx());
    tableInfos.add(getFixTable());
    return tableInfos;
  }

  /**
   * create table user{
   * id  bigint,
   * name varchar,
   * age int,
   * phone varchar,
   * score int,
   * salary double,
   * json json,
   * date date,
   * time time,
   * remark varchar，
   * PRIMARY KEY (`id`),
   * INDEX `idx_name_age_phone_salary` (`name`,`age`,`phone`,`salary` ),
   * UNIQUE KEY `uni_name_age_phone_score_salary` (`name`,`age`,`phone`,`score`,`salary` ),
   * INDEX `idx_name_age_phone_salary_json_date_time` (`name`,`age`,`phone`,`salary`,`json`,`date`,`time` ),
   * }
   *
   * @return
   */

  private Metapb.TableInfo getTableWithCompositeIdx() {
    List<Metapb.ColumnInfo> columnInfoList = Lists.newArrayListWithExpectedSize(20);
    // table columns: id(Int), name(Varchar), age(Int), phone(Varchar), score(Int), salary(Int), json(Json),date(Date),time(Time),remark(Varchar)

    Metapb.ColumnInfo id = Metapb.ColumnInfo.newBuilder()
            .setId(1)
            .setName("id")
            .setSqlType(Metapb.SQLType.newBuilder().setType(Basepb.DataType.Int).setNotNull(true).build())
            .setState(Metapb.MetaState.Public)
            .setPrimary(true).build();
    Metapb.ColumnInfo name = Metapb.ColumnInfo.newBuilder()
            .setId(2)
            .setName("name")
            .setSqlType(Metapb.SQLType.newBuilder().setType(Basepb.DataType.Varchar).setNotNull(true).build())
            .setState(Metapb.MetaState.Public)
            .setPrimary(false).build();
    Metapb.ColumnInfo age = Metapb.ColumnInfo.newBuilder()
            .setId(3)
            .setName("age")
            .setSqlType(Metapb.SQLType.newBuilder().setType(Basepb.DataType.Int).setNotNull(true).build())
            .setState(Metapb.MetaState.Public)
            .setPrimary(false).build();
    Metapb.ColumnInfo phone = Metapb.ColumnInfo.newBuilder()
            .setId(4)
            .setName("phone")
            .setSqlType(Metapb.SQLType.newBuilder().setType(Basepb.DataType.Varchar).setNotNull(true).build())
            .setState(Metapb.MetaState.Public)
            .setPrimary(false).build();
    Metapb.ColumnInfo score = Metapb.ColumnInfo.newBuilder()
            .setId(5)
            .setName("score")
            .setSqlType(Metapb.SQLType.newBuilder().setType(Basepb.DataType.Double).setNotNull(true).build())
            .setState(Metapb.MetaState.Public)
            .setPrimary(false).build();
    Metapb.ColumnInfo salary = Metapb.ColumnInfo.newBuilder()
            .setId(6)
            .setName("salary")
            .setSqlType(Metapb.SQLType.newBuilder().setType(Basepb.DataType.Decimal).setNotNull(true).build())
            .setState(Metapb.MetaState.Public)
            .setPrimary(false).build();
    Metapb.ColumnInfo json = Metapb.ColumnInfo.newBuilder()
            .setId(7)
            .setName("json")
            .setSqlType(Metapb.SQLType.newBuilder().setType(Basepb.DataType.Json).setNotNull(true).build())
            .setState(Metapb.MetaState.Public)
            .setPrimary(false).build();
    Metapb.ColumnInfo date = Metapb.ColumnInfo.newBuilder()
            .setId(8)
            .setName("date")
            .setSqlType(Metapb.SQLType.newBuilder().setType(Basepb.DataType.Date).setNotNull(true).build())
            .setState(Metapb.MetaState.Public)
            .setPrimary(false).build();

    Metapb.ColumnInfo time = Metapb.ColumnInfo.newBuilder()
            .setId(9)
            .setName("time")
            .setSqlType(Metapb.SQLType.newBuilder().setType(Basepb.DataType.Time).setNotNull(true).build())
            .setState(Metapb.MetaState.Public)
            .setPrimary(false).build();

    Metapb.ColumnInfo remark = Metapb.ColumnInfo.newBuilder()
            .setId(10)
            .setName("remark")
            .setSqlType(Metapb.SQLType.newBuilder().setType(Basepb.DataType.Varchar).setNotNull(true).build())
            .setState(Metapb.MetaState.Public)
            .setPrimary(false).build();

    columnInfoList.add(id);
    columnInfoList.add(name);
    columnInfoList.add(age);
    columnInfoList.add(phone);
    columnInfoList.add(score);
    columnInfoList.add(salary);
    columnInfoList.add(json);
    columnInfoList.add(date);
    columnInfoList.add(time);
    columnInfoList.add(remark);

    /**
     *  table indices:
     *       UNIQUE KEY `uni_name_age_phone_score_salary` (`name`,`age`,`phone`,`score`,`salary` ),
     */
    List<Integer> primaryIndexColumns = new ArrayList<>();
    primaryIndexColumns.add(1);

    Metapb.IndexInfo primaryIndexInfo = Metapb.IndexInfo.newBuilder()
            .setId(1)
            .setName("primary")
            .addAllColumns(primaryIndexColumns)
            .setPrimary(true)
            .setType(Metapb.IndexType.BTree)
            .setState(Metapb.MetaState.Public).build();

    List<Integer> uniIndexColumns = new ArrayList<>();
    uniIndexColumns.add(2);
    uniIndexColumns.add(3);
    uniIndexColumns.add(4);
    uniIndexColumns.add(5);
    uniIndexColumns.add(6);

    Metapb.IndexInfo uniIndexInfo = Metapb.IndexInfo.newBuilder()
            .setId(2)
            .setName("uni_name_age_phone_score_salary")
            .addAllColumns(uniIndexColumns)
            .setPrimary(false)
            .setType(Metapb.IndexType.BTree)
            .setState(Metapb.MetaState.Public).build();

    List<Metapb.IndexInfo> indexInfos = new ArrayList<>();
    indexInfos.add(primaryIndexInfo);
    indexInfos.add(uniIndexInfo);

    Metapb.TableInfo tableInfo =
            Metapb.TableInfo.newBuilder()
                    .setId(1)
                    .setDbId(1)
                    .setName("user")
                    .addPrimarys(1)
                    .addPrimarys(1)
                    .setState(Metapb.MetaState.Public)
                    .addAllColumns(columnInfoList)
                    .addAllIndices(indexInfos)
                    .build();

    return tableInfo;
  }

  private Metapb.TableInfo getTableWithUniqueCompositeIdx() {
    List<Metapb.ColumnInfo> columnInfoList = Lists.newArrayListWithExpectedSize(20);
    // table columns: id(Int), name(Varchar), age(Int), phone(Varchar), score(Int), salary(Int), json(Json),date(Date),time(Time),remark(Varchar)

    Metapb.ColumnInfo id = Metapb.ColumnInfo.newBuilder()
            .setId(1)
            .setName("id")
            .setSqlType(Metapb.SQLType.newBuilder().setType(Basepb.DataType.Int).setNotNull(true).build())
            .setState(Metapb.MetaState.Public)
            .setPrimary(true).build();
    Metapb.ColumnInfo name = Metapb.ColumnInfo.newBuilder()
            .setId(2)
            .setName("name")
            .setSqlType(Metapb.SQLType.newBuilder().setType(Basepb.DataType.Varchar).setNotNull(true).build())
            .setState(Metapb.MetaState.Public)
            .setPrimary(false).build();
    Metapb.ColumnInfo age = Metapb.ColumnInfo.newBuilder()
            .setId(3)
            .setName("age")
            .setSqlType(Metapb.SQLType.newBuilder().setType(Basepb.DataType.Int).setNotNull(true).build())
            .setState(Metapb.MetaState.Public)
            .setPrimary(false).build();
    Metapb.ColumnInfo phone = Metapb.ColumnInfo.newBuilder()
            .setId(4)
            .setName("phone")
            .setSqlType(Metapb.SQLType.newBuilder().setType(Basepb.DataType.Varchar).setNotNull(true).build())
            .setState(Metapb.MetaState.Public)
            .setPrimary(false).build();
    Metapb.ColumnInfo score = Metapb.ColumnInfo.newBuilder()
            .setId(5)
            .setName("score")
            .setSqlType(Metapb.SQLType.newBuilder().setType(Basepb.DataType.Double).setNotNull(true).build())
            .setState(Metapb.MetaState.Public)
            .setPrimary(false).build();
    Metapb.ColumnInfo salary = Metapb.ColumnInfo.newBuilder()
            .setId(6)
            .setName("salary")
            .setSqlType(Metapb.SQLType.newBuilder().setType(Basepb.DataType.Decimal).setNotNull(true).build())
            .setState(Metapb.MetaState.Public)
            .setPrimary(false).build();
    Metapb.ColumnInfo json = Metapb.ColumnInfo.newBuilder()
            .setId(7)
            .setName("json")
            .setSqlType(Metapb.SQLType.newBuilder().setType(Basepb.DataType.Json).setNotNull(true).build())
            .setState(Metapb.MetaState.Public)
            .setPrimary(false).build();
    Metapb.ColumnInfo date = Metapb.ColumnInfo.newBuilder()
            .setId(8)
            .setName("date")
            .setSqlType(Metapb.SQLType.newBuilder().setType(Basepb.DataType.Date).setNotNull(true).build())
            .setState(Metapb.MetaState.Public)
            .setPrimary(false).build();

    Metapb.ColumnInfo time = Metapb.ColumnInfo.newBuilder()
            .setId(9)
            .setName("time")
            .setSqlType(Metapb.SQLType.newBuilder().setType(Basepb.DataType.Time).setNotNull(true).build())
            .setState(Metapb.MetaState.Public)
            .setPrimary(false).build();

    Metapb.ColumnInfo remark = Metapb.ColumnInfo.newBuilder()
            .setId(10)
            .setName("remark")
            .setSqlType(Metapb.SQLType.newBuilder().setType(Basepb.DataType.Varchar).setNotNull(true).build())
            .setState(Metapb.MetaState.Public)
            .setPrimary(false).build();

    columnInfoList.add(id);
    columnInfoList.add(name);
    columnInfoList.add(age);
    columnInfoList.add(phone);
    columnInfoList.add(score);
    columnInfoList.add(salary);
    columnInfoList.add(json);
    columnInfoList.add(date);
    columnInfoList.add(time);
    columnInfoList.add(remark);

    /**
     *  table indices:
     *       UNIQUE KEY `uni_name_age_phone_score_salary` (`name`,`age`,`phone`,`score`,`salary` ),
     */
    List<Integer> primaryIndexColumns = new ArrayList<>();
    primaryIndexColumns.add(1);

    Metapb.IndexInfo primaryIndexInfo = Metapb.IndexInfo.newBuilder()
            .setId(1)
            .setName("primary")
            .addAllColumns(primaryIndexColumns)
            .setPrimary(true)
            .setType(Metapb.IndexType.BTree)
            .setState(Metapb.MetaState.Public).build();

    List<Integer> uniIndexColumns = new ArrayList<>();
    uniIndexColumns.add(2);
    uniIndexColumns.add(3);
    uniIndexColumns.add(4);
    uniIndexColumns.add(5);
    uniIndexColumns.add(6);

    Metapb.IndexInfo uniIndexInfo = Metapb.IndexInfo.newBuilder()
            .setId(2)
            .setName("uni_name_age_phone_score_salary")
            .addAllColumns(uniIndexColumns)
            .setPrimary(false)
            .setUnique(true)
            .setType(Metapb.IndexType.BTree)
            .setState(Metapb.MetaState.Public).build();

    List<Metapb.IndexInfo> indexInfos = new ArrayList<>();
    indexInfos.add(primaryIndexInfo);
    indexInfos.add(uniIndexInfo);

    Metapb.TableInfo tableInfo =
            Metapb.TableInfo.newBuilder()
                    .setId(1)
                    .setDbId(1)
                    .addPrimarys(1)
                    .setName("user_unique_idx")
                    .setState(Metapb.MetaState.Public)
                    .addAllColumns(columnInfoList)
                    .addAllIndices(indexInfos)
                    .build();

    return tableInfo;
  }

  private Metapb.TableInfo getTableWithDateUniqueCompositeIdx() {
    List<Metapb.ColumnInfo> columnInfoList = Lists.newArrayListWithExpectedSize(20);
    // table columns: id(Int), name(Varchar), age(Int), phone(Varchar), score(Int), salary(Int), json(Json),date(Date),time(Time),remark(Varchar)

    Metapb.ColumnInfo id = Metapb.ColumnInfo.newBuilder()
            .setId(1)
            .setName("id")
            .setSqlType(Metapb.SQLType.newBuilder().setType(Basepb.DataType.BigInt).setNotNull(true).build())
            .setState(Metapb.MetaState.Public)
            .setPrimary(true).build();
    Metapb.ColumnInfo name = Metapb.ColumnInfo.newBuilder()
            .setId(2)
            .setName("name")
            .setSqlType(Metapb.SQLType.newBuilder().setType(Basepb.DataType.Varchar).setNotNull(true).build())
            .setState(Metapb.MetaState.Public)
            .setPrimary(false).build();
    Metapb.ColumnInfo age = Metapb.ColumnInfo.newBuilder()
            .setId(3)
            .setName("age")
            .setSqlType(Metapb.SQLType.newBuilder().setType(Basepb.DataType.Int).setNotNull(true).build())
            .setState(Metapb.MetaState.Public)
            .setPrimary(false).build();
    Metapb.ColumnInfo phone = Metapb.ColumnInfo.newBuilder()
            .setId(4)
            .setName("phone")
            .setSqlType(Metapb.SQLType.newBuilder().setType(Basepb.DataType.Varchar).setNotNull(true).build())
            .setState(Metapb.MetaState.Public)
            .setPrimary(false).build();
    Metapb.ColumnInfo score = Metapb.ColumnInfo.newBuilder()
            .setId(5)
            .setName("score")
            .setSqlType(Metapb.SQLType.newBuilder().setType(Basepb.DataType.Double).setNotNull(true).build())
            .setState(Metapb.MetaState.Public)
            .setPrimary(false).build();
    Metapb.ColumnInfo salary = Metapb.ColumnInfo.newBuilder()
            .setId(6)
            .setName("salary")
            .setSqlType(Metapb.SQLType.newBuilder().setType(Basepb.DataType.Decimal).setNotNull(true).build())
            .setState(Metapb.MetaState.Public)
            .setPrimary(false).build();
    Metapb.ColumnInfo json = Metapb.ColumnInfo.newBuilder()
            .setId(7)
            .setName("json")
            .setSqlType(Metapb.SQLType.newBuilder().setType(Basepb.DataType.Json).setNotNull(true).build())
            .setState(Metapb.MetaState.Public)
            .setPrimary(false).build();
    Metapb.ColumnInfo date = Metapb.ColumnInfo.newBuilder()
            .setId(8)
            .setName("date")
            .setSqlType(Metapb.SQLType.newBuilder().setType(Basepb.DataType.Date).setNotNull(true).build())
            .setState(Metapb.MetaState.Public)
            .setPrimary(false).build();

    Metapb.ColumnInfo time = Metapb.ColumnInfo.newBuilder()
            .setId(9)
            .setName("time")
            .setSqlType(Metapb.SQLType.newBuilder().setType(Basepb.DataType.Time).setNotNull(true).build())
            .setState(Metapb.MetaState.Public)
            .setPrimary(false).build();

    Metapb.ColumnInfo remark = Metapb.ColumnInfo.newBuilder()
            .setId(10)
            .setName("remark")
            .setSqlType(Metapb.SQLType.newBuilder().setType(Basepb.DataType.Varchar).setNotNull(true).build())
            .setState(Metapb.MetaState.Public)
            .setPrimary(false).build();

    columnInfoList.add(id);
    columnInfoList.add(name);
    columnInfoList.add(age);
    columnInfoList.add(phone);
    columnInfoList.add(score);
    columnInfoList.add(salary);
    columnInfoList.add(json);
    columnInfoList.add(date);
    columnInfoList.add(time);
    columnInfoList.add(remark);

    /**
     *  table indices:
     *       UNIQUE KEY `uni_name_age_phone_score_salary` (`name`,`age`,`phone`,`score`,`salary` ),
     */
    List<Integer> primaryIndexColumns = new ArrayList<>();
    primaryIndexColumns.add(1);

    Metapb.IndexInfo primaryIndexInfo = Metapb.IndexInfo.newBuilder()
            .setId(1)
            .setName("primary")
            .addAllColumns(primaryIndexColumns)
            .setPrimary(true)
            .setType(Metapb.IndexType.BTree)
            .setState(Metapb.MetaState.Public).build();

    List<Integer> uniIndexColumns = new ArrayList<>();
    uniIndexColumns.add(2);
    uniIndexColumns.add(3);
    uniIndexColumns.add(4);
    uniIndexColumns.add(5);
    uniIndexColumns.add(6);
    uniIndexColumns.add(8);
    uniIndexColumns.add(9);

    Metapb.IndexInfo uniIndexInfo = Metapb.IndexInfo.newBuilder()
            .setId(2)
            .setName("idx_name_age_phone_salary_date_time")
            .addAllColumns(uniIndexColumns)
            .setPrimary(false)
            .setUnique(true)
            .setType(Metapb.IndexType.BTree)
            .setState(Metapb.MetaState.Public).build();

    List<Metapb.IndexInfo> indexInfos = new ArrayList<>();
    indexInfos.add(primaryIndexInfo);
    indexInfos.add(uniIndexInfo);

    Metapb.TableInfo tableInfo =
            Metapb.TableInfo.newBuilder()
                    .setId(1)
                    .setDbId(1)
                    .addPrimarys(1)
                    .setName("user_date_unique_idx")
                    .setState(Metapb.MetaState.Public)
                    .addAllColumns(columnInfoList)
                    .addAllIndices(indexInfos)
                    .build();

    return tableInfo;
  }
  private Metapb.TableInfo getFixTable() {
    List<Metapb.ColumnInfo> columnInfoList = Lists.newArrayListWithExpectedSize(20);
    // table columns: id(Int), name(Varchar), age(Int), phone(Varchar), score(Int), salary(Int), json(Json),date(Date),time(Time),remark(Varchar)

    Metapb.ColumnInfo id = Metapb.ColumnInfo.newBuilder()
            .setId(1)
            .setName("id")
            .setSqlType(Metapb.SQLType.newBuilder().setType(Basepb.DataType.BigInt).setNotNull(true).build())
            .setState(Metapb.MetaState.Public)
            .setPrimary(true).build();
    Metapb.ColumnInfo t_int = Metapb.ColumnInfo.newBuilder()
            .setId(2)
            .setName("t_int")
            .setSqlType(Metapb.SQLType.newBuilder().setType(Basepb.DataType.Int).setNotNull(true).build())
            .setState(Metapb.MetaState.Public)
            .setPrimary(false).build();
    Metapb.ColumnInfo t_unique = Metapb.ColumnInfo.newBuilder()
            .setId(3)
            .setName("t_unique")
            .setSqlType(Metapb.SQLType.newBuilder().setType(Basepb.DataType.Varchar).setNotNull(true).build())
            .setState(Metapb.MetaState.Public)
            .setPrimary(false).build();
    Metapb.ColumnInfo t_index = Metapb.ColumnInfo.newBuilder()
            .setId(4)
            .setName("t_index")
            .setSqlType(Metapb.SQLType.newBuilder().setType(Basepb.DataType.Varchar).setNotNull(true).build())
            .setState(Metapb.MetaState.Public)
            .setPrimary(false).build();
    Metapb.ColumnInfo t_tinyint = Metapb.ColumnInfo.newBuilder()
            .setId(5)
            .setName("t_tinyint")
            .setSqlType(Metapb.SQLType.newBuilder().setType(Basepb.DataType.TinyInt).setNotNull(true).build())
            .setState(Metapb.MetaState.Public)
            .setPrimary(false).build();
    Metapb.ColumnInfo t_smallint = Metapb.ColumnInfo.newBuilder()
            .setId(6)
            .setName("t_smallint")
            .setSqlType(Metapb.SQLType.newBuilder().setType(Basepb.DataType.SmallInt).setNotNull(true).build())
            .setState(Metapb.MetaState.Public)
            .setPrimary(false).build();
    Metapb.ColumnInfo t_varchar = Metapb.ColumnInfo.newBuilder()
            .setId(7)
            .setName("t_varchar")
            .setSqlType(Metapb.SQLType.newBuilder().setType(Basepb.DataType.Varchar).setNotNull(true).build())
            .setState(Metapb.MetaState.Public)
            .setPrimary(false).build();
    Metapb.ColumnInfo t_remark = Metapb.ColumnInfo.newBuilder()
            .setId(8)
            .setName("t_remark")
            .setSqlType(Metapb.SQLType.newBuilder().setType(Basepb.DataType.Json).setNotNull(true).build())
            .setState(Metapb.MetaState.Public)
            .setPrimary(false).build();

    Metapb.ColumnInfo t_float = Metapb.ColumnInfo.newBuilder()
            .setId(9)
            .setName("t_float")
            .setSqlType(Metapb.SQLType.newBuilder().setType(Basepb.DataType.Float).setNotNull(true).build())
            .setState(Metapb.MetaState.Public)
            .setPrimary(false).build();

    Metapb.ColumnInfo t_double = Metapb.ColumnInfo.newBuilder()
            .setId(10)
            .setName("t_double")
            .setSqlType(Metapb.SQLType.newBuilder().setType(Basepb.DataType.Double).setNotNull(true).build())
            .setState(Metapb.MetaState.Public)
            .setPrimary(false).build();

    columnInfoList.add(id);
    columnInfoList.add(t_int);
    columnInfoList.add(t_unique);
    columnInfoList.add(t_index);
    columnInfoList.add(t_tinyint);
    columnInfoList.add(t_smallint);
    columnInfoList.add(t_varchar);
    columnInfoList.add(t_remark);
    columnInfoList.add(t_float);
    columnInfoList.add(t_double);

    /**
     *  table indices:
     *       UNIQUE KEY `uni_name_age_phone_score_salary` (`name`,`age`,`phone`,`score`,`salary` ),
     */
    List<Integer> primaryIndexColumns = new ArrayList<>();
    primaryIndexColumns.add(1);

    Metapb.IndexInfo primaryIndexInfo = Metapb.IndexInfo.newBuilder()
            .setId(1)
            .setName("primary")
            .addAllColumns(primaryIndexColumns)
            .setPrimary(true)
            .setType(Metapb.IndexType.BTree)
            .setState(Metapb.MetaState.Public).build();

    List<Integer> uniIndexColumns = new ArrayList<>();
    uniIndexColumns.add(3);

    Metapb.IndexInfo uniIndexInfo = Metapb.IndexInfo.newBuilder()
            .setId(2)
            .setName("uni_t_unique")
            .addAllColumns(uniIndexColumns)
            .setPrimary(false)
            .setUnique(true)
            .setType(Metapb.IndexType.BTree)
            .setState(Metapb.MetaState.Public).build();

    List<Metapb.IndexInfo> indexInfos = new ArrayList<>();
    indexInfos.add(primaryIndexInfo);
    indexInfos.add(uniIndexInfo);

    Metapb.TableInfo tableInfo =
            Metapb.TableInfo.newBuilder()
                    .setId(1)
                    .setDbId(1)
                    .setName("fix")
                    .addPrimarys(1)
                    .setState(Metapb.MetaState.Public)
                    .addAllColumns(columnInfoList)
                    .addAllIndices(indexInfos)
                    .build();

    return tableInfo;
  }

}
