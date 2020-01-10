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

import io.jimdb.core.config.JimConfig;
import io.jimdb.meta.EtcdMetaStore;
import io.jimdb.core.model.meta.Column;
import io.jimdb.core.model.meta.MetaData;
import io.jimdb.core.model.meta.Table;
import io.jimdb.core.plugin.MetaStore;
import io.jimdb.test.mysql.SqlTestBase;
import io.jimdb.common.utils.lang.IOUtil;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @version V1.0
 */
public class ColumnTest extends SqlTestBase {
  private static final String OVER_NAME;
  private static final MetaStore metaStore;

  private String dbName;

  static {
    StringBuilder builder = new StringBuilder(128);
    for (int i = 0; i < 128; i++) {
      builder.append("a");
    }
    OVER_NAME = builder.toString();

    JimConfig config = new JimConfig(IOUtil.loadResource("jim_test.properties"));
    metaStore = new EtcdMetaStore();
    metaStore.init(config);
  }

  @Before
  public void tearUp() {
    dbName = "column_test_db" + System.nanoTime();
    createCatalog(dbName);

    String sql = String.format("use %s", dbName);
    execUpdate(sql, 0, true);
  }

  @After
  public void tearDown() {
    deleteCatalog(dbName);
  }

  @Test
  public void testAddError() {
    String sql = "ALTER TABLE column_test_tbl0 ADD COLUMN col1 varchar(32)";
    SQLException result = new SQLException("Unknown table 'column_test_tbl0'", "42S02", 1051);
    execUpdate(sql, result, true);

    sql = "CREATE TABLE column_test_tbl0(user varchar(32), host varchar(32), PRIMARY KEY(user,host)) COMMENT 'REPLICA=1' ENGINE=MEMORY";
    execUpdate(sql, 0, true);

    sql = "ALTER TABLE column_test_tbl0 ADD COLUMN " + OVER_NAME + " varchar(32)";
    result = new SQLException("Identifier name '" + OVER_NAME + "' is too long", "42000", 1059);
    execUpdate(sql, result, true);

    sql = "ALTER TABLE column_test_tbl0 ADD COLUMN (addcol1 varchar(32), addcol2 varchar(32))";
    result = new SQLException("This version of MySQL doesn't yet support 'schema batch add column'", "42000", 1235);
    execUpdate(sql, result, true);

    sql = "ALTER TABLE column_test_tbl0 ADD COLUMN addcol1 BIGINT UNSIGNED AUTO_INCREMENT";
    result = new SQLException("This version of MySQL doesn't yet support 'add autoIncr column'", "42000", 1235);
    execUpdate(sql, result, true);

    sql = "ALTER TABLE column_test_tbl0 ADD COLUMN addcol1 BIGINT PRIMARY KEY";
    result = new SQLException("This version of MySQL doesn't yet support 'add primary column'", "42000", 1235);
    execUpdate(sql, result, true);

    sql = "ALTER TABLE column_test_tbl0 ADD COLUMN addcol1 BIGINT after colnull";
    result = new SQLException("Unknown column 'colnull' in 'column_test_tbl0'", "42S22", 1054);
    execUpdate(sql, result, true);
  }

  @Test
  public void testAddDup() {
    String sql = "CREATE TABLE column_test_tbl0(user varchar(32), host varchar(32), PRIMARY KEY(user,host), col1 varchar(32), col1 varchar(32)) COMMENT 'REPLICA=1' ENGINE=MEMORY";
    SQLException result = new SQLException("Duplicate column name 'col1'", "42S21", 1060);
    execUpdate(sql, result, true);

    sql = "CREATE TABLE column_test_tbl0(user varchar(32), host varchar(32), PRIMARY KEY(user,host), col1 varchar(32)) COMMENT 'REPLICA=1' ENGINE=MEMORY";
    execUpdate(sql, 0, true);

    sql = "ALTER TABLE column_test_tbl0 ADD COLUMN col1 varchar(32)";
    result = new SQLException("Duplicate column name 'col1'", "42S21", 1060);
    execUpdate(sql, result, true);
  }

  @Test
  public void testCreate() {
    String sql = "CREATE TABLE column_test_tbl0(user varchar(32), host varchar(32), PRIMARY KEY(user,host), col1 varchar(32), col2 varchar(32)) COMMENT 'REPLICA=1' ENGINE=MEMORY";
    execUpdate(sql, 0, true);

    Table table = MetaData.Holder.getMetaData().getTable(dbName, "column_test_tbl0");
    Column[] columns = table.getReadableColumns();

    Assert.assertEquals(4, columns.length);
    Assert.assertEquals("user", columns[0].getName().toLowerCase());
    Assert.assertEquals("host", columns[1].getName().toLowerCase());
    Assert.assertEquals("col1", columns[2].getName().toLowerCase());
    Assert.assertEquals("col2", columns[3].getName().toLowerCase());

    sql = "ALTER TABLE column_test_tbl0 ADD COLUMN col3 varchar(32)";
    execUpdate(sql, 0, true);
    sql = "ALTER TABLE column_test_tbl0 ADD COLUMN col0 varchar(32) FIRST";
    execUpdate(sql, 0, true);
    sql = "ALTER TABLE column_test_tbl0 ADD COLUMN col4 varchar(32) after col1";
    execUpdate(sql, 0, true);

    table = MetaData.Holder.getMetaData().getTable(dbName, "column_test_tbl0");
    columns = table.getReadableColumns();

    Assert.assertEquals(7, columns.length);
    Assert.assertEquals("col0", columns[0].getName().toLowerCase());
    Assert.assertEquals("user", columns[1].getName().toLowerCase());
    Assert.assertEquals("host", columns[2].getName().toLowerCase());
    Assert.assertEquals("col1", columns[3].getName().toLowerCase());
    Assert.assertEquals("col4", columns[4].getName().toLowerCase());
    Assert.assertEquals("col2", columns[5].getName().toLowerCase());
    Assert.assertEquals("col3", columns[6].getName().toLowerCase());
  }

  @Test
  public void testDrop() {
    String sql = "ALTER TABLE column_test_tbl0 drop COLUMN col1";
    SQLException result = new SQLException("Unknown table 'column_test_tbl0'", "42S02", 1051);
    execUpdate(sql, result, true);

    sql = "CREATE TABLE column_test_tbl0(user varchar(32), host varchar(32), col0 varchar(32), col1 varchar(32), col2 varchar(32),  PRIMARY KEY(user,host)) COMMENT 'REPLICA=1' ENGINE=MEMORY";
    execUpdate(sql, 0, true);

    sql = "ALTER TABLE column_test_tbl0 drop COLUMN colnull";
    result = new SQLException("Unknown column 'colnull' in 'column_test_tbl0'", "42S22", 1054);
    execUpdate(sql, result, true);

    sql = "ALTER TABLE column_test_tbl0 drop COLUMN col0, drop COLUMN col2";
    execUpdate(sql, 0, true);

    Table table = MetaData.Holder.getMetaData().getTable(dbName, "column_test_tbl0");
    Column[] columns = table.getReadableColumns();
    Assert.assertEquals(3, columns.length);
    Assert.assertEquals("user", columns[0].getName().toLowerCase());
    Assert.assertEquals("host", columns[1].getName().toLowerCase());
    Assert.assertEquals("col1", columns[2].getName().toLowerCase());
  }

//    +-------+-------------+------+-----+---------+-------+
//    | Field | Type        | Null | Key | Default | Extra |
//    +-------+-------------+------+-----+---------+-------+
//    | id    | bigint      | NO   | PRI | NULL    |       |
//    | num   | bigint      | YES  |     | NULL    |       |
//    | name  | varchar(20) | NO   |     | NULL    |       |
//    +-------+-------------+------+----+----------+-------+
//  @Test
//  public void testColumnNull() throws Exception {
//    String sql = String.format("create table %s (id bigint primary key, num bigint, name varchar(20) not null) %s", tableName, EXTRA);
//
//    Table table = createAndGetTable(sql);
//
//    Column pkColumn = table.getWritableColumn("id");
//    Metapb.SQLType pkSqlType = pkColumn.getType();
//    Assert.assertEquals(pkSqlType.getType(), Basepb.DataType.BigInt);
//    Assert.assertEquals(pkSqlType.getNotNull(), true);
//    Assert.assertEquals(pkColumn.getDefaultValue(), NullValue.getInstance());
//
//    Column commonColumn = table.getWritableColumn("num");
//    Metapb.SQLType commonSqlType = commonColumn.getType();
//    Assert.assertEquals(commonSqlType.getType(), Basepb.DataType.BigInt);
//    Assert.assertEquals(commonSqlType.getNotNull(), false);
//    Assert.assertEquals(commonColumn.getDefaultValue(), NullValue.getInstance());
//
//    commonColumn = table.getWritableColumn("name");
//    commonSqlType = commonColumn.getType();
//    Assert.assertEquals(commonSqlType.getType(), Basepb.DataType.Varchar);
//    Assert.assertEquals(commonSqlType.getPrecision(), 20);
//    Assert.assertEquals(commonSqlType.getNotNull(), true);
//    Assert.assertEquals(commonColumn.getDefaultValue(), NullValue.getInstance());
//  }
//
//  private Table createAndGetTable(String sql) {
//    execUpdate(sql, 0, true);
//    MetaData metaData = MetaData.Holder.getMetaData();
//    return metaData.getTable(catalogName, tableName);
//  }

//----start-------------- data type: precision and scale ------------------------
//number precision
//    String sql = "CREATE TABLE new_tbl(id bigint(10) primary key);";
//    String sql = "CREATE TABLE new_tbl(id bigint(10, 2) primary key);"; //parse error
//number signed
//    String sql = "CREATE TABLE new_tbl(id bigint(10) primary key);"; //default signed
//    String sql = "CREATE TABLE new_tbl(id bigint(10)  ZEROFILL primary key);"; //default unsigned
//    String sql = "CREATE TABLE new_tbl(id bigint(10) UNSIGNED primary key);";
//int number scope
//    String sql = "CREATE TABLE new_tbl(id bigint(-1) primary key)";//parse error
//    String sql = "CREATE TABLE new_tbl(id bigint(0) primary key)"; // precision = 0;
//    String sql = "CREATE TABLE new_tbl(id bigint(256) primary key)";//max
//  @Test
//  public void testNumber() {
//    String sql = String.format("CREATE TABLE %s(id bigint(10) primary key) %s", tableName, EXTRA);
//    Table table = createAndGetTable(sql);
//
//    Column pkColumn = table.getWritableColumn("id");
//    Metapb.SQLType pkSqlType = pkColumn.getType();
//    Assert.assertEquals(pkSqlType.getType(), Basepb.DataType.BigInt);
//    Assert.assertEquals(pkSqlType.getNotNull(), true);
//    Assert.assertEquals(pkColumn.getDefaultValue(), NullValue.getInstance());
//  }
//  //bit scope
////    String sql = "CREATE TABLE new_tbl(id bit(-1) primary key)";//parse error
////    String sql = "CREATE TABLE new_tbl(id bit(0) primary key)";//precision = 1
////    String sql = "CREATE TABLE new_tbl(id bit(65) primary key)";//max()
//
//  @Test
//  public void testBit() {
//  }
//
//  //decimal
//  //precision
////    String sql = "create table new_tbl(id bigint primary key, num decimal);";  //(10,0)
////    String sql = "create table new_tbl(id bigint primary key, num decimal(8));";//(8,0)
////    String sql = "create table new_tbl(id bigint primary key, num decimal(-1));";//parse error
////    String sql = "create table new_tbl(id bigint primary key, num decimal(66));";//max()
//
//  //scale
////    String sql = "create table new_tbl(id bigint primary key, num decimal(1, -1));";//parse error
////    String sql = "create table new_tbl(id bigint primary key, num decimal(1, 32));";//Maximum is 30
////    String sql = "create table new_tbl(id bigint primary key, num decimal(1, 2));"; //M >= D
////    String sql = "create table new_tbl(id bigint primary key, num decimal(3, 2));";
//
//  //float
////    String sql = "create table new_tbl(id bigint primary key, num float);"; //no default handle
////    String sql = "create table new_tbl(id bigint primary key, num float(10,2));";
////    String sql = "create table new_tbl(id bigint primary key, num float(256,2));"; //width max = 255
////    String sql = "create table new_tbl(id bigint primary key, num float(200, 255));"; //scale Maximum is 30
////    String sql = "create table new_tbl(id bigint primary key, num float(-1));"; //parse err
////    String sql = "create table new_tbl(id bigint primary key, num float(-1, 2));"; //parse err
////    String sql = "create table new_tbl(id bigint primary key, num float(2, -1));"; //parse err
////    String sql = "create table new_tbl(id bigint primary key, num float(29, 30));"; // M >= D
////    String sql = "create table new_tbl(id bigint primary key, num float(100));"; //specifier err
////    String sql = "create table new_tbl(id bigint primary key, num float(23))";
////    String sql = "create table new_tbl(id bigint primary key, num float(25))";//float change to double
//
//  //double
////    String sql = "create table new_tbl(id bigint primary key, num double);"; //no default handle
////    String sql = "create table new_tbl(id bigint primary key, num double(10,2));";
////    String sql = "create table new_tbl(id bigint primary key, num double(256,2));"; //width max = 255
////    String sql = "create table new_tbl(id bigint primary key, num double(200, 255));"; //scale Maximum is 30
////    String sql = "create table new_tbl(id bigint primary key, num double(-1));"; //parse err
////    String sql = "create table new_tbl(id bigint primary key, num double(-1, 2));"; //parse err
////    String sql = "create table new_tbl(id bigint primary key, num double(2, -1));"; //parse err
////    String sql = "create table new_tbl(id bigint primary key, num double(29, 30));"; // M >= D
////    String sql = "create table new_tbl(id bigint primary key, num double(100));"; //parse err
////    String sql = "create table new_tbl(id bigint primary key, num double(23));";//parse err
////    String sql = "create table new_tbl(id bigint primary key, num double(25))";//parse err
//
//  //year
////    String sql = "create table new_tbl(id bigint primary key, num year);"; // default precision = 4
////    String sql = "create table new_tbl(id bigint primary key, num year(1));"; // precision = 4
////    String sql = "create table new_tbl(id bigint primary key, num year(2));"; // precision = 2
////    String sql = "create table new_tbl(id bigint primary key, num year(3));"; // precision = 4
////    String sql = "create table new_tbl(id bigint primary key, num year(4));"; // precision = 4
////    String sql = "create table new_tbl(id bigint primary key, num year(5));"; // precision = 4
////    String sql = "create table new_tbl(id bigint primary key, num year(-1));"; // parse err
//
//  //datetime
////    String sql = "create table new_tbl(id bigint primary key, num datetime(-1));"; // parse err
////    String sql = "create table new_tbl(id bigint primary key, num datetime);";
////    String sql = "create table new_tbl(id bigint primary key, num datetime(1));"; // precison = 1
////    String sql = "create table new_tbl(id bigint primary key, num datetime(6));"; // precison = 6
////    String sql = "create table new_tbl(id bigint primary key, num datetime(7));"; //  Maximum is 6.
//  //timestamp
////    String sql = "create table new_tbl(id bigint primary key, num timestamp(-1));"; // parse err
////    String sql = "create table new_tbl(id bigint primary key, num timestamp);";
////    String sql = "create table new_tbl(id bigint primary key, num timestamp(1));"; // precison = 1
////    String sql = "create table new_tbl(id bigint primary key, num timestamp(6));"; // precison = 6
////    String sql = "create table new_tbl(id bigint primary key, num timestamp(7));"; //  Maximum is 6.
//  //time
////    String sql = "create table new_tbl(id bigint primary key, num time(-1));"; // parse err
////    String sql = "create table new_tbl(id bigint primary key, num time);";
////    String sql = "create table new_tbl(id bigint primary key, num time(1));"; // precison = 1
////    String sql = "create table new_tbl(id bigint primary key, num time(6));"; // precison = 6
////    String sql = "create table new_tbl(id bigint primary key, num time(7));"; //  Maximum is 6.
//
//  //varchar
////    String sql = "create table new_tbl(id bigint primary key, name VARCHAR(20) CHARACTER SET utf8);";
//
//  //----end-------------- data type: precision and scale ------------------------
//
//  //      +-------+------------+------+-----+---------+-------+
////      | Field | Type       | Null | Key | Default | Extra |
////      +-------+------------+------+-----+---------+-------+
////      | id    | bigint     | NO   | PRI | NULL    |       |
////      | num   | bigint     | NO   |     | 4       |       |
////      +-------+------------+------+-----+---------+-------+
//  @Test
//  public void testColumnDefault() throws Exception {
//    String sql = String.format("create table %s (id bigint primary key, num bigint not null default 4) %s;",
//            tableName, EXTRA);
//    Table table = createAndGetTable(sql);
//
//    Column commonColumn = table.getWritableColumn("num");
//    Metapb.SQLType commonSqlType = commonColumn.getType();
//    Assert.assertEquals(commonSqlType.getType(), Basepb.DataType.BigInt);
//    Assert.assertEquals(commonSqlType.getNotNull(), true);
//    LongValue value = (LongValue) (commonColumn.getDefaultValue());
//    Assert.assertEquals(value.getValue(), 4);
//  }
//
//  /**
//   * one: primary key column no default value
//   * two: the type of column default value error, string to bigint
//   */
//  @Test
//  public void testColumnDefaultError() {
//    String sql = String.format("create table %s(id bigint primary key default 'a') %s;", tableName, EXTRA);
//    SQLException result = new SQLException("Invalid default value for 'id'", "42000", 1067);
//    execUpdate(sql, result, true);
//
//    sql = String.format("create table %s(id bigint primary key, num bigint default 'a') %s;", tableName, EXTRA);
//    result = new SQLException("Invalid default value for 'num'", "42000", 1067);
//    execUpdate(sql, result, true);
//  }
//
//  @Test
//  public void testDateTimeDefault() {
//    String sql = String.format("create table %s (id bigint primary key, birth datetime not null default '2000-01-01 00:00:00') %s;",
//            tableName, EXTRA);
//    Table table = createAndGetTable(sql);
//
//    Column datetimeCol = table.getWritableColumn("birth");
//    Metapb.SQLType datetimeColType = datetimeCol.getType();
//    Assert.assertEquals(datetimeColType.getType(), Basepb.DataType.DateTime);
//    Assert.assertEquals(datetimeColType.getNotNull(), true);
//    DateValue value = (DateValue) (datetimeCol.getDefaultValue());
//    Assert.assertEquals(value.convertToString(null), "2000-01-01 00:00:00");
//  }
//
//  @Test
//  public void testTimestampDefault() {
//    String sql = String.format("create table %s (id bigint primary key, birth timestamp not null default '2000-01-01 00:00:00') %s;",
//            tableName, EXTRA);
//    Table table = createAndGetTable(sql);
//
//    Column datetimeCol = table.getWritableColumn("birth");
//    Metapb.SQLType datetimeColType = datetimeCol.getType();
//    Assert.assertEquals(datetimeColType.getType(), Basepb.DataType.TimeStamp);
//    Assert.assertEquals(datetimeColType.getNotNull(), true);
//    DateValue value = (DateValue) (datetimeCol.getDefaultValue());
//    Assert.assertEquals(value.convertToString(null), "1999-12-31 16:00:00");
//  }
}
