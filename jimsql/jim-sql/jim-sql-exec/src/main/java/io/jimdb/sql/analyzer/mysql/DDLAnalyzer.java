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
package io.jimdb.sql.analyzer.mysql;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.utils.os.SystemClock;
import io.jimdb.core.Session;
import io.jimdb.core.codec.Codec;
import io.jimdb.core.codec.ValueCodec;
import io.jimdb.core.model.privilege.PrivilegeInfo;
import io.jimdb.core.model.privilege.PrivilegeType;
import io.jimdb.core.types.Types;
import io.jimdb.core.values.DateValue;
import io.jimdb.core.values.LongValue;
import io.jimdb.core.values.StringValue;
import io.jimdb.core.values.Value;
import io.jimdb.core.values.ValueConvertor;
import io.jimdb.pb.Basepb.DataType;
import io.jimdb.pb.Basepb.StoreType;
import io.jimdb.pb.Ddlpb;
import io.jimdb.pb.Ddlpb.AddIndexInfo;
import io.jimdb.pb.Ddlpb.AlterTableInfo;
import io.jimdb.pb.Ddlpb.OpType;
import io.jimdb.pb.Metapb.CatalogInfo;
import io.jimdb.pb.Metapb.ColumnInfo;
import io.jimdb.pb.Metapb.IndexInfo;
import io.jimdb.pb.Metapb.IndexType;
import io.jimdb.pb.Metapb.MetaState;
import io.jimdb.pb.Metapb.SQLType;
import io.jimdb.pb.Metapb.TableInfo;
import io.jimdb.sql.analyzer.AnalyzerUtil;
import io.jimdb.sql.ddl.DDLUtils;
import io.jimdb.sql.operator.DDL;
import io.netty.buffer.ByteBuf;

import org.apache.commons.lang3.StringUtils;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLPartition;
import com.alibaba.druid.sql.ast.SQLPartitionBy;
import com.alibaba.druid.sql.ast.SQLPartitionByRange;
import com.alibaba.druid.sql.ast.SQLPartitionValue;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.expr.SQLNullExpr;
import com.alibaba.druid.sql.ast.expr.SQLNumberExpr;
import com.alibaba.druid.sql.ast.expr.SQLNumericLiteralExpr;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableAddColumn;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableAddConstraint;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableAddIndex;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableDropColumnItem;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableDropIndex;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableDropKey;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableItem;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableRenameIndex;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLColumnConstraint;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLColumnPrimaryKey;
import com.alibaba.druid.sql.ast.statement.SQLColumnReference;
import com.alibaba.druid.sql.ast.statement.SQLColumnUniqueKey;
import com.alibaba.druid.sql.ast.statement.SQLConstraint;
import com.alibaba.druid.sql.ast.statement.SQLCreateDatabaseStatement;
import com.alibaba.druid.sql.ast.statement.SQLCreateIndexStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropDatabaseStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropIndexStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLNotNullConstraint;
import com.alibaba.druid.sql.ast.statement.SQLNullConstraint;
import com.alibaba.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlKey;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlPrimaryKey;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlUnique;
import com.alibaba.druid.sql.dialect.mysql.ast.MysqlForeignKey;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableOption;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlRenameTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlTableIndex;
import com.google.protobuf.ByteString;
import com.google.protobuf.NettyByteString;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.util.function.Tuple2;

/**
 * @version V1.0
 */
@SuppressFBWarnings("EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS")
final class DDLAnalyzer {
  private static final int MAXLEN_DBNAME = 64;

  private static final int MAXLEN_TABLENAME = 64;

  private static final int MAXLEN_INDEXNAME = 64;

  private static final int MAXLEN_COLUMNNAME = 64;

  private static final int MAXLEN_COMMENT = 1024;

  private static final int MAX_KEY_COLUMNS = 16;

  private static final int MAX_TABLE_COLUMNS = 512;

  private static final int DEFAULT_RANGE_NUM = 5;

  private static final String SYSTEM_DB = "mysql";

  private static final String[] RESERVES = {};

  private DDLAnalyzer() {
  }

  static DDL analyzeCreateDatabase(Session session, SQLCreateDatabaseStatement stmt) {
    String name = getName(stmt.getName());
    verifyDatabaseName(name, false);

    List<PrivilegeInfo> privilegeInfos = session.getStmtContext().getPrivilegeInfos();
    privilegeInfos.add(new PrivilegeInfo(name, "", "", PrivilegeType.CREATE_PRIV));

    CatalogInfo.Builder builder = CatalogInfo.newBuilder();
    builder.setName(name)
            .setState(MetaState.Absent)
            .setCreateTime(SystemClock.currentTimeMillis());
    return new DDL(OpType.CreateCatalog, builder.build(), stmt.isIfNotExists());
  }

  static DDL analyzeDropDatabase(Session session, SQLDropDatabaseStatement stmt) {
    String name = getName((SQLName) stmt.getDatabase());
    verifyDatabaseName(name, true);

    List<PrivilegeInfo> privilegeInfos = session.getStmtContext().getPrivilegeInfos();
    privilegeInfos.add(new PrivilegeInfo(name, "", "", PrivilegeType.DROP_PRIV));

    CatalogInfo.Builder builder = CatalogInfo.newBuilder();
    builder.setName(name)
            .setDeleteTime(SystemClock.currentTimeMillis())
            .setState(MetaState.Public);
    return new DDL(OpType.DropCatalog, builder.build(), stmt.isIfExists());
  }

  static DDL analyzeCreateTable(Session session, MySqlCreateTableStatement stmt) {
    // verify name
    Tuple2<String, String> names = getAndVerifyTableName(stmt.getTableSource().getExpr(), false);
    String dbName = names.getT1();
    String tableName = names.getT2();

    // add privilegeInfo
    List<PrivilegeInfo> privilegeInfos = session.getStmtContext().getPrivilegeInfos();
    privilegeInfos.add(new PrivilegeInfo(dbName, tableName, "", PrivilegeType.CREATE_PRIV));

    // unsupport Grammar
    if (stmt.getType() != null) {
      throw DBException.get(ErrorModule.DDL, ErrorCode.ER_NOT_SUPPORTED_YET, "Create TEMPORARY Table");
    }
    if (stmt.getLike() != null) {
      throw DBException.get(ErrorModule.DDL, ErrorCode.ER_NOT_SUPPORTED_YET, "Create Table LIKE table");
    }
    if (stmt.getSelect() != null) {
      throw DBException.get(ErrorModule.DDL, ErrorCode.ER_NOT_SUPPORTED_YET, "Create Table ... As query");
    }

    // build primary constraint
    List<SQLColumnDefinition> colDefines = new ArrayList<>();
    List<SQLTableElement> constraints = new ArrayList<>();
    List<String> primarys = buildPrimaryConstraint(dbName, tableName, stmt.getTableElementList(), colDefines, constraints);
    constraints = verifyConstraint(constraints);

    // build column
    Map<String, ColumnInfo> columns = buildColumns(session, dbName, tableName, colDefines);
    if (columns.isEmpty()) {
      throw DBException.get(ErrorModule.DDL, ErrorCode.ER_TABLE_MUST_HAVE_COLUMNS);
    }
    if (columns.size() > MAX_TABLE_COLUMNS) {
      throw DBException.get(ErrorModule.DDL, ErrorCode.ER_TOO_MANY_FIELDS);
    }

    // build index
    List<IndexInfo> indexInfos = buildIndex(dbName, tableName, constraints, columns);
    // replica Represented in comment: replica=3
    int replica = analyzeReplica(stmt);
    String comment = stmt.getComment() == null ? "" : stmt.getComment().toString();
    if (comment.length() > MAXLEN_COMMENT) {
      throw DBException.get(ErrorModule.DDL, ErrorCode.ER_TOO_LONG_TABLE_COMMENT, tableName, String.valueOf(MAXLEN_COMMENT));
    }

    List<Integer> primaryIDs = new ArrayList<>(primarys.size());
    for (String primary : primarys) {
      primaryIDs.add(columns.get(primary.toLowerCase()).getId());
    }
    List<ColumnInfo> columnInfos = new ArrayList<>(columns.size());
    for (ColumnInfo column : columns.values()) {
      columnInfos.add(column);
    }
    Collections.sort(columnInfos, (c1, c2) -> {
      if (c1.getOffset() == c2.getOffset()) {
        return 0;
      }
      return (c1.getOffset() > c2.getOffset()) ? 1 : -1;
    });

    TableInfo.Builder builder = TableInfo.newBuilder();
    builder.setName(dbName + "." + tableName)
            .setMaxColumnId(columns.size())
            .setReplicas(replica)
            .setSplitNum(DEFAULT_RANGE_NUM)
            .addAllColumns(columnInfos)
            .addAllPrimarys(primaryIDs)
            .addAllIndices(indexInfos)
            .setComment(comment)
            .setCreateTime(SystemClock.currentTimeMillis());
    // build split key
    buildSplitKey(builder, stmt.getPartitioning(), columns.get(primarys.get(0).toLowerCase()));
    analyzeTableOptions(builder, stmt);
    return new DDL(OpType.CreateTable, builder.build(), stmt.isIfNotExiists());
  }

  static DDL analyzeDropTable(Session session, SQLDropTableStatement stmt) {
    List<SQLExprTableSource> tableSourceList = stmt.getTableSources();
    List<AlterTableInfo> alterInfos = new ArrayList<>(tableSourceList.size());
    List<PrivilegeInfo> privilegeInfos = session.getStmtContext().getPrivilegeInfos();

    for (SQLExprTableSource table : tableSourceList) {
      Tuple2<String, String> names = getAndVerifyTableName(table.getExpr(), true);

      // add privilegeInfo
      privilegeInfos.add(new PrivilegeInfo(names.getT1(), names.getT2(), "", PrivilegeType.DROP_PRIV));

      AlterTableInfo.Builder builder = AlterTableInfo.newBuilder();
      builder.setDbName(names.getT1())
              .setTableName(names.getT2())
              .setType(OpType.DropTable)
              .setState(MetaState.Public);
      alterInfos.add(builder.build());
    }
    return new DDL(OpType.DropTable, alterInfos, stmt.isIfExists());
  }

  static DDL analyzeRenameTable(Session session, MySqlRenameTableStatement stmt) {
    if (stmt.getItems().size() != 1) {
      throw DBException.get(ErrorModule.DDL, ErrorCode.ER_NOT_SUPPORTED_YET, "schema batch rename table");
    }

    MySqlRenameTableStatement.Item item = stmt.getItems().get(0);
    Tuple2<String, String> from = getAndVerifyTableName(item.getName(), true);
    Tuple2<String, String> to = getAndVerifyTableName(item.getTo(), true);
    if (!from.getT1().equalsIgnoreCase(to.getT1())) {
      throw DBException.get(ErrorModule.DDL, ErrorCode.ER_NOT_SUPPORTED_YET, "schema rename table between different databases");
    }
    if (from.getT2().equalsIgnoreCase(to.getT2())) {
      throw DBException.get(ErrorModule.DDL, ErrorCode.ER_NOT_SUPPORTED_YET, "schema rename table to the same name");
    }

    // add privilegeInfo
    List<PrivilegeInfo> privilegeInfos = session.getStmtContext().getPrivilegeInfos();
    privilegeInfos.add(new PrivilegeInfo(from.getT1(), from.getT2(), "", PrivilegeType.ALTER_PRIV));
    privilegeInfos.add(new PrivilegeInfo(from.getT1(), from.getT2(), "", PrivilegeType.DROP_PRIV));
    privilegeInfos.add(new PrivilegeInfo(to.getT1(), to.getT2(), "", PrivilegeType.CREATE_PRIV));
    privilegeInfos.add(new PrivilegeInfo(to.getT1(), to.getT2(), "", PrivilegeType.INSERT_PRIV));

    TableInfo tableInfo = TableInfo.newBuilder()
            .setName(to.getT2())
            .setState(MetaState.Absent)
            .build();
    AlterTableInfo alterInfo = AlterTableInfo.newBuilder()
            .setType(OpType.RenameTable)
            .setDbName(from.getT1())
            .setTableName(from.getT2())
            .setState(MetaState.Absent)
            .setItem(tableInfo.toByteString())
            .build();
    return new DDL(OpType.AlterTable, Collections.singletonList(alterInfo), false);
  }

  static DDL analyzeAlterTable(Session session, SQLAlterTableStatement stmt) {
    SQLExpr name = stmt.getTableSource().getExpr();
    Tuple2<String, String> names = getAndVerifyTableName(name, true);
    String dbName = names.getT1();
    String tableName = names.getT2();

    // add privilegeInfo
    List<PrivilegeInfo> privilegeInfos = session.getStmtContext().getPrivilegeInfos();
    privilegeInfos.add(new PrivilegeInfo(names.getT1(), names.getT2(), "", PrivilegeType.ALTER_PRIV));

    List<AlterTableInfo> alterInfos = new ArrayList<>(stmt.getItems().size());
    for (SQLAlterTableItem item : stmt.getItems()) {
      // add index
      if (item instanceof SQLAlterTableAddIndex) {
        privilegeInfos.add(new PrivilegeInfo(names.getT1(), names.getT2(), "", PrivilegeType.INDEX_PRIV));
        AlterTableInfo value = analyzeAlterAddIndex((SQLAlterTableAddIndex) item, dbName, tableName);
        alterInfos.add(value);
        continue;
      }
      // drop index
      if (item instanceof SQLAlterTableDropIndex) {
        privilegeInfos.add(new PrivilegeInfo(names.getT1(), names.getT2(), "", PrivilegeType.INDEX_PRIV));
        AlterTableInfo value = analyzeAlterDropIndex(item, dbName, tableName);
        alterInfos.add(value);
        continue;
      }
      if (item instanceof SQLAlterTableDropKey) {
        AlterTableInfo value = analyzeAlterDropIndex(item, dbName, tableName);
        alterInfos.add(value);
        continue;
      }
      if (item instanceof SQLAlterTableAddConstraint) {
        AlterTableInfo value = analyzeAlterAddConstraint((SQLAlterTableAddConstraint) item, dbName, tableName);
        alterInfos.add(value);
        continue;
      }
      // add column
      if (item instanceof SQLAlterTableAddColumn) {
        AlterTableInfo value = analyzeAlterAddColumn(session, (SQLAlterTableAddColumn) item, dbName, tableName);
        alterInfos.add(value);
        continue;
      }
      // drop column
      if (item instanceof SQLAlterTableDropColumnItem) {
        AlterTableInfo value = analyzeAlterDropColumn((SQLAlterTableDropColumnItem) item, dbName, tableName);
        alterInfos.add(value);
        continue;
      }
      // rename index
      if (item instanceof SQLAlterTableRenameIndex) {
        AlterTableInfo value = analyzeAlterRenameIndex((SQLAlterTableRenameIndex) item, dbName, tableName);
        alterInfos.add(value);
        continue;
      }
      // alter table option
      if (item instanceof MySqlAlterTableOption) {
        AlterTableInfo value = analyzeAlterTableOption((MySqlAlterTableOption) item, dbName, tableName);
        alterInfos.add(value);
        continue;
      }

      throw DBException.get(ErrorModule.DDL, ErrorCode.ER_NOT_SUPPORTED_YET, "Alter Table Item");
    }

    return new DDL(OpType.AlterTable, alterInfos, false);
  }

  static DDL analyzeCreateIndex(Session session, SQLCreateIndexStatement stmt) {
    Tuple2<String, String> names = getAndVerifyTableName(((SQLExprTableSource) stmt.getTable()).getExpr(), true);
    String dbName = names.getT1();
    String tableName = names.getT2();
    String indexName = getName(stmt.getName());
    String indexType = StringUtils.isBlank(stmt.getUsing()) ? "" : stmt.getUsing();
    String comment = stmt.getComment() == null ? "" : stmt.getComment().toString();
    boolean unique = false;
    if (StringUtils.isNotBlank(stmt.getType())) {
      if ("unique".equalsIgnoreCase(stmt.getType())) {
        unique = true;
      } else {
        throw DBException.get(ErrorModule.DDL, ErrorCode.ER_NOT_SUPPORTED_YET, stmt.getType() + " index");
      }
    }

    List<PrivilegeInfo> privilegeInfos = session.getStmtContext().getPrivilegeInfos();
    privilegeInfos.add(new PrivilegeInfo(dbName, tableName, "", PrivilegeType.INDEX_PRIV));

    List<String> columns = verifyIndex(dbName, tableName, indexName, indexType, comment, false, stmt.getItems());
    AddIndexInfo indexInfo = AddIndexInfo.newBuilder()
            .setName(indexName)
            .setUnique(unique)
            .setPrimary(false)
            .addAllColumns(columns)
            .setComment(comment)
            .setType(IndexType.BTree)
            .setState(MetaState.Absent)
            .setCreateTime(SystemClock.currentTimeMillis())
            .build();

    AlterTableInfo alterInfo = AlterTableInfo.newBuilder()
            .setType(OpType.AddIndex)
            .setDbName(dbName)
            .setTableName(tableName)
            .setState(MetaState.Absent)
            .setItem(indexInfo.toByteString())
            .build();
    return new DDL(OpType.AlterTable, Collections.singletonList(alterInfo), false);
  }

  static DDL analyzeDropIndex(Session session, SQLDropIndexStatement stmt) {
    Tuple2<String, String> names = getAndVerifyTableName(stmt.getTableName().getExpr(), true);
    String indexName = getName(stmt.getIndexName());
    verifyIndexName(indexName, true);

    List<PrivilegeInfo> privilegeInfos = session.getStmtContext().getPrivilegeInfos();
    privilegeInfos.add(new PrivilegeInfo(names.getT1(), names.getT2(), "", PrivilegeType.INDEX_PRIV));

    IndexInfo indexInfo = IndexInfo.newBuilder().setName(indexName).build();
    AlterTableInfo alterInfo = AlterTableInfo.newBuilder()
            .setType(OpType.DropIndex)
            .setDbName(names.getT1())
            .setTableName(names.getT2())
            .setState(MetaState.Public)
            .setItem(indexInfo.toByteString())
            .build();
    return new DDL(OpType.AlterTable, Collections.singletonList(alterInfo), false);
  }

  private static AlterTableInfo analyzeAlterAddColumn(Session session, SQLAlterTableAddColumn stmt,
                                                      String dbName, String tableName) {
    if (stmt.getColumns().size() > 1) {
      throw DBException.get(ErrorModule.DDL, ErrorCode.ER_NOT_SUPPORTED_YET, "schema batch add column");
    }
    if (stmt.getFirstColumn() != null) {
      throw DBException.get(ErrorModule.DDL, ErrorCode.ER_NOT_SUPPORTED_YET, "schema add column by first column");
    }

    final String afterColumn = stmt.getAfterColumn() == null ? "" : splitColumnName(dbName, tableName, stmt.getAfterColumn());
    // build column
    ColumnInfo column = buildColumns(session, dbName, tableName, stmt.getColumns()).values().toArray(new ColumnInfo[0])[0];
    if (column.getAutoIncr()) {
      throw DBException.get(ErrorModule.DDL, ErrorCode.ER_NOT_SUPPORTED_YET, "add autoIncr column");
    }
    if (column.getPrimary()) {
      throw DBException.get(ErrorModule.DDL, ErrorCode.ER_NOT_SUPPORTED_YET, "add primary column");
    }
    Ddlpb.AddColumnInfo addColumn = Ddlpb.AddColumnInfo.newBuilder()
            .setName(column.getName())
            .setAfterName(afterColumn)
            .setFirst(stmt.isFirst())
            .setSqlType(column.getSqlType())
            .setDefaultValue(column.getDefaultValue())
            .setReorgValue(column.getReorgValue())
            .setHasDefault(column.getHasDefault())
            .setComment(column.getComment())
            .setState(MetaState.Absent)
            .setCreateTime(SystemClock.currentTimeMillis())
            .build();

    AlterTableInfo.Builder builder = AlterTableInfo.newBuilder();
    builder.setType(OpType.AddColumn)
            .setItem(addColumn.toByteString())
            .setDbName(dbName)
            .setTableName(tableName)
            .setState(MetaState.Absent);
    return builder.build();
  }

  private static AlterTableInfo analyzeAlterDropColumn(SQLAlterTableDropColumnItem stmt, String dbName, String tableName) {
    if (stmt.getColumns().size() > 1) {
      throw DBException.get(ErrorModule.DDL, ErrorCode.ER_NOT_SUPPORTED_YET, "schema batch drop column");
    }

    String colName = splitColumnName(dbName, tableName, stmt.getColumns().get(0));
    ColumnInfo columnInfo = ColumnInfo.newBuilder()
            .setName(colName)
            .setState(MetaState.Public)
            .build();

    AlterTableInfo.Builder builder = AlterTableInfo.newBuilder();
    builder.setType(OpType.DropColumn)
            .setItem(columnInfo.toByteString())
            .setDbName(dbName)
            .setTableName(tableName)
            .setState(MetaState.Public);
    return builder.build();
  }

  private static AlterTableInfo analyzeAlterAddConstraint(SQLAlterTableAddConstraint stmt, String dbName, String tableName) {
    SQLConstraint constraint = stmt.getConstraint();
    if (constraint instanceof MySqlUnique) {
      MySqlUnique uniqueIndex = (MySqlUnique) constraint;
      String indexName = getName(uniqueIndex.getName());
      String indexType = StringUtils.isBlank(uniqueIndex.getIndexType()) ? "" : uniqueIndex.getIndexType();
      String comment = uniqueIndex.getComment() == null ? "" : uniqueIndex.getComment().toString();

      List<String> columns = verifyIndex(dbName, tableName, indexName, indexType, comment, false, uniqueIndex.getColumns());
      AddIndexInfo indexInfo = AddIndexInfo.newBuilder()
              .setName(indexName)
              .setPrimary(false)
              .setUnique(true)
              .setComment(comment)
              .setType(IndexType.BTree)
              .addAllColumns(columns)
              .setState(MetaState.Absent)
              .setCreateTime(SystemClock.currentTimeMillis())
              .build();

      AlterTableInfo.Builder builder = AlterTableInfo.newBuilder();
      builder.setType(OpType.AddIndex)
              .setItem(indexInfo.toByteString())
              .setDbName(dbName)
              .setTableName(tableName)
              .setState(MetaState.Absent);
      return builder.build();
    }

    throw DBException.get(ErrorModule.DDL, ErrorCode.ER_NOT_SUPPORTED_YET, "AddConstraint Type");
  }

  private static AlterTableInfo analyzeAlterAddIndex(SQLAlterTableAddIndex stmt, String dbName, String tableName) {
    String indexName = getName(stmt.getName());
    String indexType = StringUtils.isBlank(stmt.getType()) ? "" : stmt.getType();
    String comment = stmt.getComment() == null ? "" : stmt.getComment().toString();

    List<String> columns = verifyIndex(dbName, tableName, indexName, indexType, comment, false, stmt.getItems());
    AddIndexInfo indexInfo = AddIndexInfo.newBuilder()
            .setName(indexName)
            .setPrimary(false)
            .setUnique(stmt.isUnique())
            .setComment(comment)
            .setType(IndexType.BTree)
            .addAllColumns(columns)
            .setState(MetaState.Absent)
            .setCreateTime(SystemClock.currentTimeMillis())
            .build();

    AlterTableInfo.Builder builder = AlterTableInfo.newBuilder();
    builder.setType(OpType.AddIndex)
            .setItem(indexInfo.toByteString())
            .setDbName(dbName)
            .setTableName(tableName)
            .setState(MetaState.Absent);
    return builder.build();
  }

  private static AlterTableInfo analyzeAlterDropIndex(SQLAlterTableItem stmt, String dbName, String tableName) {
    String indexName = stmt instanceof SQLAlterTableDropIndex ? getName(((SQLAlterTableDropIndex) stmt).getIndexName()) : getName(((SQLAlterTableDropKey) stmt).getKeyName());
    verifyIndexName(indexName, true);

    IndexInfo indexInfo = IndexInfo.newBuilder().setName(indexName).build();
    AlterTableInfo.Builder builder = AlterTableInfo.newBuilder();
    builder.setType(OpType.DropIndex)
            .setItem(indexInfo.toByteString())
            .setDbName(dbName)
            .setTableName(tableName)
            .setState(MetaState.Absent);
    return builder.build();
  }

  private static AlterTableInfo analyzeAlterRenameIndex(SQLAlterTableRenameIndex stmt, String dbName, String tableName) {
    String from = getName(stmt.getName());
    String to = getName(stmt.getTo());

    verifyIndexName(from, false);
    verifyIndexName(to, false);
    if (from.equalsIgnoreCase(to)) {
      throw DBException.get(ErrorModule.DDL, ErrorCode.ER_NOT_SUPPORTED_YET, "schema rename index to the same name");
    }

    IndexInfo indexInfo = IndexInfo.newBuilder()
            .setName(from)
            .setComment(to)
            .setState(MetaState.Absent)
            .build();

    AlterTableInfo.Builder builder = AlterTableInfo.newBuilder();
    builder.setType(OpType.RenameIndex)
            .setItem(indexInfo.toByteString())
            .setDbName(dbName)
            .setTableName(tableName)
            .setState(MetaState.Absent);
    return builder.build();
  }

  private static AlterTableInfo analyzeAlterTableOption(MySqlAlterTableOption stmt, String dbName, String tableName) {
    String optionName = stmt.getName();
    SQLObject optionValue = stmt.getValue();

    if ("AUTO_INCREMENT".equalsIgnoreCase(optionName)) {
      long autoInitId = getAutoInitIdOption(optionValue);
      AlterTableInfo.Builder builder = AlterTableInfo.newBuilder();
      builder.setType(OpType.AlterAutoInitId)
              .setItem(ByteString.copyFrom(String.valueOf(autoInitId).getBytes(StandardCharsets.UTF_8)))
              .setDbName(dbName)
              .setTableName(tableName)
              .setState(MetaState.Absent);
      return builder.build();
    }

    throw DBException.get(ErrorModule.DDL, ErrorCode.ER_NOT_SUPPORTED_YET, "Table Option(" + optionName + ")");
  }

  private static List<String> buildPrimaryConstraint(String dbName, String tableName, List<SQLTableElement> tableElements,
                                                     List<SQLColumnDefinition> colDefines, List<SQLTableElement> tableConstraints) {
    if (tableElements == null || tableElements.isEmpty()) {
      return Collections.EMPTY_LIST;
    }

    List<String> primarys = new ArrayList<>();
    Map<String, SQLColumnDefinition> notNulls = new HashMap<>();
    Map<String, SQLColumnDefinition> columns = new HashMap<>();
    for (SQLTableElement element : tableElements) {
      if (element instanceof SQLColumnDefinition) {
        SQLColumnDefinition colDefine = (SQLColumnDefinition) element;
        String colName = splitColumnName(dbName, tableName, colDefine.getName());
        colDefines.add(colDefine);
        columns.put(colName, colDefine);

        List<SQLColumnConstraint> colConstraints = colDefine.getConstraints();
        if (colConstraints != null && !colConstraints.isEmpty()) {
          for (SQLColumnConstraint constraint : colConstraints) {
            if (constraint instanceof SQLColumnPrimaryKey) {
              primarys.add(colName);
              continue;
            }

            if (constraint instanceof SQLColumnUniqueKey) {
              MySqlUnique unique = new MySqlUnique();
              unique.setIndexType(IndexType.BTree.name());
              unique.addColumn(new SQLIdentifierExpr(colName));
              tableConstraints.add(unique);
              continue;
            }
            if (constraint instanceof SQLNotNullConstraint) {
              notNulls.put(colName.toLowerCase(), colDefine);
              continue;
            }
          }
        }

        continue;
      }

      tableConstraints.add(element);
    }

    if (primarys.isEmpty()) {
      int pos = -1;
      List<SQLSelectOrderByItem> items = null;
      for (int i = 0; i < tableConstraints.size(); i++) {
        SQLTableElement element = tableConstraints.get(i);
        if (element instanceof MySqlPrimaryKey) {
          pos = i;
          items = ((MySqlPrimaryKey) element).getColumns();
          break;
        }

        if (pos > -1) {
          continue;
        }

        if (element instanceof MySqlUnique) {
          boolean skip = true;
          for (SQLSelectOrderByItem column : ((MySqlUnique) element).getColumns()) {
            skip = true;
            String colName = splitColumnName(dbName, tableName, column.getExpr());
            if (notNulls.containsKey(colName.toLowerCase())) {
              skip = false;
            }

            if (skip) {
              break;
            }
          }

          if (!skip) {
            pos = i;
            items = ((MySqlUnique) element).getColumns();
          }
        }
      }

      if (pos > -1) {
        tableConstraints.remove(pos);
        for (SQLSelectOrderByItem item : items) {
          String colName = splitColumnName(dbName, tableName, item.getExpr());
          primarys.add(colName);
        }
      }
    }

    verifyPrimary(columns, tableConstraints, primarys);
    verifyAutoIncr(columns, primarys);

    MySqlPrimaryKey primaryKey = new MySqlPrimaryKey();
    primaryKey.setIndexType(IndexType.BTree.name());
    for (String primary : primarys) {
      primaryKey.addColumn(new SQLIdentifierExpr(primary));
    }
    tableConstraints.add(0, primaryKey);
    return primarys;
  }

  private static void verifyPrimary(Map<String, SQLColumnDefinition> columns, List<SQLTableElement> tableConstraints, List<String> primarys) {
    if (primarys.isEmpty()) {
      throw DBException.get(ErrorModule.DDL, ErrorCode.ER_REQUIRES_PRIMARY_KEY);
    }

    for (String primary : primarys) {
      SQLColumnDefinition column = columns.get(primary);
      if (column.getDefaultExpr() != null) {
        throw DBException.get(ErrorModule.DDL, ErrorCode.ER_INVALID_DEFAULT, primary);
      }

      List<SQLColumnConstraint> constraints = column.getConstraints();
      if (constraints != null && !constraints.isEmpty()) {
        for (SQLColumnConstraint constraint : constraints) {
          if (constraint instanceof SQLNullConstraint) {
            throw DBException.get(ErrorModule.DDL, ErrorCode.ER_PRIMARY_CANT_HAVE_NULL);
          }
        }
      }

      column.addConstraint(new SQLColumnPrimaryKey());
    }

    for (SQLTableElement constraint : tableConstraints) {
      if (constraint instanceof MySqlPrimaryKey) {
        throw DBException.get(ErrorModule.DDL, ErrorCode.ER_MULTIPLE_PRI_KEY);
      }
    }
  }

  private static void verifyAutoIncr(Map<String, SQLColumnDefinition> columns, List<String> primarys) {
    boolean hasAutoIncr = false;
    for (Map.Entry<String, SQLColumnDefinition> entry : columns.entrySet()) {
      if (!entry.getValue().isAutoIncrement()) {
        continue;
      }

      if (hasAutoIncr) {
        throw DBException.get(ErrorModule.DDL, ErrorCode.ER_WRONG_AUTO_KEY);
      }
      hasAutoIncr = true;

      if (entry.getValue().getDefaultExpr() != null) {
        throw DBException.get(ErrorModule.DDL, ErrorCode.ER_INVALID_DEFAULT, entry.getKey());
      }

      boolean isKey = false;
      if (primarys.size() == 1 && primarys.get(0).equalsIgnoreCase(entry.getKey())) {
        isKey = true;
      }
      if (!isKey) {
        throw DBException.get(ErrorModule.DDL, ErrorCode.ER_WRONG_AUTO_KEY);
      }

      if (!"BIGINT".equalsIgnoreCase(entry.getValue().getDataType().getName())) {
        throw DBException.get(ErrorModule.DDL, ErrorCode.ER_WRONG_FIELD_SPEC, entry.getKey());
      }
    }
  }

  private static List<SQLTableElement> verifyConstraint(List<SQLTableElement> constraints) {
    if (constraints != null && constraints.size() > 0) {
      for (SQLTableElement constraint : constraints) {
        if (constraint instanceof MysqlForeignKey) {
          throw DBException.get(ErrorModule.DDL, ErrorCode.ER_NOT_SUPPORTED_YET, "Foreign Key");
        }
        if (!(constraint instanceof MySqlPrimaryKey || constraint instanceof MySqlUnique || constraint instanceof MySqlKey
                || constraint instanceof MySqlTableIndex)) {
          throw DBException.get(ErrorModule.DDL, ErrorCode.ER_NOT_SUPPORTED_YET, "Table Constraint");
        }
      }
    }

    return constraints;
  }

  private static Map<String, ColumnInfo> buildColumns(Session session, String dbName, String tableName,
                                                      List<SQLColumnDefinition> colDefines) {
    int colID = 0;
    int offset = 0;
    Map<String, ColumnInfo> columns = new HashMap<>();

    if (colDefines != null) {
      for (SQLColumnDefinition colDefine : colDefines) {
        ColumnInfo.Builder builder = buildColumn(session, dbName, tableName, colDefine);
        if (columns.containsKey(builder.getName().toLowerCase())) {
          throw DBException.get(ErrorModule.DDL, ErrorCode.ER_DUP_FIELDNAME, builder.getName());
        }

        builder.setId(++colID)
                .setOffset(offset++);
        columns.put(builder.getName().toLowerCase(), builder.build());
      }
    }
    return columns;
  }

  private static ColumnInfo.Builder buildColumn(Session session, String dbName, String tableName, SQLColumnDefinition colDef) {
    // verify name
    String colName = splitColumnName(dbName, tableName, colDef.getName());
    String comment = colDef.getComment() == null ? "" : colDef.getComment().toString();
    if (comment.length() > MAXLEN_COMMENT) {
      throw DBException.get(ErrorModule.DDL, ErrorCode.ER_TOO_LONG_FIELD_COMMENT, tableName, String.valueOf(MAXLEN_COMMENT));
    }

    boolean primary = false;
    Boolean notNull = null;
    boolean autoIncr = colDef.isAutoIncrement();
    List<SQLColumnConstraint> constraints = colDef.getConstraints();
    if (constraints != null && !constraints.isEmpty()) {
      for (SQLColumnConstraint constraint : constraints) {
        if (constraint instanceof SQLColumnPrimaryKey) {
          primary = true;
          continue;
        }
        if (constraint instanceof SQLColumnUniqueKey) {
          continue;
        }
        if (constraint instanceof SQLNotNullConstraint) {
          notNull = Boolean.TRUE;
          continue;
        }
        if (constraint instanceof SQLNullConstraint) {
          notNull = Boolean.FALSE;
          continue;
        }
        if (constraint instanceof SQLColumnReference) {
          throw DBException.get(ErrorModule.DDL, ErrorCode.ER_NOT_SUPPORTED_YET, "column REFERENCES Table(column)");
        }
        throw DBException.get(ErrorModule.DDL, ErrorCode.ER_NOT_SUPPORTED_YET, "column constraints");
      }
    }

    if (primary) {
      notNull = Boolean.TRUE;
    }

    ColumnInfo.Builder builder = analyzeTypeAndDefault(session, colDef, primary, notNull);
    builder.setName(colName)
            .setPrimary(primary)
            .setAutoIncr(autoIncr)
            .setComment(comment)
            .setCreateTime(SystemClock.currentTimeMillis());
    return builder;
  }

  private static ColumnInfo.Builder analyzeTypeAndDefault(Session session, SQLColumnDefinition colDef,
                                                          boolean primary, Boolean notNull) {
    boolean nutNullBool = notNull == null ? false : notNull.booleanValue();
    String colName = colDef.getName().getSimpleName();
    //data type
    SQLType.Builder sqlTypeBuilder = Types.buildSQLType(colDef.getDataType(), colName).toBuilder().setNotNull(nutNullBool);
    //only support auto_increment unsigned
    if (colDef.isAutoIncrement() && !sqlTypeBuilder.getUnsigned()) {
      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_NOT_SUPPORTED_YET, "auto_increment signed");
    }
    //on update
    SQLExpr onUpdateExpr = colDef.getOnUpdate();
    if (onUpdateExpr != null && Types.isTimestampFunc(onUpdateExpr, sqlTypeBuilder.getType(),
            sqlTypeBuilder.getScale(), colName)) {
      sqlTypeBuilder.setOnUpdate(true);
    }
    ColumnInfo.Builder columnBuilder = ColumnInfo.newBuilder().setSqlType(sqlTypeBuilder);
    if (primary) {
      return columnBuilder;
    }
    //default
    Value defaultValue = handleDefaultValue(session, colDef.getDefaultExpr(), sqlTypeBuilder, colName, notNull);
    if (defaultValue != null) {
      Value stringValue = ValueConvertor.convertType(session, defaultValue, SQLType.newBuilder().setType(DataType.Varchar).build());
      columnBuilder.setDefaultValue(ByteString.copyFrom(stringValue.getString().getBytes(StandardCharsets.UTF_8)))
              .setHasDefault(true);
    } else {
      columnBuilder.setHasDefault(false);
    }

    ByteString reorgValue = handleReorgValue(session, sqlTypeBuilder, defaultValue);
    if (reorgValue != null) {
      columnBuilder.setReorgValue(reorgValue);
    }
    return columnBuilder;
  }

  private static Value handleDefaultValue(Session session, SQLExpr defaultExpr, SQLType.Builder sqlTypeBuilder,
                                          String colName, Boolean notNull) {
    Value defaultValue = null;
    DataType dt = sqlTypeBuilder.getType();
    if (defaultExpr != null && !(defaultExpr instanceof SQLNullExpr)) {
      //The BLOB, TEXT, GEOMETRY, and JSON data types cannot be assigned a default value.
      if (dt == DataType.Json || dt == DataType.Text || dt == DataType.TinyBlob
              || dt == DataType.Blob || dt == DataType.MediumBlob || dt == DataType.LongBlob) {
        throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_BLOB_CANT_HAVE_DEFAULT, colName);
      }
      if (Types.isTimestampFunc(defaultExpr, dt, sqlTypeBuilder.getScale(), colName)) {
        sqlTypeBuilder.setOnInit(true);
      } else {
        defaultValue = AnalyzerUtil.resolveValueExpr(defaultExpr, null);
        try {
          defaultValue = ValueConvertor.convertType(session, defaultValue, sqlTypeBuilder.build());
        } catch (Throwable e) {
          throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_INVALID_DEFAULT, e, colName);
        }
      }
    } else {
      if (notNull == null) {
        if (dt == DataType.TimeStamp) {
          throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_INVALID_DEFAULT, colName);
        }
      }
      //no support
//      else if (notNull.booleanValue()) {
//        if (dt == DataType.TimeStamp) {
//          //not null, first colunm timestamp default:CURRENT_TIMESTAMP | on update CURRENT_TIMESTAMP
//          //not null, after first column ,  throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_INVALID_DEFAULT, colName);
//          sqlTypeBuilder.setOnInit(true).setOnUpdate(true);
//        }
//      }
    }
    return defaultValue;
  }

  private static ByteString handleReorgValue(Session session, SQLType.Builder sqlTypeBuilder, Value defaultValue) {
    if (!sqlTypeBuilder.getNotNull() && defaultValue == null) {
      return null;
    }

    Value reorgValue = defaultValue;
    SQLType sqlType = sqlTypeBuilder.build();

    if (reorgValue == null || reorgValue.isNull()) {
      if (sqlType.getOnInit() && sqlType.getOnUpdate()) {
        reorgValue = DateValue.getNow(sqlType.getType(), sqlType.getScale(), session.getStmtContext().getLocalTimeZone());
      } else {
        if (Types.isNumberType(sqlType) || Types.isYear(sqlType)) {
          reorgValue = LongValue.getInstance(0);
        } else if (Types.isDateType(sqlType)) {
          reorgValue = DateValue.getInstance("0", sqlType.getType(), 0, null);
        } else if (Types.isVarCharType(sqlType)) {
          reorgValue = StringValue.getInstance("");
        }
      }
    }

    if (reorgValue == null || reorgValue.isNull()) {
      return null;
    }

    reorgValue = ValueConvertor.convertType(session, reorgValue, sqlType);
    ByteBuf buf = Codec.allocBuffer(20);
    ValueCodec.encodeValue(buf, reorgValue, 0);
    return NettyByteString.wrap(buf);
  }

  private static List<IndexInfo> buildIndex(String dbName, String tableName, List<SQLTableElement> constraints, Map<String, ColumnInfo> tableCols) {
    if (constraints == null || constraints.isEmpty()) {
      return Collections.EMPTY_LIST;
    }

    String indexName = "";
    String comment = "";
    String indexType = "";
    List<SQLSelectOrderByItem> indexCols = null;
    List<IndexInfo> indexInfos = new ArrayList<>(constraints.size());

    for (SQLTableElement element : constraints) {
      boolean isPrimary = false;
      boolean isUnique = false;
      if (element instanceof MySqlPrimaryKey) {
        MySqlPrimaryKey primaryKey = (MySqlPrimaryKey) element;
        indexName = getName(primaryKey.getName());
        comment = primaryKey.getComment() == null ? "" : primaryKey.getComment().toString();
        indexType = primaryKey.getIndexType();
        indexCols = primaryKey.getColumns();
        isPrimary = true;
      } else if (element instanceof MySqlUnique) {
        MySqlUnique unique = (MySqlUnique) element;
        indexName = getName(unique.getName());
        comment = unique.getComment() == null ? "" : unique.getComment().toString();
        indexType = unique.getIndexType();
        indexCols = unique.getColumns();
        isUnique = true;
      } else if (element instanceof MySqlKey) {
        MySqlKey key = (MySqlKey) element;
        indexName = getName(key.getName());
        comment = key.getComment() == null ? "" : key.getComment().toString();
        indexType = key.getIndexType();
        indexCols = key.getColumns();
      } else if (element instanceof MySqlTableIndex) {
        MySqlTableIndex index = (MySqlTableIndex) element;
        indexName = getName(index.getName());
        indexType = index.getIndexType();
        indexCols = index.getColumns();
      }

      List<String> columns = verifyIndex(dbName, tableName, indexName, indexType, comment, isPrimary, indexCols);
      IndexInfo.Builder builder = IndexInfo.newBuilder();
      builder.setName(indexName)
              .setUnique(isUnique)
              .setPrimary(isPrimary)
              .setType(IndexType.BTree)
              .setComment(comment);
      indexInfos.add(DDLUtils.buildIndexInfo(builder.build(), columns, tableCols, indexInfos));
    }

    return indexInfos;
  }

  private static List<String> verifyIndex(String dbName, String tableName, String indexName, String indexType, String comment,
                                          boolean isPrimary, List<SQLSelectOrderByItem> indexCols) {
    if (StringUtils.isNotBlank(indexType) && !IndexType.BTree.name().equalsIgnoreCase(indexType)) {
      throw DBException.get(ErrorModule.DDL, ErrorCode.ER_NOT_SUPPORTED_YET, indexType + " Index");
    }

    verifyIndexName(indexName, isPrimary);
    if (comment.length() > MAXLEN_COMMENT) {
      throw DBException.get(ErrorModule.DDL, ErrorCode.ER_TOO_LONG_INDEX_COMMENT, indexName, String.valueOf(MAXLEN_COMMENT));
    }

    if (indexCols.size() > MAX_KEY_COLUMNS) {
      throw DBException.get(ErrorModule.DDL, ErrorCode.ER_TOO_MANY_KEY_PARTS, String.valueOf(MAX_KEY_COLUMNS));
    }

    List<String> colNames = new ArrayList<>(indexCols.size());
    for (SQLSelectOrderByItem column : indexCols) {
      SQLExpr colNameExpr = column.getExpr();
      if (colNameExpr instanceof SQLMethodInvokeExpr) {
        throw DBException.get(ErrorModule.DDL, ErrorCode.ER_NOT_SUPPORTED_YET, "Prefixes Index");
      }

      String colName = splitColumnName(dbName, tableName, column.getExpr()).toLowerCase();
      if (colNames.contains(colName)) {
        throw DBException.get(ErrorModule.DDL, ErrorCode.ER_DUP_FIELDNAME, colName);
      }
      colNames.add(colName);
    }
    return colNames;
  }

  private static void buildSplitKey(TableInfo.Builder builder, SQLPartitionBy partitionBy, ColumnInfo primaryKey) {
    if (partitionBy == null) {
      return;
    }

    List<SQLExpr> partitionCols = partitionBy.getColumns();
    if (!(partitionBy instanceof SQLPartitionByRange)) {
      throw DBException.get(ErrorModule.DDL, ErrorCode.ER_NOT_SUPPORTED_YET, "Non-Range Partition Type");
    }
    if (partitionCols != null && (partitionCols.size() > 1 || !getName((SQLName) partitionCols.get(0)).equalsIgnoreCase(primaryKey.getName()))) {
      throw DBException.get(ErrorModule.DDL, ErrorCode.ER_META_UNSUPPORTED_PARTITION);
    }

    int splitNum = DEFAULT_RANGE_NUM;
    if (partitionBy.getPartitionsCount() instanceof SQLNumericLiteralExpr) {
      splitNum = ((SQLNumericLiteralExpr) partitionBy.getPartitionsCount()).getNumber().intValue();
    }
    builder.setSplitNum(splitNum);

    List<SQLPartition> partitions = partitionBy.getPartitions();
    if (partitions != null && partitions.size() > 0) {
      List<String> splitKeys = new ArrayList<>(splitNum);
      for (SQLPartition partition : partitions) {
        SQLPartitionValue value = partition.getValues();
        List<SQLExpr> valueItems = value.getItems();
        if (value.getOperator() != SQLPartitionValue.Operator.LessThan || valueItems.size() > 1) {
          throw DBException.get(ErrorModule.DDL, ErrorCode.ER_META_UNSUPPORTED_PARTITION_VALUE);
        }

        String key = getSplitKey(primaryKey.getSqlType().getType(), valueItems.get(0));
        if (StringUtils.isNotBlank(key)) {
          splitKeys.add(key);
        }
      }

      builder.addAllSplitKeys(splitKeys);
    }
  }

  private static String getSplitKey(DataType type, SQLExpr expr) {
    String value = "";
    switch (type) {
      case TinyInt:
      case SmallInt:
      case MediumInt:
      case Int:
      case BigInt:
        if (!(expr instanceof SQLIntegerExpr)) {
          if ("MAXVALUE".equalsIgnoreCase(DDLUtils.trimName(expr.toString()))) {
            return null;
          }
          throw DBException.get(ErrorModule.DDL, ErrorCode.ER_WRONG_TYPE_COLUMN_VALUE_ERROR);
        }

        value = ((SQLIntegerExpr) expr).getNumber().toString();
        break;

      case Float:
      case Double:
      case Decimal:
        if (!(expr instanceof SQLNumberExpr || expr instanceof SQLIntegerExpr)) {
          if ("MAXVALUE".equalsIgnoreCase(DDLUtils.trimName(expr.toString()))) {
            return null;
          }
          throw DBException.get(ErrorModule.DDL, ErrorCode.ER_WRONG_TYPE_COLUMN_VALUE_ERROR);
        }

        value = ((SQLNumericLiteralExpr) expr).getNumber().toString();
        break;

      default:
        if (!(expr instanceof SQLCharExpr)) {
          if ("MAXVALUE".equalsIgnoreCase(DDLUtils.trimName(expr.toString()))) {
            return null;
          }
          throw DBException.get(ErrorModule.DDL, ErrorCode.ER_WRONG_TYPE_COLUMN_VALUE_ERROR);
        }

        value = ((SQLCharExpr) expr).getValue().toString();
    }
    return DDLUtils.trimName(value);
  }

  private static void analyzeTableOptions(TableInfo.Builder builder, MySqlCreateTableStatement stmt) {
    builder.setType(StoreType.Store_Hot);
    builder.setAutoInitId(1);
    Map<String, SQLObject> options = stmt.getTableOptions();
    if (options != null) {
      for (Map.Entry<String, SQLObject> option : options.entrySet()) {
        if ("ENGINE".equalsIgnoreCase(DDLUtils.trimName(option.getKey()))) {
          StoreType type = getEngineOption(DDLUtils.trimName(option.getValue().toString()));
          builder.setType(type);
          continue;
        }

        if ("AUTO_INCREMENT".equalsIgnoreCase(DDLUtils.trimName(option.getKey()))) {
          long autoInitId = getAutoInitIdOption(option.getValue());
          builder.setAutoInitId(autoInitId);
          continue;
        }

        throw DBException.get(ErrorModule.DDL, ErrorCode.ER_NOT_SUPPORTED_YET, "Table Option(" + option.getKey() + ")");
      }
    }
  }

  private static StoreType getEngineOption(String engineName) {
    String engine = engineName.toUpperCase();
    StoreType type;
    switch (engine) {
      case "MEMORY":
        type = StoreType.Store_Hot;
        break;
      case "MIX":
        type = StoreType.Store_Mix;
        break;
      case "DISK":
        type = StoreType.Store_Warm;
        break;
      default:
        throw DBException.get(ErrorModule.DDL, ErrorCode.ER_NOT_SUPPORTED_YET, "Table ENGINE(" + engineName + ")");
    }
    return type;
  }

  private static long getAutoInitIdOption(SQLObject value) {
    if (!(value instanceof SQLIntegerExpr)) {
      throw DBException.get(ErrorModule.DDL, ErrorCode.ER_WRONG_VALUE, "AUTO_INCREMENT");
    }
    long autoInitId = ((SQLIntegerExpr) value).getNumber().longValue();
    if (autoInitId < 0) {
      throw DBException.get(ErrorModule.DDL, ErrorCode.ER_WRONG_VALUE, "AUTO_INCREMENT=" + autoInitId);
    }
    return autoInitId;
  }

  private static int analyzeReplica(MySqlCreateTableStatement stmt) {
    int replica = 3;
    if (stmt.getComment() != null) {
      String[] comments = stmt.getComment().toString().split(";");
      for (String comment : comments) {
        String[] strs = comment.split("=");
        String key = DDLUtils.trimName(strs[0]);
        if (strs.length != 2 || !"REPLICA".equalsIgnoreCase(key)) {
          continue;
        }

        String value = DDLUtils.trimName(strs[1]);
        replica = Integer.parseInt(value);
        if ((replica & 1) != 1) {
          throw DBException.get(ErrorModule.DDL, ErrorCode.ER_META_INVALID_REPLICA_NUMBER, comment);
        }
        break;
      }
    }
    return replica;
  }

  /**
   * Verify that the identifier is legal.
   * Permitted characters: [0-9,a-z,A-Z$_] (but may not consist solely of digits)
   *
   * @param identifier
   * @return
   * @See https://dev.mysql.com/doc/refman/5.7/en/identifiers.html
   */
  private static boolean verifyIdentifier(String identifier) {
    if (StringUtils.isBlank(identifier)) {
      return false;
    }

    char c;
    for (int i = 0; i < identifier.length(); i++) {
      c = identifier.charAt(i);
      if (!(c == '$' || c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9'))) {
        return false;
      }
    }

    for (String r : RESERVES) {
      if (r.equalsIgnoreCase(identifier)) {
        return false;
      }
    }

    return true;
  }

  private static Tuple2<String, String> getAndVerifyTableName(SQLExpr expr, boolean reserve) {
    // verify name
    Tuple2<String, String> names = DDLUtils.splitTableName(expr);
    verifyDatabaseName(names.getT1(), reserve);
    verifyTableName(names.getT2());
    return names;
  }

  private static void verifyDatabaseName(String name, boolean reserve) {
    if (StringUtils.isBlank(name)) {
      throw DBException.get(ErrorModule.DDL, ErrorCode.ER_NO_DB_ERROR);
    }
    if (!verifyIdentifier(name)) {
      throw DBException.get(ErrorModule.DDL, ErrorCode.ER_WRONG_DB_NAME, name);
    }
    if (name.length() > MAXLEN_DBNAME) {
      throw DBException.get(ErrorModule.DDL, ErrorCode.ER_TOO_LONG_IDENT, name);
    }
    if (reserve && SYSTEM_DB.equalsIgnoreCase(name)) {
      throw DBException.get(ErrorModule.DDL, ErrorCode.ER_NOT_SUPPORTED_YET, "operate database " + name);
    }
  }

  private static void verifyTableName(String name) {
    if (!verifyIdentifier(name)) {
      throw DBException.get(ErrorModule.DDL, ErrorCode.ER_WRONG_TABLE_NAME, name);
    }
    if (name.length() > MAXLEN_TABLENAME) {
      throw DBException.get(ErrorModule.DDL, ErrorCode.ER_TOO_LONG_IDENT, name);
    }
  }

  private static void verifyIndexName(String name, boolean isPrimary) {
    if (StringUtils.isNotBlank(name)) {
      if (!isPrimary && Types.PRIMARY_KEY_NAME.equalsIgnoreCase(name)) {
        throw DBException.get(ErrorModule.DDL, ErrorCode.ER_WRONG_NAME_FOR_INDEX, name);
      }
      if (!verifyIdentifier(name)) {
        throw DBException.get(ErrorModule.DDL, ErrorCode.ER_WRONG_NAME_FOR_INDEX, name);
      }
      if (name.length() > MAXLEN_INDEXNAME) {
        throw DBException.get(ErrorModule.DDL, ErrorCode.ER_TOO_LONG_IDENT, name);
      }
    }
  }

  private static String splitColumnName(String dbName, String tableName, SQLExpr expr) {
    String names = "";
    if (expr instanceof SQLMethodInvokeExpr) {
      names = ((SQLMethodInvokeExpr) expr).getMethodName();
    } else {
      names = expr.toString();
    }
    return splitColumnName(dbName, tableName, names);
  }

  private static String splitColumnName(String dbName, String tableName, String colName) {
    colName = DDLUtils.trimName(colName);
    int pos = colName.lastIndexOf('.');
    String name = pos > 0 ? DDLUtils.trimName(colName.substring(pos + 1)) : colName;
    colName = pos > 0 ? DDLUtils.trimName(colName.substring(0, pos)) : "";
    pos = colName.lastIndexOf('.');
    String colDBName = pos > 0 ? DDLUtils.trimName(colName.substring(0, pos)) : "";
    String colTableName = pos > 0 ? DDLUtils.trimName(colName.substring(pos + 1)) : colName;

    if (StringUtils.isNotBlank(colDBName) && !colDBName.equalsIgnoreCase(dbName)) {
      throw DBException.get(ErrorModule.DDL, ErrorCode.ER_WRONG_DB_NAME, colDBName);
    }
    if (StringUtils.isNotBlank(colTableName) && !colTableName.equalsIgnoreCase(tableName)) {
      throw DBException.get(ErrorModule.DDL, ErrorCode.ER_WRONG_TABLE_NAME, colTableName);
    }
    if (!verifyIdentifier(name)) {
      throw DBException.get(ErrorModule.DDL, ErrorCode.ER_WRONG_COLUMN_NAME, name);
    }
    if (name.length() > MAXLEN_COLUMNNAME) {
      throw DBException.get(ErrorModule.DDL, ErrorCode.ER_TOO_LONG_IDENT, name);
    }

    return name;
  }

  private static String getName(SQLName name) {
    if (name == null) {
      return "";
    }

    return DDLUtils.trimName(name.toString());
  }
}
