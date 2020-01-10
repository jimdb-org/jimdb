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
package io.jimdb.sql.ddl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.jimdb.common.exception.ErrorCode;
import io.jimdb.pb.Ddlpb.AddColumnInfo;
import io.jimdb.pb.Metapb;
import io.jimdb.pb.Metapb.ColumnInfo;
import io.jimdb.pb.Metapb.MetaState;
import io.jimdb.pb.Metapb.TableInfo;
import io.jimdb.core.plugin.MetaStore;
import io.jimdb.common.utils.os.SystemClock;

import org.apache.commons.lang3.StringUtils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings("OCP_OVERLY_CONCRETE_PARAMETER")
final class TaskColumnHandler {
  private TaskColumnHandler() {
  }

  static AddColumnInfo addColumn(MetaStore metaStore, TableInfo tableInfo, AddColumnInfo addColumn) {
    int pos = 0;
    ColumnInfo columnInfo = null;
    for (ColumnInfo column : tableInfo.getColumnsList()) {
      if (column.getName().equalsIgnoreCase(addColumn.getName())) {
        if (column.getState() == MetaState.Public) {
          if (addColumn.getState() == MetaState.Absent) {
            throw new DDLException(DDLException.ErrorType.FAILED, ErrorCode.ER_DUP_FIELDNAME, addColumn.getName());
          }
          return addColumn.toBuilder().setState(MetaState.Public).build();
        }

        columnInfo = column;
        break;
      }
      ++pos;
    }

    TableInfo.Builder tableBuilder = tableInfo.toBuilder();
    if (columnInfo == null) {
      columnInfo = buildColumn(tableBuilder, addColumn);
      pos = tableBuilder.getColumnsCount() - 1;
    }

    switch (columnInfo.getState()) {
      case Absent:
        columnInfo = columnInfo.toBuilder()
                .setState(MetaState.DeleteOnly)
                .build();
        tableBuilder.setColumns(pos, columnInfo);
        if (!metaStore.storeTable(tableInfo, tableBuilder.build())) {
          throw new DDLException(DDLException.ErrorType.CONCURRENT, String.format("Table[%d] occur concurrent process error", tableInfo.getId()));
        }

        return addColumn.toBuilder()
                .setState(MetaState.DeleteOnly)
                .build();

      case DeleteOnly:
        columnInfo = columnInfo.toBuilder()
                .setState(MetaState.WriteOnly)
                .build();
        tableBuilder.setColumns(pos, columnInfo);
        if (!metaStore.storeTable(tableInfo, tableBuilder.build())) {
          throw new DDLException(DDLException.ErrorType.CONCURRENT, String.format("Table[%d] occur concurrent process error", tableInfo.getId()));
        }

        return addColumn.toBuilder()
                .setState(MetaState.WriteOnly)
                .build();

      case WriteOnly:
        columnInfo = columnInfo.toBuilder()
                .setState(MetaState.WriteReorg)
                .build();
        tableBuilder.setColumns(pos, columnInfo);
        if (!metaStore.storeTable(tableInfo, tableBuilder.build())) {
          throw new DDLException(DDLException.ErrorType.CONCURRENT, String.format("Table[%d] occur concurrent process error", tableInfo.getId()));
        }

        return addColumn.toBuilder()
                .setState(MetaState.WriteReorg)
                .build();

      case WriteReorg:
        resetOffset(tableBuilder, addColumn, pos);
        if (!metaStore.storeTable(tableInfo, tableBuilder.build())) {
          throw new DDLException(DDLException.ErrorType.CONCURRENT, String.format("Table[%d] occur concurrent process error", tableInfo.getId()));
        }

        return addColumn.toBuilder()
                .setState(MetaState.Public)
                .build();

      case Public:
        return addColumn.toBuilder()
                .setState(MetaState.Public)
                .build();

      default:
        throw new DDLException(DDLException.ErrorType.FAILED, "invalid table state: " + columnInfo.getState().name());
    }
  }

  static ColumnInfo dropColumn(MetaStore metaStore, TableInfo tableInfo, ColumnInfo dropColumn) {
    int pos = 0;
    ColumnInfo columnInfo = null;
    for (ColumnInfo column : tableInfo.getColumnsList()) {
      if (column.getName().equalsIgnoreCase(dropColumn.getName())) {
        if (dropColumn.getId() == 0 || dropColumn.getId() == column.getId()) {
          columnInfo = column;
          break;
        }
      }
      ++pos;
    }

    if (columnInfo == null) {
      if (dropColumn.getId() == 0) {
        throw new DDLException(DDLException.ErrorType.FAILED, ErrorCode.ER_BAD_FIELD_ERROR, dropColumn.getName(), tableInfo.getName());
      }
      return dropColumn.toBuilder().setState(MetaState.Absent).build();
    }
    TableInfo.Builder tableBuilder = tableInfo.toBuilder();

    switch (columnInfo.getState()) {
      case Public:
        verifyDropColumn(tableInfo, columnInfo);
        resetDropOffset(tableBuilder, columnInfo, pos);
        pos = tableBuilder.getColumnsCount() - 1;

        columnInfo = columnInfo.toBuilder()
                .setState(MetaState.WriteOnly)
                .build();
        tableBuilder.setColumns(pos, columnInfo);
        if (!metaStore.storeTable(tableInfo, tableBuilder.build())) {
          throw new DDLException(DDLException.ErrorType.CONCURRENT, String.format("Table[%d] occur concurrent process error", tableInfo.getId()));
        }

        return dropColumn.toBuilder()
                .setId(columnInfo.getId())
                .setState(MetaState.WriteOnly)
                .build();

      case WriteOnly:
        columnInfo = columnInfo.toBuilder()
                .setState(MetaState.DeleteOnly)
                .build();
        tableBuilder.setColumns(pos, columnInfo);
        if (!metaStore.storeTable(tableInfo, tableBuilder.build())) {
          throw new DDLException(DDLException.ErrorType.CONCURRENT, String.format("Table[%d] occur concurrent process error", tableInfo.getId()));
        }

        return dropColumn.toBuilder()
                .setId(columnInfo.getId())
                .setState(MetaState.DeleteOnly)
                .build();

      case DeleteOnly:
        columnInfo = columnInfo.toBuilder()
                .setState(MetaState.DeleteReorg)
                .build();
        tableBuilder.setColumns(pos, columnInfo);
        if (!metaStore.storeTable(tableInfo, tableBuilder.build())) {
          throw new DDLException(DDLException.ErrorType.CONCURRENT, String.format("Table[%d] occur concurrent process error", tableInfo.getId()));
        }

        return dropColumn.toBuilder()
                .setId(columnInfo.getId())
                .setState(MetaState.DeleteReorg)
                .build();

      case DeleteReorg:
        tableBuilder.removeColumns(pos);
        if (!metaStore.storeTable(tableInfo, tableBuilder.build())) {
          throw new DDLException(DDLException.ErrorType.CONCURRENT, String.format("Table[%d] occur concurrent process error", tableInfo.getId()));
        }

        return dropColumn.toBuilder()
                .setId(columnInfo.getId())
                .setState(MetaState.Absent)
                .build();

      default:
        throw new DDLException(DDLException.ErrorType.FAILED, "invalid table state: " + columnInfo.getState().name());
    }
  }

  private static ColumnInfo buildColumn(TableInfo.Builder tableBuilder, AddColumnInfo addColumnInfo) {
    String afterName = addColumnInfo.getAfterName();
    List<ColumnInfo> columns = tableBuilder.getColumnsList();
    if (StringUtils.isNotBlank(afterName)) {
      boolean foundAfter = false;
      for (ColumnInfo column : columns) {
        if (column.getState() == MetaState.Public && column.getName().equalsIgnoreCase(afterName)) {
          foundAfter = true;
          break;
        }
      }
      if (!foundAfter) {
        throw new DDLException(DDLException.ErrorType.FAILED, ErrorCode.ER_BAD_FIELD_ERROR, afterName, tableBuilder.getName());
      }
    }

    int colID = tableBuilder.getMaxColumnId() + 1;
    ColumnInfo columnInfo = ColumnInfo.newBuilder()
            .setId(colID)
            .setName(addColumnInfo.getName())
            .setSqlType(addColumnInfo.getSqlType())
            .setOffset(columns.size())
            .setDefaultValue(addColumnInfo.getDefaultValue())
            .setHasDefault(addColumnInfo.getHasDefault())
            .setReorgValue(addColumnInfo.getReorgValue())
            .setComment(addColumnInfo.getComment())
            .setCreateTime(SystemClock.currentTimeMillis())
            .setPrimary(false)
            .setAutoIncr(false)
            .setState(MetaState.Absent)
            .build();

    tableBuilder.setMaxColumnId(colID)
            .addColumns(columnInfo);
    return columnInfo;
  }

  private static void resetOffset(TableInfo.Builder tableBuilder, AddColumnInfo addColumnInfo, int pos) {
    ColumnInfo newColumn = tableBuilder.getColumns(pos);
    newColumn = newColumn.toBuilder().setState(MetaState.Public).build();
    tableBuilder.removeColumns(pos);
    List<ColumnInfo> columns = new ArrayList<>(tableBuilder.getColumnsList());
    Collections.sort(columns, (c1, c2) -> {
      if (c1.getOffset() == c2.getOffset()) {
        return 0;
      }
      return c1.getOffset() > c2.getOffset() ? 1 : -1;
    });

    String afterName = addColumnInfo.getAfterName();
    if (addColumnInfo.getFirst()) {
      columns.add(0, newColumn);
    } else if (StringUtils.isNotBlank(afterName)) {
      for (int i = 0; i < columns.size(); i++) {
        if (afterName.equalsIgnoreCase(columns.get(i).getName())) {
          columns.add(i + 1, newColumn);
          break;
        }
      }
    } else {
      columns.add(newColumn);
    }

    int offset = 0;
    List<ColumnInfo> newColumnList = new ArrayList<>(columns.size());
    for (ColumnInfo column : columns) {
      column = column.toBuilder().setOffset(offset++).build();
      newColumnList.add(column);
    }
    tableBuilder.clearColumns().addAllColumns(newColumnList);
  }

  private static void verifyDropColumn(TableInfo tableInfo, ColumnInfo columnInfo) {
    if (columnInfo.getPrimary()) {
      throw new DDLException(DDLException.ErrorType.FAILED, ErrorCode.ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_NOPK);
    }
    if (tableInfo.getColumnsCount() <= 1) {
      throw new DDLException(DDLException.ErrorType.FAILED, ErrorCode.ER_CANT_REMOVE_ALL_FIELDS);
    }

    for (Metapb.IndexInfo indexInfo : tableInfo.getIndicesList()) {
      if (indexInfo.getColumnsList().contains(columnInfo.getId())) {
        throw new DDLException(DDLException.ErrorType.FAILED, ErrorCode.ER_FK_COLUMN_CANNOT_DROP, columnInfo.getName());
      }
    }
  }

  private static void resetDropOffset(TableInfo.Builder tableBuilder, ColumnInfo columnInfo, int pos) {
    tableBuilder.removeColumns(pos);
    List<ColumnInfo> columns = new ArrayList<>(tableBuilder.getColumnsList());
    Collections.sort(columns, (c1, c2) -> {
      if (c1.getOffset() == c2.getOffset()) {
        return 0;
      }
      return c1.getOffset() > c2.getOffset() ? 1 : -1;
    });

    int offset = 0;
    columns.add(columnInfo);
    List<ColumnInfo> newColumnList = new ArrayList<>(columns.size());
    for (ColumnInfo column : columns) {
      column = column.toBuilder().setOffset(offset++).build();
      newColumnList.add(column);
    }
    tableBuilder.clearColumns().addAllColumns(newColumnList);
  }
}
