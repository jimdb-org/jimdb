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
package io.jimdb.core.model.meta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.pb.Basepb.StoreType;
import io.jimdb.pb.Metapb.ColumnInfo;
import io.jimdb.pb.Metapb.IndexInfo;
import io.jimdb.pb.Metapb.MetaState;
import io.jimdb.pb.Metapb.TableInfo;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings({ "FCCD_FIND_CLASS_CIRCULAR_DEPENDENCY", "EI_EXPOSE_REP" })
public final class Table {
  private final Integer id;
  private final TableInfo tableInfo;
  private final Catalog catalog;

  private final Column[] primarys;
  private final Column[] readableColumns;
  private final Column[] writableColumns;

  private Index primaryIndex;
  private final Index[] readableIndices;
  private final Index[] writableIndices;
  private final Index[] deletableIndices;

  private final Map<String, Column> readableColumnMap;
  private final Map<String, Column> writableColumnMap;

  public Table(Catalog catalog, TableInfo tableInfo) {
    List<ColumnInfo> tableColumns = tableInfo.getColumnsList() == null ? Collections.EMPTY_LIST : tableInfo.getColumnsList();
    List<Integer> tablePrimarys = tableInfo.getPrimarysList() == null ? Collections.EMPTY_LIST : tableInfo.getPrimarysList();
    List<IndexInfo> tableIndex = tableInfo.getIndicesList() == null ? Collections.EMPTY_LIST : tableInfo.getIndicesList();

    this.id = tableInfo.getId();
    this.tableInfo = tableInfo;
    this.catalog = catalog;
    this.primarys = new Column[tablePrimarys.size()];
    this.readableColumnMap = tableColumns.size() == 0 ? Collections.EMPTY_MAP : new HashMap<>(tableColumns.size(), 1.0F);
    this.writableColumnMap = tableColumns.size() == 0 ? Collections.EMPTY_MAP : new HashMap<>(tableColumns.size(), 1.0F);

    int readOffset = 0;
    int writeOffset = 0;
    for (ColumnInfo columnInfo : tableColumns) {
      MetaState state = columnInfo.getState();
      if (state == MetaState.Absent || state == MetaState.DeleteOnly || state == MetaState.DeleteReorg) {
        continue;
      }

      if (state == MetaState.Public) {
        this.readableColumnMap.put(columnInfo.getName().toLowerCase(), new Column(this, columnInfo, readOffset++));
      }
      this.writableColumnMap.put(columnInfo.getName().toLowerCase(), new Column(this, columnInfo, writeOffset++));
    }
    List<Column> readableList = new ArrayList<>(this.readableColumnMap.values());
    List<Column> writableList = new ArrayList<>(this.writableColumnMap.values());
    Collections.sort(readableList, (c1, c2) -> {
      if (c1.getOffset() == c2.getOffset()) {
        return 0;
      }
      return c1.getOffset() > c2.getOffset() ? 1 : -1;
    });
    Collections.sort(writableList, (c1, c2) -> {
      if (c1.getOffset() == c2.getOffset()) {
        return 0;
      }
      return c1.getOffset() > c2.getOffset() ? 1 : -1;
    });
    this.readableColumns = readableList.toArray(new Column[0]);
    this.writableColumns = writableList.toArray(new Column[0]);

    for (int i = 0; i < tablePrimarys.size(); i++) {
      int colId = tablePrimarys.get(i);
      for (Column column : this.readableColumns) {
        if (column.getId() == colId) {
          this.primarys[i] = column;
          break;
        }
      }
    }

    Map<Integer, Index> readableIndexMap = tableIndex.isEmpty() ? Collections.EMPTY_MAP : new HashMap<>(tableIndex.size(), 1.0F);
    Map<Integer, Index> writableIndexMap = tableIndex.isEmpty() ? Collections.EMPTY_MAP : new HashMap<>(tableIndex.size(), 1.0F);
    Map<Integer, Index> deletableIndexMap = tableIndex.isEmpty() ? Collections.EMPTY_MAP : new HashMap<>(tableIndex.size(), 1.0F);
    for (IndexInfo indexInfo : tableIndex) {
      MetaState state = indexInfo.getState();
      if (state == MetaState.Absent) {
        continue;
      }

      Index index = new Index(this, indexInfo);
      if (index.isPrimary()) {
        primaryIndex = index;
      }
      if (state == MetaState.Public) {
        readableIndexMap.put(indexInfo.getId(), index);
      }
      if (state != MetaState.DeleteOnly && state != MetaState.DeleteReorg) {
        writableIndexMap.put(indexInfo.getId(), index);
      }
      deletableIndexMap.put(indexInfo.getId(), index);
    }
    List<Index> readIndexList = new ArrayList<>(readableIndexMap.values());
    List<Index> writeIndexList = new ArrayList<>(writableIndexMap.values());
    List<Index> deleteIndexList = new ArrayList<>(deletableIndexMap.values());
    this.sortIndex(readIndexList);
    this.sortIndex(writeIndexList);
    this.sortIndex(deleteIndexList);
    this.readableIndices = readIndexList.toArray(new Index[0]);
    this.writableIndices = writeIndexList.toArray(new Index[0]);
    this.deletableIndices = deleteIndexList.toArray(new Index[0]);
  }

  private void sortIndex(List<Index> indices) {
    Collections.sort(indices, (c1, c2) -> {
      if (c1.isPrimary()) {
        return -1;
      }
      if (c2.isPrimary()) {
        return 1;
      }

      if (c1.isUnique() && !c2.isUnique()) {
        return -1;
      }
      if (!c1.isUnique() && c2.isUnique()) {
        return 1;
      }

      return c1.getId().intValue() >= c2.getId().intValue() ? 1 : -1;
    });
  }

  public Integer getId() {
    return id;
  }

  public Integer getCatalogId() {
    return catalog.getId();
  }

  public Catalog getCatalog() {
    return catalog;
  }

  public String getName() {
    return tableInfo.getName();
  }

  public StoreType getEngine() {
    return tableInfo.getType();
  }

  public String getComment() {
    return tableInfo.getComment();
  }

  public Column[] getPrimary() {
    return primarys;
  }

  public Index getPrimaryIndex() {
    return primaryIndex;
  }

  public Column[] getReadableColumns() {
    return readableColumns;
  }

  public Column[] getWritableColumns() {
    return writableColumns;
  }

  public Index[] getReadableIndices() {
    return readableIndices;
  }

  public Index[] getWritableIndices() {
    return writableIndices;
  }

  public Index[] getDeletableIndices() {
    return deletableIndices;
  }

  public Column getReadableColumn(String name) {
    Column column = readableColumnMap.get(name.toLowerCase());
    if (column == null) {
      throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_BAD_FIELD_ERROR, name, tableInfo.getName());
    }
    return column;
  }

  public Column getWritableColumn(String name) {
    Column column = writableColumnMap.get(name.toLowerCase());
    if (column == null) {
      throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_BAD_FIELD_ERROR, name, tableInfo.getName());
    }
    return column;
  }

  public static Table getStatsTable() {
    return null;
//    Table statsTable = new Table();
//    statsTable.setId(904351);
//    statsTable.setName("statsTable");
//    statsTable.setDbId(904306);
//    statsTable.setDbName("stats");
//    JimColumn[] columns = new JimColumn[14];
//
//    JimColumn idColumn = new JimColumn(1L, "id", DataType.BigInt, 0);
//    //idColumn.setPrimaryKey(1);
//    columns[0] = idColumn;
//
//    columns[1] = new JimColumn(2L, "isIndex", DataType.Binary, 1);
//    columns[2] = new JimColumn(3L, "rowCount", DataType.BigInt, 2);
//    columns[3] = new JimColumn(4L, "hId", DataType.BigInt, 3);
//    columns[4] = new JimColumn(5L, "hNdv", DataType.BigInt, 4);
//    columns[5] = new JimColumn(6L, "hNullCount", DataType.BigInt, 5);
//    columns[6] = new JimColumn(7L, "hTotalCount", DataType.Double, 6);
//    columns[7] = new JimColumn(8L, "hBuckets", DataType.Varchar, 7);
//    columns[8] = new JimColumn(9L, "cRowCount", DataType.Varchar, 8);
//    columns[9] = new JimColumn(10L, "cColumnCount", DataType.Varchar, 9);
//    columns[10] = new JimColumn(11L, "cHashTables", DataType.Varchar, 10);
//    columns[11] = new JimColumn(12L, "cDefaultValue", DataType.BigInt, 11);
//    columns[12] = new JimColumn(13L, "cNdv", DataType.BigInt, 12);
//    columns[13] = new JimColumn(14L, "cTotalCount", DataType.BigInt, 13);
//
//    statsTable.setColumns(columns);
//    return statsTable;
  }
}
