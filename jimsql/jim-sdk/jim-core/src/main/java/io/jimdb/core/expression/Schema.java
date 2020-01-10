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
package io.jimdb.core.expression;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import io.jimdb.core.Session;
import io.jimdb.core.model.meta.Column;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings({ "FCCD_FIND_CLASS_CIRCULAR_DEPENDENCY", "LII_LIST_INDEXED_ITERATING" })
public final class Schema implements Cloneable {
  private final List<ColumnExpr> columns;
  private List<KeyColumn> keyColumns;

  // TODO We need to think about if there is anyway to make this map globally to all schemas
//  private Map<Long, Integer> uidToIndexMap;

  public Schema(List<ColumnExpr> columns) {
    int size = columns.size();
    this.columns = columns;
    this.keyColumns = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      columns.get(i).setOffset(i);
    }

//    this.uidToIndexMap = columns.stream().collect(Collectors.toMap(ColumnExpr::getUid, ColumnExpr::getOffset));
  }

  public Schema(@NonNull Session session, Column[] columns) {
    this(Arrays.stream(columns)
                 .map(column -> new ColumnExpr(session.allocColumnID(), column))
                 .collect(Collectors.toList())
    );
  }

  @Override
  public Schema clone() {
    List<ColumnExpr> columnExprs = new ArrayList<>(columns.size());
    for (ColumnExpr column : columns) {
      columnExprs.add(column.clone());
    }

    Schema result = new Schema(columnExprs);
    for (KeyColumn key : keyColumns) {
      result.addKeyColumn(key.clone());
    }
    return result;
  }

  public boolean isEmpty() {
    return columns == null || columns.isEmpty();
  }

  public int size() {
    return columns == null ? 0 : columns.size();
  }

  public List<ColumnExpr> getColumns() {
    return columns;
  }

  public List<KeyColumn> getKeyColumns() {
    return keyColumns;
  }

  public void addKeyColumn(final KeyColumn keyColumn) {
    if (keyColumn != null) {
      keyColumns.add(keyColumn);
    }
  }

  public void resetKeyColumn() {
    this.keyColumns = new ArrayList<>();
  }

  public List<Integer> indexPosInSchema(List<ColumnExpr> columnExprs) {
    List<Integer> posList = null;
    if (columnExprs != null) {
      posList = new ArrayList<>(columnExprs.size());
      for (ColumnExpr columnExpr : columnExprs) {
        int pos = getColumnIndex(columnExpr);
        if (pos == -1) {
          return null;
        }
        posList.add(pos);
      }
    }

    return posList;
  }

  // TODO switch to the following implementation if memory becomes a concern
  public int getColumnIndex(ColumnExpr columnExpr) {
    final int offset = columnExpr.getOffset();
    final Long uid = columnExpr.getUid();

    if (offset != -1 && columns.size() > offset && columns.get(offset).getUid().equals(uid)) {
      return offset;
    }

    for (int i = 0; i < this.columns.size(); i++) {
      if (uid.equals(this.columns.get(i).getUid())) {
        return i;
      }
    }

    return -1;
  }

  public boolean isUniqueIndexCol(ColumnExpr col) {
    for (KeyColumn index : keyColumns) {
      if (index.getColumnExprs().size() == 1) {
        ColumnExpr columnExpr = index.getColumnExprs().get(0);
        return columnExpr.getUid().equals(col.getUid());
      }
    }

    return false;
  }

  public ColumnExpr getColumn(int index) {
    return columns.get(index);
  }

  public ColumnExpr getColumn(String name) {
    for (ColumnExpr col : this.columns) {
      if (name.equalsIgnoreCase(col.getAliasCol())) {
        return col;
      }
    }
    return null;
  }

  /**
   * Find an Column from schema for a SQLName. It compares the table/column names.
   *
   * @param column
   * @return
   */
  public ColumnExpr getColumn(final SQLName column) {
    String dbName = null;
    String tblName = null;
    String colName = column.getSimpleName().toLowerCase();
    if (column instanceof SQLPropertyExpr) {
      SQLExpr table = ((SQLPropertyExpr) column).getOwner();
      if (table instanceof SQLPropertyExpr) {
        tblName = ((SQLPropertyExpr) table).getSimpleName().toLowerCase();
        dbName = ((SQLPropertyExpr) table).getOwnernName().toLowerCase();
      } else {
        tblName = ((SQLPropertyExpr) column).getOwnernName().toLowerCase();
      }
    }
    ColumnExpr col;
    for (int i = 0; i < this.columns.size(); i++) {
      col = this.columns.get(i);
      if ((colName.equalsIgnoreCase(col.getOriCol()) || (col.getAliasCol() != null && colName.equalsIgnoreCase(col.getAliasCol())))
              && (dbName == null || dbName.equalsIgnoreCase(col.getCatalog()))
              && (tblName == null || tblName.equalsIgnoreCase(col.getAliasTable()))) {
        return col;
      }
    }
    return null;
  }
}
