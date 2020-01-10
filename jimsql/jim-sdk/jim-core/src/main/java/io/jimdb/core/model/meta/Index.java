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
package io.jimdb.core.model.meta;

import java.util.Collections;
import java.util.List;

import io.jimdb.pb.Metapb.MetaState;
import io.jimdb.pb.Metapb.IndexInfo;
import io.jimdb.pb.Metapb.IndexType;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings({ "EI_EXPOSE_REP", "FCCD_FIND_CLASS_CIRCULAR_DEPENDENCY" })
public final class Index {
  private final Integer id;

  private final IndexInfo indexInfo;

  private final Column[] columns;

  private final Table table;

  public Index(Table table, IndexInfo indexInfo) {
    this.table = table;
    this.indexInfo = indexInfo;
    this.id = indexInfo.getId();

    List<Integer> indexColumns = indexInfo.getColumnsList() == null ? Collections.EMPTY_LIST : indexInfo.getColumnsList();
    Column[] tableColumns = table.getReadableColumns();
    this.columns = new Column[indexColumns.size()];

    for (int i = 0; i < indexColumns.size(); i++) {
      int colId = indexColumns.get(i);
      for (Column column : tableColumns) {
        if (column.getId() == colId) {
          this.columns[i] = column;
          break;
        }
      }
    }
  }

  public Integer getId() {
    return id;
  }

  public String getName() {
    return indexInfo.getName();
  }

  public Column[] getColumns() {
    return columns;
  }

  public boolean isUnique() {
    return indexInfo.getUnique();
  }

  public boolean isPrimary() {
    return indexInfo.getPrimary();
  }

  public IndexType getType() {
    return indexInfo.getType();
  }

  public String getIndexComment() {
    return indexInfo.getComment();
  }

  public MetaState getState() {
    return indexInfo.getState();
  }

  public Table getTable() {
    return table;
  }
}
