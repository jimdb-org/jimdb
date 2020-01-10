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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.pb.Metapb.CatalogInfo;
import io.jimdb.pb.Metapb.MetaState;
import io.jimdb.pb.Metapb.TableInfo;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings({ "FCCD_FIND_CLASS_CIRCULAR_DEPENDENCY", "EI_EXPOSE_REP" })
public final class Catalog {
  private final Integer id;

  private final CatalogInfo catalogInfo;

  private final Table[] tables;

  private final Map<String, Table> tableMap;

  public Catalog(CatalogInfo catalogInfo, List<TableInfo> tableInfos) {
    this.catalogInfo = catalogInfo;
    this.id = catalogInfo.getId();
    this.tableMap = tableInfos == null ? Collections.EMPTY_MAP : new HashMap<>(tableInfos.size(), 1.0F);

    if (tableInfos != null) {
      for (TableInfo tableInfo : tableInfos) {
        if (tableInfo.getState() != MetaState.Public) {
          continue;
        }
        Table table = new Table(this, tableInfo);
        this.tableMap.put(tableInfo.getName().toLowerCase(), table);
      }
    }
    List<Table> tableList = new ArrayList<>(tableMap.values());
    Collections.sort(tableList, (c1, c2) -> {
      if (c1.getId().intValue() == c2.getId().intValue()) {
        return 0;
      }
      return c1.getId().intValue() > c2.getId().intValue() ? 1 : -1;
    });
    this.tables = tableList.toArray(new Table[0]);
  }

  public Integer getId() {
    return id;
  }

  public String getName() {
    return catalogInfo.getName();
  }

  public long getCreateTime() {
    return catalogInfo.getCreateTime();
  }

  public MetaState getState() {
    return catalogInfo.getState();
  }

  public Table getTable(String name) {
    Table table = tableMap.get(name.toLowerCase());
    if (table == null) {
      throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_BAD_TABLE_ERROR, name);
    }
    return table;
  }

  public Table getTable(int id) {
    for (Table table : tables) {
      if (table.getId().intValue() == id) {
        return table;
      }
    }

    throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_BAD_TABLE_ERROR, String.valueOf(id));
  }

  public Table[] getTables() {
    return tables;
  }
}
