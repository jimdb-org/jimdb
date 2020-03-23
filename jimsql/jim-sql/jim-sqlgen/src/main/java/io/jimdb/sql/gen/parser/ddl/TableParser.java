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
package io.jimdb.sql.gen.parser.ddl;

import java.util.ArrayList;
import java.util.List;

import javax.script.ScriptEngine;

import io.jimdb.sql.gen.model.Field;
import io.jimdb.sql.gen.model.Index;
import io.jimdb.sql.gen.model.Table;
import io.jimdb.sql.gen.parser.ScriptExecutor;

import org.apache.commons.lang3.StringUtils;

import jdk.nashorn.api.scripting.ScriptObjectMirror;

/**
 * @version V1.0
 */
public final class TableParser {
  private static final String FILED_TAG = "tables";
  private static final String FILED_NAME = "name";
  private static final String FILED_REPLICA = "replica";
  private static final String FILED_PARTITION = "partition";
  private static final String FILED_ROWS = "rows";
  private static final String FILED_ENGINES = "engines";

  private TableParser() {
  }

  public static Table[] parse(ScriptEngine engine) {
    Object obj = engine.get(FILED_TAG);
    if (obj == null || !(obj instanceof ScriptObjectMirror)) {
      throw new RuntimeException("tables definition must not empty");
    }

    List<Table> tables = new ArrayList<>();
    ScriptExecutor.visit((ScriptObjectMirror) obj, v -> {
      Table table = parseTable(v);
      if (table != null) {
        tables.add(table);
      }
    });

    if (tables.isEmpty()) {
      throw new RuntimeException("tables definition invalid");
    }
    return tables.toArray(new Table[0]);
  }

  private static Table parseTable(ScriptObjectMirror obj) {
    Table table = new Table();
    table.setName(ScriptExecutor.getValue(obj, FILED_NAME));
    String replica = ScriptExecutor.getValue(obj, FILED_REPLICA);
    if (StringUtils.isNotBlank(replica)) {
      table.setReplica(Integer.parseInt(replica));
    }
    String partition = ScriptExecutor.getValue(obj, FILED_PARTITION);
    if (StringUtils.isNotBlank(partition)) {
      table.setPartition(Integer.parseInt(partition));
    }
    String[] rows = ScriptExecutor.getValues(obj, FILED_ROWS);
    if (rows != null && rows.length > 0) {
      List<Integer> rowSize = new ArrayList<>(rows.length);
      for (String row : rows) {
        if (StringUtils.isNotBlank(row)) {
          rowSize.add(Integer.valueOf(row));
        }
      }
      if (rowSize.size() > 0) {
        table.setRows(rowSize.toArray(new Integer[0]));
      }
    }
    String[] engines = ScriptExecutor.getValues(obj, FILED_ENGINES);
    if (engines != null && engines.length > 0) {
      table.setEngines(engines);
    }

    Field[] fields = FieldParser.parse(obj);
    if (fields == null || fields.length == 0) {
      throw new RuntimeException("table fields must not empty");
    }
    table.setFields(fields);

    Index[] indices = IndexParser.parse(obj);
    table.setIndices(indices);
    return table;
  }
}
