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

import io.jimdb.sql.gen.model.Index;
import io.jimdb.sql.gen.parser.ScriptExecutor;

import org.apache.commons.lang3.StringUtils;

import jdk.nashorn.api.scripting.ScriptObjectMirror;

/**
 * @version V1.0
 */
public final class IndexParser {
  private static final String FILED_TAG = "indices";
  private static final String FILED_NAME = "name";
  private static final String FILED_FIELDS = "fields";
  private static final String FILED_TYPE = "type";

  private IndexParser() {
  }

  public static Index[] parse(ScriptObjectMirror state) {
    final List<Index> indices = new ArrayList<>();
    ScriptExecutor.visit(state, FILED_TAG, obj -> {
      Index index = parseIndex(obj);
      if (index != null) {
        indices.add(index);
      }
    });

    return indices.toArray(new Index[0]);
  }

  private static Index parseIndex(ScriptObjectMirror obj) {
    String[] fields = ScriptExecutor.getValues(obj, FILED_FIELDS);
    if (fields == null || fields.length == 0) {
      throw new RuntimeException("index fields must not empty");
    }

    Index index = new Index();
    index.setFields(fields);
    index.setName(ScriptExecutor.getValue(obj, FILED_NAME));
    String type = ScriptExecutor.getValue(obj, FILED_TYPE);
    if (StringUtils.isNotBlank(type)) {
      switch (type.toUpperCase()) {
        case "PRIMARY":
          index.setType(Index.IndexType.PRIMARY);
          break;
        case "UNIQUE":
          index.setType(Index.IndexType.UNIQUE);
          break;
        default:
          index.setType(Index.IndexType.INDEX);
          break;
      }
    }
    return index;
  }
}
