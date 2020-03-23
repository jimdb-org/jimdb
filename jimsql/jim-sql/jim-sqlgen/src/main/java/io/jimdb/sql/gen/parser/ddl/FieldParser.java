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

import io.jimdb.sql.gen.model.Field;
import io.jimdb.sql.gen.parser.ScriptExecutor;

import org.apache.commons.lang3.StringUtils;

import jdk.nashorn.api.scripting.ScriptObjectMirror;

/**
 * @version V1.0
 */
public final class FieldParser {
  private static final String FILED_TAG = "fields";
  private static final String FILED_NAME = "name";
  private static final String FILED_TYPE = "type";
  private static final String FILED_NOTNULL = "notnull";
  private static final String FILED_DEFAULT = "default";
  private static final String FILED_SIGN = "sign";

  private FieldParser() {
  }

  public static Field[] parse(ScriptObjectMirror state) {
    final List<Field> fields = new ArrayList<>();
    ScriptExecutor.visit(state, FILED_TAG, obj -> {
      Field field = parseField(obj);
      if (field != null) {
        fields.add(field);
      }
    });

    return fields.toArray(new Field[0]);
  }

  private static Field parseField(ScriptObjectMirror obj) {
    String type = ScriptExecutor.getValue(obj, FILED_TYPE);
    if (StringUtils.isBlank(type)) {
      throw new RuntimeException("field type must not empty");
    }

    Field field = new Field();
    field.setType(type);
    field.setName(ScriptExecutor.getValue(obj, FILED_NAME));
    field.setNotNull("true".equalsIgnoreCase(ScriptExecutor.getValue(obj, FILED_NOTNULL)));
    field.setDefValue(ScriptExecutor.getValue(obj, FILED_DEFAULT));

    String sign = ScriptExecutor.getValue(obj, FILED_SIGN);
    if (StringUtils.isNotBlank(sign)) {
      switch (sign.toUpperCase()) {
        case "UNSIGNED":
          field.setSign(Field.SignType.UNSIGNED);
          break;
        case "ALL":
          field.setSign(Field.SignType.ALL);
          break;
        default:
          field.setSign(Field.SignType.SIGNED);
          break;
      }
    }

    return field;
  }
}
