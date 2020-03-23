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

import java.util.HashMap;
import java.util.Map;

import javax.script.ScriptEngine;

import io.jimdb.sql.gen.generators.GeneratorManager;
import io.jimdb.sql.gen.model.Data;
import io.jimdb.sql.gen.parser.ScriptExecutor;

import jdk.nashorn.api.scripting.ScriptObjectMirror;

/**
 * @version V1.0
 */
public final class DataParser {
  private static final String FILED_TAG = "datas";

  private static final Map<String, String[]> DEFAULT_GENS;

  static {
    DEFAULT_GENS = new HashMap() {
      {
        put("number", new String[]{ "digit", "digit", "null", "digit", "digit" });
        put("string", new String[]{ "letter", "letter", "null", "letter", "letter" });
        put("blob", new String[]{ "letter", "letter", "null", "letter", "letter" });
        put("temporal", new String[]{ "null", "datetime", "timestamp", "date", "time", "year" });
        put("enum", new String[]{ "letter", "letter", "null", "letter", "letter" });
      }
    };
  }

  private DataParser() {
  }

  public static Data parse(ScriptEngine engine) {
    Data data = new Data();

    Object obj = engine.get(FILED_TAG);
    if (obj != null) {
      if (!(obj instanceof ScriptObjectMirror) || ((ScriptObjectMirror) obj).isArray()) {
        throw new RuntimeException("datas definition invalid");
      }

      ScriptExecutor.visit((ScriptObjectMirror) obj, (k, v) -> data.set(k, GeneratorManager.getGenerator(v)));
    }

    DEFAULT_GENS.forEach((k, v) -> {
      if (data.get(k) == null) {
        data.set(k, GeneratorManager.getGenerator(v));
      }
    });
    return data;
  }
}
