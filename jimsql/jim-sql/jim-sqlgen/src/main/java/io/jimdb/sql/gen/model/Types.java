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
package io.jimdb.sql.gen.model;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings("PMB_POSSIBLE_MEMORY_BLOAT")
public final class Types {
  private static final Map<String, String> TYPES = new HashMap<>(16);

  public static final String DEFAULT_TYPE = "string";

  static {
    register("number", "number");
    register("int", "number");
    register("bigint", "number");
    register("float", "number");
    register("double", "number");
    register("decimal", "number");
    register("fixed", "number");
    register("bool", "number");
    register("bit", "number");
    register("string", "string");
    register("char", "string");
    register("varchar", "string");
    register("blob", "blob");
    register("text", "blob");
    register("binary", "blob");
    register("temporal", "temporal");
    register("date", "temporal");
    register("time", "temporal");
    register("year", "temporal");
    register("enum", "enum");
    register("set", "enum");
  }

  public static void register(String name, String symbol) {
    TYPES.put(name, symbol);
  }

  public static String getSymbol(String name) {
    String symbol = TYPES.get(name);
    return StringUtils.isBlank(symbol) ? DEFAULT_TYPE : symbol;
  }
}
