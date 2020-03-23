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
package io.jimdb.sql.gen.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import jdk.nashorn.api.scripting.ScriptObjectMirror;

/**
 * @version V1.0
 */
@SuppressFBWarnings({ "EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS", "SCRIPT_ENGINE_INJECTION", "OCP_OVERLY_CONCRETE_PARAMETER" })
public final class ScriptExecutor {
  private static final ScriptEngineManager MANAGER = new ScriptEngineManager();

  private ScriptExecutor() {
  }

  public static ScriptEngine getEngine() {
    return MANAGER.getEngineByName("JavaScript");
  }

  public static void exec(ScriptEngine engine, String script) {
    try {
      engine.eval(script);
    } catch (ScriptException ex) {
      throw new RuntimeException("ScriptExecutor exec script error.", ex);
    }
  }

  public static String getValue(ScriptObjectMirror obj, String key) {
    Object value = obj.get(key);
    return value == null ? null : value.toString();
  }

  public static String[] getValues(ScriptObjectMirror obj, String key) {
    Object value = obj.get(key);
    return value == null ? null : toString(value);
  }

  public static void visit(ScriptObjectMirror state, String key, Consumer<ScriptObjectMirror> action) {
    Object obj = state.get(key);
    if (obj == null || !(obj instanceof ScriptObjectMirror)) {
      return;
    }

    visit((ScriptObjectMirror) obj, action);
  }

  public static void visit(ScriptObjectMirror scriptObj, Consumer<ScriptObjectMirror> action) {
    if (!scriptObj.isArray()) {
      action.accept(scriptObj);
      return;
    }

    scriptObj.forEach((k, v) -> {
      if (v instanceof ScriptObjectMirror) {
        action.accept((ScriptObjectMirror) v);
      }
    });
  }

  public static void visit(ScriptObjectMirror scriptObj, BiConsumer<String, String[]> action) {
    scriptObj.forEach((k, v) -> action.accept(k, toString(v)));
  }

  private static String[] toString(Object obj) {
    if (!(obj instanceof ScriptObjectMirror) || !((ScriptObjectMirror) obj).isArray()) {
      return new String[]{ obj.toString() };
    }

    List<String> values = new ArrayList<>();
    ((ScriptObjectMirror) obj).forEach((k, v) -> values.add(v.toString()));
    return values.toArray(new String[0]);
  }
}
