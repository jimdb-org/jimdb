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
package io.jimdb.fi;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

import io.jimdb.common.utils.lang.IOUtil;

import org.apache.commons.lang3.StringUtils;

/**
 * A fail point implementation for Java.
 * <p>
 * Fail points are code instrumentations that allow errors and other behavior
 * to be injected dynamically at runtime for testing purposes.
 * <p>
 * Fail points are flexible and can be configured to exhibit a variety of behavior,
 * including errors, early returns, and sleeping. They can be controlled both
 * programmatically and via the environment, and can be triggered conditionally and probabilistically.
 * <p>
 * This implementation is inspired by FreeBSD's:
 * <https://freebsd.org/cgi/man.cgi?query=fail>.
 *
 * @version V1.0
 */
public final class FailPoint {
  private static final ConcurrentHashMap<String, ActionContainer> FAULT_REGISTRY = new ConcurrentHashMap<>(0);

  private static final boolean FAULT_ENABLE;

  private FailPoint() {
  }

  static {
    if ("true".equalsIgnoreCase(System.getProperty("fi.enable", "false"))
            || StringUtils.isNotBlank(System.getProperty("fi.config", ""))) {
      FAULT_ENABLE = true;
    } else {
      FAULT_ENABLE = false;
    }

    if (FAULT_ENABLE) {
      setup(null);
    }
  }

  public static boolean isEnable() {
    return FAULT_ENABLE;
  }

  /**
   * Registry with failpoints configuration.
   * An alternative location can be set through
   * -Dfi.config=<file_name>
   *
   * @param confFile
   */
  public static void setup(String confFile) {
    clear();

    if (StringUtils.isBlank(confFile)) {
      confFile = System.getProperty("fi.config", "");
    }
    if (StringUtils.isNotBlank(confFile)) {
      Properties properties = IOUtil.loadResource(confFile);
      if (properties != null) {
        properties.forEach((k, v) -> {
          String action = v == null ? null : v.toString();
          if (StringUtils.isNotBlank(action)) {
            config(k.toString(), action);
          }
        });
      }
    }
  }

  /**
   * Configure the actions for a fail point at runtime.
   *
   * @param name   fault name
   * @param action fault actions
   */
  public static void config(String name, String action) {
    FAULT_REGISTRY.putIfAbsent(name, new ActionContainer());
    FAULT_REGISTRY.get(name).setActions(action);
  }

  /**
   * Remove the actions for a fail point at runtime.
   *
   * @param name
   */
  public static void remove(String name) {
    ActionContainer action = FAULT_REGISTRY.remove(name);
    if (action != null) {
      action.setActions("");
    }
  }

  /**
   * Clear actions for all fail points.
   */
  public static void clear() {
    FAULT_REGISTRY.forEachKey(1, k -> remove(k));
  }

  public static Map<String, String> registers() {
    Map<String, String> result = new HashMap<>();
    FAULT_REGISTRY.forEach((k, v) -> result.put(k, v.toString()));
    return result;
  }

  /**
   * A basic fail point.
   * This form of fail point can be configured to error, print, sleep, pause, but not to return.
   *
   * @param name fault name
   */
  public static void inject(String name) {
    if (FAULT_ENABLE) {
      execute(name);
    }
  }

  /**
   * A fail point with conditional execution.
   * The predicate expression that must evaluate to true before the fail point is evaluated.
   *
   * @param name      fault name
   * @param predicate predicate expression
   */
  public static void inject(String name, BooleanSupplier predicate) {
    if (FAULT_ENABLE && predicate.getAsBoolean()) {
      execute(name);
    }
  }

  /**
   * A fail point that maybe early return value.
   *
   * @param name fault name
   * @param func early-return expression
   * @return
   */
  public static <R> R inject(String name, Function<String, R> func) {
    if (FAULT_ENABLE) {
      String rs = execute(name);
      return rs == null ? null : func.apply(rs);
    }

    return null;
  }

  /**
   * A fail point with conditional execution.
   * The predicate expression that must evaluate to true before the fail point is evaluated.
   *
   * @param name      fault name
   * @param predicate predicate expression
   * @param func      early-return expression
   * @return
   */
  public static <R> R inject(String name, BooleanSupplier predicate, Function<String, R> func) {
    if (FAULT_ENABLE && predicate.getAsBoolean()) {
      String rs = execute(name);
      return rs == null ? null : func.apply(rs);
    }

    return null;
  }

  private static String execute(String name) {
    ActionContainer action = FAULT_REGISTRY.get(name);
    if (action == null) {
      return null;
    }

    return action.execute(name);
  }
}
