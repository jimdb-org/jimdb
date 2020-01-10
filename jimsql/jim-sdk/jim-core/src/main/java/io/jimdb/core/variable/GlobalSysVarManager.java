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
package io.jimdb.core.variable;

import java.util.Map;

/**
 * @version V1.0
 */
public interface GlobalSysVarManager {

  /**
   * Return all the global system variable values.
   *
   * @return
   */
  Map<String, SysVariable> getAllVars();

  /**
   * Get the global system variable value for name.
   * If the system variable is not present, then return null.
   *
   * @param name
   * @return
   */
  String getVar(String name);

  /**
   * Set the global system variable name to value.
   *
   * @param name
   * @param value
   */
  void setVar(String name, String value);
}
