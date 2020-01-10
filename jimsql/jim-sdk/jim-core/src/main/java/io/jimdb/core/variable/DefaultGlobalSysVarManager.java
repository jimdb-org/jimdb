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

import io.jimdb.core.plugin.SQLEngine;
import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;

/**
 * @version V1.0
 */
public final class DefaultGlobalSysVarManager implements GlobalSysVarManager {
  private final SQLEngine sqlEngine;

  public DefaultGlobalSysVarManager(final SQLEngine sqlEngine) {
    this.sqlEngine = sqlEngine;
  }

  @Override
  public Map<String, SysVariable> getAllVars() {
    return sqlEngine.getAllVars();
  }

  @Override
  public String getVar(String name) {
    SysVariable var = sqlEngine.getSysVariable(name);
    return var == null ? null : var.getValue();
  }

  @Override
  public void setVar(String name, String value) {
    throw DBException.get(ErrorModule.EXECUTOR, ErrorCode.ER_NOT_SUPPORTED_YET, " set GLOBAL variable");
  }
}
