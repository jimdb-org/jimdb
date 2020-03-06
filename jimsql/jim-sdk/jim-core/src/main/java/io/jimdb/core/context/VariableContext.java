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
package io.jimdb.core.context;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.jimdb.core.plugin.SQLEngine;
import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.utils.lang.Resettable;
import io.jimdb.core.variable.DefaultGlobalSysVarManager;
import io.jimdb.core.variable.GlobalSysVarManager;
import io.jimdb.core.variable.SysVariable;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @version V1.0
 */
public final class VariableContext implements Resettable {
  private static final Logger LOG = LoggerFactory.getLogger(VariableContext.class);

  public static final String VAR_TIME_ZONE = "time_zone";
  public static final String VAR_AUTO_COMMIT = "autocommit";




  private final SQLEngine sqlEngine;
  private final GlobalSysVarManager globalVars;
  private final Map<String, String> userVars;
  private final Map<String, String> sessionVars;

  @sun.misc.Contended
  private volatile short status;   // session status. e.g. in transaction, auto commit and so on.
  private volatile boolean autocommit;
  private volatile String defaultCatalog;
  private volatile ZoneId timeZone;

  public VariableContext(final SQLEngine sqlEngine, final GlobalSysVarManager globalVars) {
    this.sqlEngine = sqlEngine;
    this.globalVars = globalVars == null ? new DefaultGlobalSysVarManager(sqlEngine) : globalVars;
    this.userVars = new ConcurrentHashMap<>();
    this.sessionVars = new ConcurrentHashMap<>();
    this.timeZone = ZoneId.systemDefault();
    this.setSessionVariable(VAR_AUTO_COMMIT, "on");
  }

  public String getDefaultCatalog() {
    return defaultCatalog;
  }

  public void setDefaultCatalog(String defaultCatalog) {
    this.defaultCatalog = defaultCatalog;
  }

  public ZoneId getTimeZone() {
    return timeZone;
  }

  public void setTimeZone(ZoneId timeZone) {
    this.timeZone = timeZone;
  }

  public boolean isAutocommit() {
    return autocommit;
  }

  public void setAutocommit(boolean autocommit) {
    this.autocommit = autocommit;
  }

  public short getStatus() {
    return status;
  }

  public void setStatus(SQLEngine.ServerStatus status, boolean on) {
    short s = this.sqlEngine.getServerStatusFlag(status);
    if (on) {
      this.status |= s;
    } else {
      this.status &= ~s;
    }
  }

  public SysVariable getVariableDefine(String name) {
    return this.sqlEngine.getSysVariable(name);
  }

  public String getUserVariable(String name) {
    return this.userVars.get(name.toLowerCase());
  }

  public void setUserVariable(String name, String val) {
    if (val == null) {
      this.userVars.remove(name.toLowerCase());
      return;
    }
    this.userVars.put(name.toLowerCase(), val);
  }

  public String getSessionVariable(String name) {
    String val = this.sessionVars.get(name.toLowerCase());
    if (StringUtils.isNotBlank(val)) {
      return val;
    }

    SysVariable sysVar = this.getVariableDefine(name);
    if (sysVar == null) {
      throw DBException.get(ErrorModule.EXECUTOR, ErrorCode.ER_UNKNOWN_SYSTEM_VARIABLE, name);
    }

    if (!sysVar.isSession() && !sysVar.isNone()) {
      throw DBException.get(ErrorModule.EXECUTOR, ErrorCode.ER_INCORRECT_GLOBAL_LOCAL_VAR, name, "GLOBAL");
    }
    return this.globalVars.getVar(name);
  }

  public void setGlobalVariable(String name, String value) {
    this.globalVars.setVar(name, value);
  }

  public String getGlobalVariable(String name) {
    String value = this.getNoneVariable(name);
    if (value != null) {
      return value;
    }

    return this.globalVars.getVar(name);
  }

  public void setSessionVariable(String name, String value) {
    SysVariable sysVar = this.getVariableDefine(name);
    if (sysVar == null) {
      throw DBException.get(ErrorModule.EXECUTOR, ErrorCode.ER_UNKNOWN_SYSTEM_VARIABLE, name);
    }

    name = name.toLowerCase();
    switch (name) {
      case VAR_TIME_ZONE:
        this.timeZone = this.parseTimeZone(value);
        break;

      case VAR_AUTO_COMMIT:
        boolean autocommit = this.parseOn(value);
        this.setStatus(SQLEngine.ServerStatus.AUTOCOMMIT, autocommit);
        this.setAutocommit(autocommit);
        if (autocommit) {
          this.setStatus(SQLEngine.ServerStatus.INTRANS, false);
        }
        break;

      default:
        break;
    }

    this.sessionVars.put(name, value);
  }

  public String getNoneVariable(String name) {
    SysVariable sysVar = this.getVariableDefine(name);
    if (sysVar == null) {
      throw DBException.get(ErrorModule.EXECUTOR, ErrorCode.ER_UNKNOWN_SYSTEM_VARIABLE, name);
    }

    if (sysVar.isNone()) {
      String value = sysVar.getValue();
      return value == null ? "" : value;
    }
    return null;
  }

  public Map<String, SysVariable> getAllVars() {
    return this.globalVars.getAllVars();
  }

  @Override
  public void reset() {
  }

  @Override
  public void close() {
  }

  private boolean parseOn(String val) {
    return "on".equalsIgnoreCase(val) || "1".equals(val);
  }

  private ZoneId parseTimeZone(String zone) {
    if ("system".equalsIgnoreCase(zone)) {
      return ZoneId.systemDefault();
    }

    try {
      return ZoneId.of(zone);
    } catch (Exception ex) {
      LOG.warn("Parse timezone error: ", ex);
    }

    // The value can be given as a string indicating an offset from UTC, such as '+18:00' to '-18:00'.
    try {
      return ZoneId.ofOffset("", ZoneOffset.of(zone));
    } catch (Exception ex) {
      LOG.warn("Parse timezone error: ", ex);
    }

    throw DBException.get(ErrorModule.PROTO, ErrorCode.ER_UNKNOWN_TIME_ZONE);
  }

  public SQLEngine getSqlEngine() {
    return sqlEngine;
  }


}
