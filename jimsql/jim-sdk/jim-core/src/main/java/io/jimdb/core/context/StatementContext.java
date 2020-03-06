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

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Supplier;

import io.jimdb.common.exception.BaseException;
import io.jimdb.common.utils.lang.Resettable;
import io.jimdb.core.SQLAnalyzer;
import io.jimdb.core.expression.Schema;
import io.jimdb.core.model.meta.Table;
import io.jimdb.core.model.privilege.PrivilegeInfo;
import io.jimdb.core.model.privilege.PrivilegeType;
import io.jimdb.core.values.DateValue;
import io.jimdb.pb.Basepb;

import com.google.common.collect.Maps;

/**
 * @version V1.0
 */
public final class StatementContext implements Resettable {
  private static final AtomicIntegerFieldUpdater<StatementContext> ERRCOUNT_UPDATER =
          AtomicIntegerFieldUpdater.newUpdater(StatementContext.class, "errCnt");

  private volatile int errCnt;
  private volatile boolean binaryProtocol = false;
  private volatile boolean replying = false;
  private volatile Instant timeout;

  private boolean nullReject;
  private int analyzerCnt;
  private List<PrivilegeInfo> privilegeInfos;

  private final Vector<SQLWarn> warnings;
  private final List<SQLAnalyzer> analyzers;
  private Map<Table, Schema> retrievedTablesAndSchemas; // used for updating table statistic
  private TimeZone localTimeZone = TimeZone.getDefault();
  private DateValue nowCached = null;

  public StatementContext() {
    this.warnings = new Vector<>(8);
    this.analyzers = new ArrayList<>(4);
    this.privilegeInfos = new ArrayList<>(8);
    this.retrievedTablesAndSchemas = Maps.newHashMap();
  }

  @Override
  public void reset() {
    this.analyzerCnt = 0;
    this.analyzers.forEach(analyzer -> analyzer.reset());
    this.nullReject = false;
    this.privilegeInfos.clear();
  }

  @Override
  public void close() {
    this.reset();
    this.errCnt = 0;
    this.binaryProtocol = false;
    this.replying = false;
    this.timeout = null;
    this.warnings.clear();
  }

  public SQLAnalyzer retainAnalyzer(final Supplier<SQLAnalyzer> builder) {
    analyzerCnt += 1;
    final SQLAnalyzer result;
    if (analyzers.size() < analyzerCnt) {
      result = builder.get();
      analyzers.add(result);
      return result;
    }

    result = analyzers.get(analyzerCnt - 1);
    result.reset();
    return result;
  }

  public void releaseAnalyzer() {
    this.analyzerCnt -= 1;
  }

  public Set<Table> getRetrievedTables() {
    return retrievedTablesAndSchemas.keySet();
  }

  public Schema getRetrievedSchema(Table table) {
    return retrievedTablesAndSchemas.get(table);
  }

  public void addRetrievedTableAndSchema(Table table, Schema schema) {
    retrievedTablesAndSchemas.put(table, schema);
  }

  public boolean isNullReject() {
    return nullReject;
  }

  public void setNullReject(boolean nullReject) {
    this.nullReject = nullReject;
  }

  public int getErrCount() {
    return errCnt;
  }

  public Instant getTimeout() {
    return timeout;
  }

  public void setTimeout(final Instant timeout) {
    this.timeout = timeout;
  }

  public boolean isBinaryProtocol() {
    return binaryProtocol;
  }

  public void setBinaryProtocol(boolean binaryProtocol) {
    this.binaryProtocol = binaryProtocol;
  }

  public boolean isReplying() {
    return replying;
  }

  public void setReplying(boolean replying) {
    this.replying = replying;
  }

  public List<PrivilegeInfo> getPrivilegeInfos() {
    return privilegeInfos;
  }

  public void addPrivilegeInfo(String catalog, String table, PrivilegeType privilegeType) {
    this.privilegeInfos.add(new PrivilegeInfo(catalog, table, privilegeType));
  }

  public TimeZone getLocalTimeZone() {
    return localTimeZone;
  }

  public void setLocalTimeZone(TimeZone localTimeZone) {
    this.localTimeZone = localTimeZone;
  }

  public void addWarning(final BaseException warn) {
    if (this.warnings.size() < Short.MAX_VALUE) {
      this.warnings.add(new SQLWarn(SQLWarnLevel.Warning, warn));
    }
  }

  public void addNote(final BaseException note) {
    if (this.warnings.size() < Short.MAX_VALUE) {
      this.warnings.add(new SQLWarn(SQLWarnLevel.Note, note));
    }
  }

  public void addError(final BaseException error) {
    if (this.warnings.size() < Short.MAX_VALUE) {
      this.warnings.add(new SQLWarn(SQLWarnLevel.Error, error));
      ERRCOUNT_UPDATER.incrementAndGet(this);
    }
  }

  public DateValue getNowCached(TimeZone timeZone) {
    if (nowCached == null) {
      nowCached = DateValue.getNow(Basepb.DataType.DateTime, 6, timeZone);
    }
    return nowCached;
  }

  /**
   *
   */
  public static final class SQLWarn {
    private final SQLWarnLevel level;
    private final BaseException cause;

    public SQLWarn(final SQLWarnLevel level, final BaseException cause) {
      this.level = level;
      this.cause = cause;
    }

    public SQLWarnLevel getLevel() {
      return level;
    }

    public BaseException getCause() {
      return cause;
    }
  }

  /**
   *
   */
  public enum SQLWarnLevel {
    Note, Warning, Error;
  }
}
