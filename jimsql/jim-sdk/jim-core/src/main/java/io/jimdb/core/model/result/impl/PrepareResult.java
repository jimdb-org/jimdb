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
package io.jimdb.core.model.result.impl;

import java.util.List;

import io.jimdb.core.model.result.ResultType;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.model.result.ExecResult;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings({ "EI_EXPOSE_REP", "EI_EXPOSE_REP2" })
public final class PrepareResult extends ExecResult {

  private int stmtId;

  private int columnsNum;

  private int parametersNum;

  private int warnCount;

  private SQLStatement sqlStatement;

  private List<SQLVariantRefExpr> params;

  private long metaVersion;

  private boolean useCache;

  private ColumnExpr[] columnExprs;

  public void setColumnExprs(ColumnExpr[] columnExprs) {
    this.columnExprs = columnExprs;
  }

  @Override
  public ColumnExpr[] getColumns() {
    return columnExprs;
  }

  public void setStmtId(int stmtId) {
    this.stmtId = stmtId;
  }

  public void setColumnsNum(int columnsNum) {
    this.columnsNum = columnsNum;
  }

  public void setParametersNum(int parametersNum) {
    this.parametersNum = parametersNum;
  }

  public void setWarnCount(int warnCount) {
    this.warnCount = warnCount;
  }

  public int getStmtId() {
    return stmtId;
  }

  public int getColumnsNum() {
    return columnsNum;
  }

  public int getParametersNum() {
    return parametersNum;
  }

  public int getWarnCount() {
    return warnCount;
  }

  public SQLStatement getSqlStatement() {
    return sqlStatement;
  }

  public void setSqlStatement(SQLStatement sqlStatement) {
    this.sqlStatement = sqlStatement;
  }

  public List<SQLVariantRefExpr> getParams() {
    return params;
  }

  public void setParams(List<SQLVariantRefExpr> params) {
    this.params = params;
  }

  public long getMetaVersion() {
    return metaVersion;
  }

  public void setMetaVersion(long metaVersion) {
    this.metaVersion = metaVersion;
  }

  public boolean isUseCache() {
    return useCache;
  }

  public void setUseCache(boolean useCache) {
    this.useCache = useCache;
  }

  @Override
  public ResultType getType() {
    return ResultType.PREPARE;
  }

  public void close(){

  }
}
