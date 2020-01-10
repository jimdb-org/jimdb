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
package io.jimdb.core.model.prepare;

import java.util.List;

import io.jimdb.pb.Metapb;
import io.jimdb.core.values.Value;

import com.alibaba.druid.sql.ast.SQLStatement;

/**
 * @version V1.0
 */
public final class JimStatement {

  private int stmtId;

  /**
   * Use when fetch
   */
  private String sql;

  private SQLStatement sqlStatement;

  private int parametersNum;

  private List<Value> preparedParams;
  private List<Metapb.SQLType> prepareTypes;

  public String getSql() {
    return sql;
  }

  public void setSql(String sql) {
    this.sql = sql;
  }

  public List<Value> getPreparedParams() {
    return preparedParams;
  }

  public void setPreparedParams(List<Value> preparedParams) {
    this.preparedParams = preparedParams;
  }

  public List<Metapb.SQLType> getPrepareTypes() {
    return prepareTypes;
  }

  public void setPrepareTypes(List<Metapb.SQLType> prepareTypes) {
    this.prepareTypes = prepareTypes;
  }

  public int getParametersNum() {
    return parametersNum;
  }

  public void setParametersNum(int parametersNum) {
    this.parametersNum = parametersNum;
  }

  public int getStmtId() {
    return stmtId;
  }

  public void setStmtId(int stmtId) {
    this.stmtId = stmtId;
  }

  public SQLStatement getSqlStatement() {
    return sqlStatement;
  }

  public void setSqlStatement(SQLStatement sqlStatement) {
    this.sqlStatement = sqlStatement;
  }
}
