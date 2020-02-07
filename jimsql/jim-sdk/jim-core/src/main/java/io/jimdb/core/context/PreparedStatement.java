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

import io.jimdb.common.utils.lang.Resetable;
import io.jimdb.core.values.Value;
import io.jimdb.pb.Metapb.SQLType;

import com.alibaba.druid.sql.ast.SQLStatement;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings({ "EI_EXPOSE_REP", "EI_EXPOSE_REP2" })
public final class PreparedStatement implements Resetable {
  private final int id;
  private final int params;
  private final boolean cachePlan;
  private final String sql;
  private final SQLStatement sqlStmt;
  private final Value[] boundValues;
  private final PreparedContext context;

  private volatile SQLType[] paramTypes;

  public PreparedStatement(PreparedContext context, String sql, int id, boolean cachePlan, int params, SQLStatement sqlStmt) {
    this.id = id;
    this.sql = sql;
    this.cachePlan = cachePlan;
    this.params = params;
    this.sqlStmt = sqlStmt;
    this.boundValues = new Value[params];
    this.context = context;
  }

  public int getParams() {
    return params;
  }

  public String getSql() {
    return sql;
  }

  public boolean isCachePlan() {
    return cachePlan;
  }

  public SQLStatement getSqlStmt() {
    return sqlStmt;
  }

  public Value[] getBoundValues() {
    return boundValues;
  }

  public SQLType[] getParamTypes() {
    return paramTypes;
  }

  public void setParamTypes(SQLType[] paramTypes) {
    this.paramTypes = paramTypes;
  }

  @Override
  public void reset() {
    for (int i = 0; i < boundValues.length; i++) {
      boundValues[i] = null;
    }
  }

  @Override
  public void close() {
    context.removeStatement(id);
    context.removePlanner(id);
  }
}
