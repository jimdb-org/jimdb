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

import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.jimdb.common.utils.lang.Resettable;
import io.jimdb.core.values.Value;
import io.jimdb.pb.Metapb.SQLType;

import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings({ "EI_EXPOSE_REP", "EI_EXPOSE_REP2" })
public final class PreparedContext implements Resettable {
  private volatile int stmtSeq;
  private volatile SQLType[] paramTypes;
  private volatile Value[] paramValues;
  private volatile List<SQLVariantRefExpr> variantExprs;

  private final Map<Integer, PreparedStatement> stmtCache;
  private final Map<Integer, SoftReference<PreparedPlanner>> plannerCache;

  public PreparedContext() {
    this.stmtCache = new ConcurrentHashMap<>();
    this.plannerCache = new ConcurrentHashMap<>();
  }

  public int nextID() {
    return ++stmtSeq;
  }

  public int getStatementCount() {
    return stmtCache.size();
  }

  public PreparedStatement getStatement(int id) {
    return stmtCache.get(id);
  }

  public void setStatement(int id, PreparedStatement stmt) {
    stmtCache.put(id, stmt);
  }

  public void removeStatement(int id) {
    stmtCache.remove(id);
  }

  public PreparedPlanner getPlanner(int id) {
    Reference<PreparedPlanner> ref = plannerCache.get(id);
    return ref == null ? null : ref.get();
  }

  public void setPlanner(int id, PreparedPlanner planner) {
    plannerCache.put(id, new SoftReference<>(planner));
  }

  public void removePlanner(int id) {
    plannerCache.remove(id);
  }

  public SQLType[] getParamTypes() {
    return paramTypes;
  }

  public void setParamTypes(SQLType[] paramTypes) {
    this.paramTypes = paramTypes;
  }

  public Value[] getParamValues() {
    return paramValues;
  }

  public void setParamValues(Value[] paramValues) {
    this.paramValues = paramValues;
  }

  public List<SQLVariantRefExpr> getVariantExprs() {
    return variantExprs;
  }

  public void addVariantRefExpr(SQLVariantRefExpr variantExpr) {
    if (variantExprs == null) {
      variantExprs = new ArrayList<>();
    }
    variantExprs.add(variantExpr);
  }

  @Override
  public void reset() {
    paramTypes = null;
    paramValues = null;
    variantExprs = null;
  }

  @Override
  public void close() {
    this.reset();
    stmtSeq = 0;
  }
}
