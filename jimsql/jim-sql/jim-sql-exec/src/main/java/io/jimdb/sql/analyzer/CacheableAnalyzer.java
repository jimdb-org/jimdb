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
package io.jimdb.sql.analyzer;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLLimit;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.expr.SQLInSubQueryExpr;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectGroupByClause;
import com.alibaba.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.druid.sql.visitor.SQLASTVisitorAdapter;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings({ "ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD", "LSC_LITERAL_STRING_COMPARISON" })
public final class CacheableAnalyzer extends SQLASTVisitorAdapter {

  private static boolean cacheable = true;

  public static boolean checkCacheable(SQLObject sqlObject) {

    if (!(sqlObject instanceof SQLSelectStatement) && !(sqlObject instanceof SQLInsertStatement) && !(sqlObject
            instanceof SQLDeleteStatement) && !(sqlObject instanceof SQLUpdateStatement)) {
      return false;
    }
    CacheableAnalyzer cacheableAnalyzer = new CacheableAnalyzer();
    sqlObject.accept(cacheableAnalyzer);

    return cacheableAnalyzer.isCacheable();
  }

  public boolean isCacheable() {
    return cacheable;
  }

  @Override
  public boolean visit(SQLInSubQueryExpr x) {
    cacheable = false;
    return false;
  }

  @Override
  public boolean visit(SQLVariantRefExpr sqlVariantRefExpr) {
    if (!sqlVariantRefExpr.getName().equals("?")) {
      cacheable = false;
    }
    return false;
  }

  @Override
  public boolean visit(SQLOrderBy sqlOrderBy) {
    for (SQLSelectOrderByItem item : sqlOrderBy.getItems()) {
      if (item.getExpr() instanceof SQLVariantRefExpr) {
        SQLVariantRefExpr sqlVariantRefExpr = (SQLVariantRefExpr) item.getExpr();
        if (sqlVariantRefExpr.getName().equals("?")) {
          cacheable = false;
          break;
        }
      }
    }
    return false;
  }

  @Override
  public boolean visit(SQLSelectGroupByClause groupByClause) {
    for (SQLExpr item : groupByClause.getItems()) {
      if (item instanceof SQLVariantRefExpr) {
        SQLVariantRefExpr sqlVariantRefExpr = (SQLVariantRefExpr) item;
        if (sqlVariantRefExpr.getName().equals("?")) {
          cacheable = false;
          break;
        }
      }
    }
    return false;
  }

  @Override
  public boolean visit(SQLLimit limit) {
    if (limit.getRowCount() != null) {
      if (limit.getRowCount() instanceof SQLVariantRefExpr) {
        SQLVariantRefExpr sqlVariantRefExpr = (SQLVariantRefExpr) limit.getRowCount();
        if (sqlVariantRefExpr.getName().equals("?")) {
          cacheable = false;
          return false;
        }
      }
    }

    if (limit.getOffset() != null) {
      if (limit.getOffset() instanceof SQLVariantRefExpr) {
        SQLVariantRefExpr sqlVariantRefExpr = (SQLVariantRefExpr) limit.getOffset();
        if (sqlVariantRefExpr.getName().equals("?")) {
          cacheable = false;
          return false;
        }
      }
    }

    return false;
  }
}















