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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.Schema;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlPrepareStatement;
import com.alibaba.druid.sql.visitor.SQLASTVisitorAdapter;

/**
 * GroupByAnalyzer
 *
 * @since 2019/10/9
 */
public class GroupByAnalyzer extends SQLASTVisitorAdapter {
  private List<SQLSelectItem> selects;
  private Schema schema;
  private boolean inExpr;
  private boolean isParam;
  private SQLObject outExpr;

  public GroupByAnalyzer(List<SQLSelectItem> selects, Schema schema) {
    this.selects = selects;
    this.schema = schema;
  }

  @Override
  public void preVisit(SQLObject astNode) {
    // prepare
    if (astNode instanceof MySqlPrepareStatement) {
      this.isParam = true;
      return;
    }

    this.inExpr = true;
    this.outExpr = astNode;
  }

  @Override
  public void postVisit(SQLObject astNode) {
    if (astNode instanceof SQLName) {
      SQLName sqlName = (SQLName) astNode;
      List<SQLAggregateExpr> orgAggFuncList = new ArrayList<>(this.selects.size());
      Map<SQLAggregateExpr, Integer> aggMapper = new HashMap<>(4);
      AggregateExprAnalyzer aggAnalyzer = new AggregateExprAnalyzer(orgAggFuncList, aggMapper);
      ColumnExpr column = this.schema.getColumn(sqlName.getSimpleName());
      if (column == null || !this.inExpr) {
        int index = AnalyzerUtil.resolveFromSelectFields(this.selects, sqlName, false);
        if (column != null) {
          this.outExpr = astNode;
          return;
        }
        if (index != -1) {
          SQLExpr expr = this.selects.get(index).getExpr();
          expr.accept(aggAnalyzer);
          if (orgAggFuncList.isEmpty()) {
            this.outExpr = expr;
          }
        }
        this.outExpr = astNode;
      }
    }

  }

  public void setInExpr(boolean inExpr) {
    this.inExpr = inExpr;
  }

  public boolean isParam() {
    return isParam;
  }

  public SQLObject getOutExpr() {
    return outExpr;
  }
}
