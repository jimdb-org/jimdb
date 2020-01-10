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
package io.jimdb.sql.analyzer;

import java.util.List;
import java.util.Map;

import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.visitor.SQLASTVisitorAdapter;

/**
 * @version V1.0
 */
public class AggregateExprAnalyzer extends SQLASTVisitorAdapter {
  private List<SQLAggregateExpr> aggFuncList;
  private Map<SQLAggregateExpr, Integer> aggMapper;

  public AggregateExprAnalyzer(List<SQLAggregateExpr> aggFuncList, Map<SQLAggregateExpr, Integer> aggMapper) {
    this.aggFuncList = aggFuncList;
    this.aggMapper = aggMapper;
  }

  @Override
  public boolean visit(SQLPropertyExpr expr) {
    return false;
  }

  @Override
  public boolean visit(SQLIdentifierExpr sqlIdentifierExpr) {
    return false;
  }

  @Override
  public boolean visit(SQLAggregateExpr expr) {
    return false;
  }

  @Override
  public void postVisit(SQLObject astNode) {
    if (astNode.getClass() == SQLAggregateExpr.class) {
      SQLAggregateExpr astNode1 = (SQLAggregateExpr) astNode;
      aggMapper.put(astNode1, aggFuncList.size());
      aggFuncList.add(astNode1);
    }
  }

  public List<SQLAggregateExpr> getAggFuncList() {
    return aggFuncList;
  }

  public void setAggFuncList(List<SQLAggregateExpr> aggFuncList) {
    this.aggFuncList = aggFuncList;
  }

  public Map<SQLAggregateExpr, Integer> getAggMapper() {
    return aggMapper;
  }

  public void setAggMapper(Map<SQLAggregateExpr, Integer> aggMapper) {
    this.aggMapper = aggMapper;
  }
}
