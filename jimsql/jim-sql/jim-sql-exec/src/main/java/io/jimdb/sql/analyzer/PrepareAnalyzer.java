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

import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.druid.sql.visitor.SQLASTVisitorAdapter;

/**
 * @version V1.0
 */
public class PrepareAnalyzer extends SQLASTVisitorAdapter {

  List<SQLVariantRefExpr> variantRefExprs;

  @Override
  public void postVisit(SQLObject node) {

    if (node.getClass() == SQLVariantRefExpr.class) {
      SQLVariantRefExpr sqlVariantRefExpr = (SQLVariantRefExpr) node;
      if ("?".equals(sqlVariantRefExpr.getName())) {
        variantRefExprs.add(sqlVariantRefExpr);
      }
    }
  }

  public List<SQLVariantRefExpr> getVariantRefExprs() {
    return variantRefExprs;
  }

  public void setVariantRefExprs(List<SQLVariantRefExpr> variantRefExprs) {
    this.variantRefExprs = variantRefExprs;
  }
}


