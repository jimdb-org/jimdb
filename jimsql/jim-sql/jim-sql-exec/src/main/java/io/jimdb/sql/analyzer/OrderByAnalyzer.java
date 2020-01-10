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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.Schema;
import io.jimdb.sql.operator.RelOperator;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.visitor.SQLASTVisitorAdapter;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings()
public class OrderByAnalyzer extends SQLASTVisitorAdapter {

  protected boolean inAggFunc;
  protected boolean inExpr;
  protected boolean orderBy;
  protected RelOperator operator;
  protected SQLExpr sqlExpr;

  protected ExpressionAnalyzer.ClauseType clause = ExpressionAnalyzer.ClauseType.NULL;
  protected List<SQLSelectItem> selectItems = new ArrayList<>();
  protected Map<SQLAggregateExpr, Integer> aggMapper;
  protected Map<SQLName, Integer> colMapper;
  protected List<SQLExpr> groupByCols;
  protected Exception exception;

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
  public void preVisit(SQLObject astNode) {
    if (astNode instanceof SQLAggregateExpr) {
      this.inAggFunc = true;
      return;
    }
    if (astNode instanceof SQLPropertyExpr || astNode instanceof SQLIdentifierExpr) {
      return;
    }
    this.inExpr = true;
  }

  @Override
  public void postVisit(SQLObject astNode) {
    if (astNode.getClass() == SQLAggregateExpr.class) {
      SQLAggregateExpr sqlAggregateExpr = (SQLAggregateExpr) astNode;

      // analyze args for supporting the sql(select name as n from user order by count(n))
      sqlAggregateExpr.getArguments().forEach(arg -> {
        arg.accept(this);
        if (this.getSqlExpr() != null) {
          sqlAggregateExpr.replace(arg, this.getSqlExpr());
          this.sqlExpr = null;
        }
      });

      this.inAggFunc = false;
      aggMapper.putIfAbsent(sqlAggregateExpr, selectItems.size());
      SQLSelectItem sqlSelectItem = new SQLSelectItemWrapper();
      String colName = "sel_agg_" + selectItems.size();
      sqlSelectItem.setAlias(colName);
      sqlSelectItem.setExpr(sqlAggregateExpr);
      selectItems.add(sqlSelectItem);
      return;
    }

    if (astNode instanceof SQLName) {
      visitHandler((SQLName) astNode);
      return;
    }

    this.sqlExpr = (SQLExpr) astNode;
  }

  private void visitHandler(SQLName sqlName) {
    boolean resolveFieldsFirst = true;
    if (this.inAggFunc || (orderBy && inExpr)) {
      resolveFieldsFirst = false;
    }

    if (!this.inAggFunc && !orderBy && this.groupByCols != null) {
      for (SQLExpr item : this.groupByCols) {
        if (item instanceof SQLName && (AnalyzerUtil.matchCol(sqlName, item) || AnalyzerUtil.matchCol(item, sqlName))) {
          resolveFieldsFirst = false;
          break;
        }
      }
    }

    int index;
    if (resolveFieldsFirst) {
      index = AnalyzerUtil.resolveFromSelectFields(this.selectItems, sqlName, false);
      //TODO has window function return
//      if (index != -1 && this.clause == ExpressionAnalyzer.ClauseType.HAVING) {
//        SQLSelectItemWrapper sqlSelectItemWarp = new SQLSelectItemWrapper();
//        sqlSelectItemWarp.setExpr(sqlName);
//        sqlSelectItemWarp.setAlias(sqlName.getSimpleName());
//        selectItems.add(sqlSelectItemWarp);
//      }
      if (index == -1) {
        if (this.orderBy) {
          index = resolveFromSchema(sqlName, this.operator.getSchema());
        } else {
          //TODO never execute ?
          index = AnalyzerUtil.resolveFromSelectFields(this.selectItems, sqlName, true);
        }
      }
    } else {
      index = resolveFromSchema(sqlName, this.operator.getSchema());
      if (index == -1 && this.clause != ExpressionAnalyzer.ClauseType.WINDOW) {
        index = AnalyzerUtil.resolveFromSelectFields(this.selectItems, sqlName, false);
        this.sqlExpr = sqlName;
      }
    }

    if (index == -1) {
      // TODO If we can't find it any where, it may be a correlated(subquery) columns.
      throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_BAD_FIELD_ERROR, sqlName.getSimpleName(),
              this.clause.getValue());
    }

    if (this.inAggFunc) {
      this.sqlExpr = selectItems.get(index).getExpr();
    }
    if (this.colMapper != null) {
      this.colMapper.put(sqlName, index);
    }
    this.sqlExpr = sqlName;
  }

  private int resolveFromSchema(SQLName sqlName, Schema schema) {
    ColumnExpr column = schema.getColumn(sqlName);

    if (column == null) {
      return -1;
    }

    SQLIdentifierExpr newCol = new SQLIdentifierExpr();
    newCol.setName(column.getOriCol());

    for (int i = 0; i < this.selectItems.size(); i++) {
      SQLSelectItem sqlSelectItem = selectItems.get(i);
      SQLExpr expr = sqlSelectItem.getExpr();

      if (((expr instanceof SQLIdentifierExpr) || (expr instanceof SQLPropertyExpr)) && AnalyzerUtil.matchCol(expr, newCol)) {
        return i;
      }
    }

    SQLSelectItemWrapper newSelectItem = new SQLSelectItemWrapper();
    newSelectItem.setExpr(newCol);
    newSelectItem.setAlias(newCol.getName());

    this.selectItems.add(newSelectItem);

    return this.selectItems.size() - 1;
  }

  public void setInExpr(boolean inExpr) {
    this.inExpr = inExpr;
  }

  public void setOrderBy(boolean orderBy) {
    this.orderBy = orderBy;
  }

  public RelOperator getOperator() {
    return operator;
  }

  public void setOperator(RelOperator operator) {
    this.operator = operator;
  }

  public void setClause(ExpressionAnalyzer.ClauseType clause) {
    this.clause = clause;
  }

  public void setSelectItems(List<SQLSelectItem> selectItems) {
    this.selectItems = selectItems;
  }

  public Map<SQLAggregateExpr, Integer> getAggMapper() {
    return aggMapper;
  }

  public void setAggMapper(Map<SQLAggregateExpr, Integer> aggMapper) {
    this.aggMapper = aggMapper;
  }

  public SQLExpr getSqlExpr() {
    return sqlExpr;
  }

  public void setGroupByCols(List<SQLExpr> groupByCols) {
    this.groupByCols = groupByCols;
  }

  /**
   * @version V1.0
   */
  public static class SQLSelectItemWrapper extends SQLSelectItem {
  }
}










