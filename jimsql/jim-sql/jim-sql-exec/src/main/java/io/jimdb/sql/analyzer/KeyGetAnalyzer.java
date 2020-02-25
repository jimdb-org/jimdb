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
import java.util.Collections;
import java.util.List;

import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.exception.JimException;
import io.jimdb.core.Session;
import io.jimdb.core.context.PreparedContext;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.Schema;
import io.jimdb.core.model.meta.Column;
import io.jimdb.core.model.meta.Index;
import io.jimdb.core.model.meta.Table;
import io.jimdb.core.model.privilege.PrivilegeType;
import io.jimdb.core.values.LongValue;
import io.jimdb.core.values.Value;
import io.jimdb.core.values.ValueConvertor;
import io.jimdb.sql.operator.KeyGet;

import org.apache.commons.lang3.StringUtils;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLLimit;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.expr.SQLAllColumnExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.expr.SQLValuableExpr;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;

import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

/**
 * @version V1.0
 */
public final class KeyGetAnalyzer {
  public static KeyGet analyze(Session session, SQLSelectQueryBlock queryBlock, AnalyzerType analyzerType) throws JimException {
    if (queryBlock.isForUpdate() || (queryBlock.getGroupBy() != null && queryBlock.getGroupBy().getHaving() != null)) {
      return null;
    }

    SQLTableSource tableSource = queryBlock.getFrom();
    if (tableSource == null || !(tableSource instanceof SQLExprTableSource)) {
      return null;
    }
    SQLExpr where = queryBlock.getWhere();
    if (where == null || !(where instanceof SQLBinaryOpExpr)) {
      return null;
    }

    SQLLimit limit = queryBlock.getLimit();
    if (limit != null) {
      if (limit.getOffset() instanceof SQLVariantRefExpr || limit.getRowCount() instanceof SQLVariantRefExpr) {
        return null;
      }

      PreparedContext context = session.getPreparedContext();
      long offset = getOffset(limit, context);
      long count = getCount(limit, context);
      if (offset > 0) {
        return null;
      }
      if (count == 0) {
        return null;
      }
    }

    Table table = AnalyzerUtil.resolveTable(session, tableSource);
    String aliasTable = table.getName();
    if (StringUtils.isNotBlank(queryBlock.getFrom().getAlias())) {
      aliasTable = queryBlock.getFrom().getAlias();
    }
    session.getStmtContext().addPrivilegeInfo(table.getCatalog().getName(), table.getName(), PrivilegeType.SELECT_PRIV);
    Schema schema = buildSchema(table, aliasTable, queryBlock.getSelectList(), analyzerType);
    if (schema == null) {
      return null;
    }
    List<ConditionExpr> conds = analyzeFilter(table, aliasTable, where, analyzerType);
    if (conds.isEmpty()) {
      return null;
    }

    List<Index> indexs = new ArrayList<>(4);
    List<Value[]> values = new ArrayList<>(8);
    final Index[] tblIndex;
    switch (analyzerType) {
      case WRITE:
        tblIndex = table.getWritableIndices();
        break;
      case DELETE:
        tblIndex = table.getDeletableIndices();
        break;
      default:
        tblIndex = table.getReadableIndices();
    }
    for (ConditionExpr cond : conds) {
      Index matchIndex = null;
      Value[] value = null;
      for (Index index : tblIndex) {
        if (!index.isPrimary() && !index.isUnique()) {
          continue;
        }

        value = analyzeIndex(session, index, cond.getConditions());
        if (value == null || value.length == 0) {
          continue;
        }
        matchIndex = index;
        break;
      }

      if (matchIndex == null) {
        return null;
      }
      indexs.add(matchIndex);
      values.add(value);
    }

    return new KeyGet(indexs, values, schema);
  }

  static long getCount(SQLLimit limit, PreparedContext context) {
    long count;
    if (context.getParamValues() != null && limit.getRowCount() instanceof SQLVariantRefExpr) {
      SQLVariantRefExpr variantRefExpr = (SQLVariantRefExpr) limit.getRowCount();
      LongValue value = (LongValue) context.getParamValues()[variantRefExpr.getIndex()];
      count = value.getValue();
    } else {
      count = AnalyzerUtil.resolveLimt(limit.getRowCount());
    }
    return count;
  }

  static long getOffset(SQLLimit limit, PreparedContext context) {
    long offset;
    if (context.getParamValues() != null && limit.getOffset() instanceof SQLVariantRefExpr) {
      SQLVariantRefExpr variantRefExpr = (SQLVariantRefExpr) limit.getOffset();
      LongValue value = (LongValue) context.getParamValues()[variantRefExpr.getIndex()];
      offset = value.getValue();
    } else {
      offset = AnalyzerUtil.resolveLimt(limit.getOffset());
    }
    return offset;
  }

  protected static List<ConditionExpr> analyzeFilter(Table table, String aliasTable, SQLExpr expr, AnalyzerType analyzerType) {
    if (!(expr instanceof SQLBinaryOpExpr)) {
      return Collections.EMPTY_LIST;
    }

    SQLBinaryOpExpr opExpr = (SQLBinaryOpExpr) expr;
    if (opExpr.getOperator() == SQLBinaryOperator.BooleanAnd) {
      List<ConditionExpr> leftConds = analyzeFilter(table, aliasTable, opExpr.getLeft(), analyzerType);
      if (leftConds.isEmpty()) {
        return Collections.EMPTY_LIST;
      }
      List<ConditionExpr> rightConds = analyzeFilter(table, aliasTable, opExpr.getRight(), analyzerType);
      if (rightConds.isEmpty()) {
        return Collections.EMPTY_LIST;
      }

      ConditionExpr cond;
      List<ConditionExpr> conds = new ArrayList<>();
      for (ConditionExpr left : leftConds) {
        for (ConditionExpr right : rightConds) {
          cond = new ConditionExpr();
          cond.addAll(left);
          cond.addAll(right);
          conds.add(cond);
        }
      }
      return conds;
    }

    if (opExpr.getOperator() == SQLBinaryOperator.BooleanOr) {
      List<ConditionExpr> leftConds = analyzeFilter(table, aliasTable, opExpr.getLeft(), analyzerType);
      if (leftConds.isEmpty()) {
        return Collections.EMPTY_LIST;
      }
      List<ConditionExpr> rightConds = analyzeFilter(table, aliasTable, opExpr.getRight(), analyzerType);
      if (rightConds.isEmpty()) {
        return Collections.EMPTY_LIST;
      }

      leftConds.addAll(rightConds);
      return leftConds;
    }

    if (opExpr.getOperator() != SQLBinaryOperator.Equality) {
      return Collections.EMPTY_LIST;
    }

    SQLName colName;
    Column column;
    Value val = null;
    if (opExpr.getLeft() instanceof SQLName) {
      colName = (SQLName) opExpr.getLeft();
      if (opExpr.getRight() instanceof SQLValuableExpr) {
        val = AnalyzerUtil.resolveValueExpr(opExpr.getRight(), null);
      }
    } else if (opExpr.getRight() instanceof SQLName) {
      colName = (SQLName) opExpr.getRight();
      if (opExpr.getLeft() instanceof SQLValuableExpr) {
        val = AnalyzerUtil.resolveValueExpr(opExpr.getLeft(), null);
      }
    } else {
      return Collections.EMPTY_LIST;
    }

    if (val == null) {
      return Collections.EMPTY_LIST;
    }
    if (colName instanceof SQLPropertyExpr) {
      String owner = ((SQLPropertyExpr) colName).getOwnernName();
      if (owner != null && !(owner.equals(aliasTable))) {
        throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_SYSTEM_PARSE_ERROR, String.format("table alias '%s' not found.", owner));
      }
    }
    column = analyzerType == AnalyzerType.SELECT ? table.getReadableColumn(colName.getSimpleName()) : table.getWritableColumn(colName.getSimpleName());
    if (column == null) {
      throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_BAD_FIELD_ERROR, colName.getSimpleName(), aliasTable);
    }

    List<ConditionExpr> conds = new ArrayList<>();
    ConditionExpr cond = new ConditionExpr();
    cond.addCondition(column, val);
    conds.add(cond);
    return conds;
  }

  protected static Value[] analyzeIndex(Session session, Index index, List<Tuple2<Column, Value>> conditions) {
    final Column[] idxColumns = index.getColumns();
    if (idxColumns.length != conditions.size()) {
      return null;
    }

    Column column;
    Value[] result = new Value[conditions.size()];
    for (int i = 0; i < idxColumns.length; i++) {
      Value val = null;
      column = idxColumns[i];
      for (Tuple2<Column, Value> cond : conditions) {
        if (cond.getT1().getId().equals(column.getId())) {
          val = cond.getT2();
          break;
        }
      }

      if (val == null) {
        return null;
      }
      result[i] = ValueConvertor.convertType(session, val, column.getType());
    }
    return result;
  }

  protected static Schema buildSchema(Table table, String aliasTable, List<SQLSelectItem> selectList, AnalyzerType analyzerType) {
    Column[] tblColumns = analyzerType == AnalyzerType.SELECT ? table.getReadableColumns() : table.getWritableColumns();
    List<ColumnExpr> columns = new ArrayList<>(tblColumns.length);
    if (selectList.size() == 1 && selectList.get(0).getExpr() instanceof SQLAllColumnExpr) {
      for (Column col : tblColumns) {
        columns.add(buildColumn(table, col, aliasTable, col.getName()));
      }
      return new Schema(columns);
    }

    SQLExpr selectExpr;
    Column col;
    for (SQLSelectItem select : selectList) {
      selectExpr = select.getExpr();
      if (!(selectExpr instanceof SQLName)) {
        return null;
      }

      if (selectExpr instanceof SQLPropertyExpr) {
        String owner = ((SQLPropertyExpr) selectExpr).getOwnernName();
        if (owner != null && !(owner.equals(aliasTable))) {
          throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_SYSTEM_PARSE_ERROR, String.format("table alias '%s' not found.", owner));
        }
        if ("*".equals(((SQLPropertyExpr) selectExpr).getName())) {
          for (Column column : tblColumns) {
            columns.add(buildColumn(table, column, aliasTable, column.getName()));
          }
          continue;
        }
      }

      String colName = ((SQLName) selectExpr).getSimpleName();
      col = analyzerType == AnalyzerType.SELECT ? table.getReadableColumn(colName) : table.getWritableColumn(colName);
      if (col == null) {
        throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_BAD_FIELD_ERROR, ((SQLName) selectExpr).getSimpleName(), aliasTable);
      }

      String aliasColumn = select.getAlias();
      if (aliasColumn == null || aliasColumn.length() == 0) {
        aliasColumn = col.getName();
      }
      columns.add(buildColumn(table, col, aliasTable, aliasColumn));
    }

    return new Schema(columns);
  }

  protected static ColumnExpr buildColumn(Table table, Column column, String aliasTable, String aliasColumn) {
    ColumnExpr result = new ColumnExpr(column.getId().longValue(), column);
    result.setCatalog(table.getCatalog().getName());
    result.setOriTable(table.getName());
    result.setAliasTable(aliasTable);
    result.setAliasCol(aliasColumn);
    return result;
  }

  /**
   *
   */
  static final class ConditionExpr {
    private final List<Tuple2<Column, Value>> conditions;

    ConditionExpr() {
      this.conditions = new ArrayList<>(2);
    }

    public List<Tuple2<Column, Value>> getConditions() {
      return conditions;
    }

    void addCondition(final Column col, final Value val) {
      this.conditions.add(Tuples.of(col, val));
    }

    void addAll(final ConditionExpr expr) {
      this.conditions.addAll(expr.getConditions());
    }
  }
}
