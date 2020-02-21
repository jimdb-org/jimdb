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
import java.util.List;
import java.util.Optional;

import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.exception.JimException;
import io.jimdb.common.utils.lang.StringUtil;
import io.jimdb.core.Session;
import io.jimdb.core.context.StatementContext;
import io.jimdb.core.expression.Assignment;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.ExpressionUtil;
import io.jimdb.core.expression.KeyColumn;
import io.jimdb.core.expression.Schema;
import io.jimdb.core.expression.ValueExpr;
import io.jimdb.core.model.meta.Column;
import io.jimdb.core.model.meta.Index;
import io.jimdb.core.model.meta.Table;
import io.jimdb.core.values.NullValue;
import io.jimdb.sql.operator.DualTable;
import io.jimdb.sql.operator.Insert;
import io.jimdb.sql.operator.Operator;
import io.jimdb.sql.operator.RelOperator;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLDefaultExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.util.function.Tuple2;

/**
 * @version V1.0
 */
public final class InsertAnalyzer {
  private InsertAnalyzer() {
  }

  public static Operator analyzeInsert(StatementAnalyzer analyzer, SQLInsertStatement insert, List<SQLExpr> dupUpdates) throws JimException {
    Session session = analyzer.session;
    String tblName = null;
    String dbName = session.getVarContext().getDefaultCatalog();
    SQLExpr tableSource = insert.getTableSource() == null ? null : insert.getTableSource().getExpr();
    if (tableSource != null) {
      if (tableSource instanceof SQLIdentifierExpr) {
        tblName = ((SQLIdentifierExpr) tableSource).getName();
      } else if (tableSource instanceof SQLPropertyExpr) {
        tblName = ((SQLPropertyExpr) tableSource).getSimpleName();
        dbName = ((SQLPropertyExpr) tableSource).getOwnernName();
      }
    }
    if (StringUtil.isBlank(dbName)) {
      throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_NO_DB_ERROR);
    }
    if (tblName == null) {
      throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_NO_SUCH_TABLE, dbName, "");
    }
    Table table;
    try {
      table = session.getTxnContext().getMetaData().getTable(dbName, tblName);
    } catch (JimException ex) {
      if (ex.getCode() == ErrorCode.ER_BAD_TABLE_ERROR || ex.getCode() == ErrorCode.ER_BAD_DB_ERROR) {
        throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_NO_SUCH_TABLE, ex, dbName, tblName);
      }
      throw ex;
    }
    Schema schema = buildSchema(session, dbName, table);
    Insert insertop = new Insert(table, schema);
    DualTable mockTable = new DualTable(0);
    mockTable.setSchema(schema);
    analyzeInsertValues(analyzer, insert, insertop, table, mockTable);

    if (dupUpdates != null && dupUpdates.size() > 0) {
      mockTable.setSchema(insertop.getSchema4Dup());
      analyzeDuplicate(analyzer, insertop, dupUpdates, table, mockTable);
    }
    insertop.resolveOffset();
    return insertop;
  }

  private static Schema buildSchema(Session session, String dbName, Table table) {
    List<ColumnExpr> columns = ExpressionUtil.buildColumnExpr(session, dbName, table.getName(), table.getWritableColumns());
    Schema schema = new Schema(columns);

    for (Index index : table.getWritableIndices()) {
      if (!index.isUnique()) {
        continue;
      }

      boolean flag = true;
      KeyColumn key = new KeyColumn(index.getColumns().length);
      for (Column idxCol : index.getColumns()) {
        int i = -1;
        boolean found = false;
        for (Column tblCol : table.getWritableColumns()) {
          i++;
          if (idxCol.getId().equals(tblCol.getId())) {
            if (tblCol.getType().getNotNull()) {
              key.addColumnExpr(columns.get(i));
              found = true;
            }
            break;
          }
        }

        if (!found) {
          flag = false;
          break;
        }
      }

      if (flag) {
        schema.addKeyColumn(key);
      }
    }

    return schema;
  }

  private static void analyzeInsertValues(final StatementAnalyzer analyzer, final SQLInsertStatement insert,
                                          final Insert insertop, final Table insertTable, final DualTable mockTable) {
    List<SQLInsertStatement.ValuesClause> rowValues = insert.getValuesList();
    if (rowValues == null || rowValues.isEmpty()) {
      throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_WRONG_VALUE_COUNT_ON_ROW, "1");
    }

    Column[] insertColumns = getAndCheckInsertColumns(insert, insertTable);
    int numRows = rowValues.size();
    int numColumns = insertColumns.length;
    List<SQLExpr> colValues;
    List<Expression[]> result = new ArrayList<>(numRows);
    for (int i = 0; i < numRows; i++) {
      colValues = rowValues.get(i).getValues();
      if (colValues == null || colValues.size() != numColumns) {
        throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_WRONG_VALUE_COUNT_ON_ROW, String.valueOf(i + 1));
      }

      Expression[] colExprs = new Expression[insertColumns.length];
      for (int j = 0, k = 0; j < insertColumns.length; j++) {
        final Column insertColumn = insertColumns[j];
        SQLExpr sqlExpr = colValues.get(k++);
        if (sqlExpr.getClass() == SQLDefaultExpr.class) {
          colExprs[j] = new ValueExpr(insertColumn.getDefaultValue(), insertColumn.getType());
          continue;
        }
        Tuple2<Optional<Expression>, RelOperator> rewriteValue = analyzer.analyzeExpression(mockTable, sqlExpr,
                null, true, sqlObj -> checkRefColumn(insertop, sqlObj));
        colExprs[j] = rewriteValue.getT1().orElseGet(() -> new ValueExpr(NullValue.getInstance(), insertColumn.getType()));
      }
      result.add(colExprs);
    }

    insertop.setColumns(insertColumns);
    insertop.setValues(result);
    insertop.setSchema4Dup(insertop.getSchema());
  }

  private static SQLObject checkRefColumn(Insert insertop, SQLObject sqlObj) {
    if (insertop.isHasRefColumn()) {
      return sqlObj;
    }

    if (sqlObj instanceof SQLIdentifierExpr || sqlObj instanceof SQLBinaryOpExpr || sqlObj instanceof SQLPropertyExpr) {
      insertop.setHasRefColumn(true);
    }
    return sqlObj;
  }

  private static Column[] getAndCheckInsertColumns(SQLInsertStatement insert, Table table) {
    List<SQLExpr> insertCols = insert.getColumns();
    if (insertCols == null || insertCols.isEmpty()) {
      return table.getWritableColumns();
    }

    String colName;
    int size = insertCols.size();
    Column[] result = new Column[size];
    for (int i = 0; i < size; i++) {
      colName = ((SQLName) insertCols.get(i)).getSimpleName();
      result[i] = table.getWritableColumn(colName);
    }
    return result;
  }

  private static void analyzeDuplicate(StatementAnalyzer analyzer, Insert insertop, List<SQLExpr> dupUpdates, Table insertTable, DualTable mockTable) {
    List<Assignment> dupAssigns = new ArrayList<>(dupUpdates.size());
    for (SQLExpr dupExpr : dupUpdates) {
      if (!(dupExpr instanceof SQLBinaryOpExpr)) {
        continue;
      }
      SQLExpr left = ((SQLBinaryOpExpr) dupExpr).getLeft();
      SQLExpr right = ((SQLBinaryOpExpr) dupExpr).getRight();
      if (!(left instanceof SQLName)) {
        continue;
      }

      SQLName assignColumn = (SQLName) left;
      ColumnExpr columnExpr = insertop.getSchema().getColumn(assignColumn);
      if (columnExpr == null) {
        throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_BAD_FIELD_ERROR, assignColumn.getSimpleName(), "field list");
      }
      Column column = insertTable.getWritableColumn(assignColumn.getSimpleName());
      if (column.isPrimary() || column.isAutoIncr()) {
        throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN, assignColumn.getSimpleName(), insertTable.getName());
      }

      Expression rewriteExpr = rewriteInsertDuplicate(analyzer, insertop, right, mockTable);
      dupAssigns.add(new Assignment(columnExpr, rewriteExpr));
    }
    insertop.setDuplicate(dupAssigns.toArray(new Assignment[0]));
  }

  @SuppressFBWarnings("CE_CLASS_ENVY")
  private static Expression rewriteInsertDuplicate(StatementAnalyzer analyzer, Insert insertop, SQLExpr assign, DualTable mockTable) {
    final StatementContext stmtCtx = analyzer.session.getStmtContext();
    try {
      final ExpressionAnalyzer exprAnalyzer = (ExpressionAnalyzer) stmtCtx.retainAnalyzer(analyzer::buildExpressionAnalyzer);
      exprAnalyzer.setExprAnalyzer(true);
      exprAnalyzer.setOperator(mockTable);
      exprAnalyzer.setSQLExpr(assign);
      exprAnalyzer.setAggMapper(null);
      exprAnalyzer.setScalar(true);
      exprAnalyzer.setPreFunc(null);
      exprAnalyzer.setSession(analyzer.session);
      exprAnalyzer.setClause(analyzer.getClause());
      exprAnalyzer.setInsertTable(insertop.getSchema());
      return exprAnalyzer.doAnalyze().getT1().get();
    } finally {
      stmtCtx.releaseAnalyzer();
    }
  }
}
