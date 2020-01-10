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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import io.jimdb.core.context.StatementContext;
import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.exception.JimException;
import io.jimdb.core.expression.Assignment;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.ExpressionType;
import io.jimdb.core.expression.ExpressionUtil;
import io.jimdb.core.expression.RowValueAccessor;
import io.jimdb.core.expression.Schema;
import io.jimdb.core.expression.TableAccessPath;
import io.jimdb.core.expression.ValueExpr;
import io.jimdb.core.expression.aggregate.AggregateExpr;
import io.jimdb.core.expression.aggregate.AggregateType;
import io.jimdb.core.expression.functions.builtin.cast.CastFuncBuilder;
import io.jimdb.core.model.meta.Column;
import io.jimdb.core.model.meta.Index;
import io.jimdb.core.model.meta.Table;
import io.jimdb.sql.operator.Aggregation;
import io.jimdb.sql.operator.DualTable;
import io.jimdb.sql.operator.Limit;
import io.jimdb.sql.operator.Operator;
import io.jimdb.sql.operator.Order;
import io.jimdb.sql.operator.Projection;
import io.jimdb.sql.operator.RelOperator;
import io.jimdb.sql.operator.Selection;
import io.jimdb.sql.operator.TableSource;
import io.jimdb.sql.operator.Update;
import io.jimdb.sql.optimizer.IndexHint;
import io.jimdb.sql.optimizer.OptimizeFlag;
import io.jimdb.core.values.Value;

import org.apache.commons.lang3.StringUtils;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLHint;
import com.alibaba.druid.sql.ast.SQLLimit;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.SQLSetQuantifier;
import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.druid.sql.ast.expr.SQLAggregateOption;
import com.alibaba.druid.sql.ast.expr.SQLAllColumnExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLBooleanExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLNullExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.expr.SQLUnaryExpr;
import com.alibaba.druid.sql.ast.expr.SQLUnaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLValuableExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelectGroupByClause;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLSubqueryTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.ast.statement.SQLUnionQueryTableSource;
import com.alibaba.druid.sql.ast.statement.SQLUpdateSetItem;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlForceIndexHint;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlIgnoreIndexHint;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlIndexHint;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlIndexHintImpl;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlUseIndexHint;
import com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlCharExpr;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.util.function.Tuple2;

/**
 * @version V1.0
 */
public abstract class StatementAnalyzer extends ExpressionAnalyzer {
  protected Operator resultOperator;
  protected int optimizationFlag = OptimizeFlag.PRUNCOLUMNS;

  protected abstract ExpressionAnalyzer buildExpressionAnalyzer();

  public Operator getResultOperator() {
    return resultOperator;
  }

  @Override
  public void reset() {
    super.reset();
    this.resultOperator = null;
    this.optimizationFlag = OptimizeFlag.PRUNCOLUMNS;
  }

  public int getOptimizationFlag() {
    return optimizationFlag;
  }

  @Override
  public boolean visit(SQLSelectStatement stmt) {
    this.resultOperator = KeyGetAnalyzer.analyze(session, stmt.getSelect().getQueryBlock(), AnalyzerType.SELECT);
    if (this.resultOperator == null) {
      this.resultOperator = this.analyzeSelect(stmt.getSelect().getQueryBlock(), AnalyzerType.SELECT);
    }
    return false;
  }

  protected Operator analyzeUpdate(RelOperator selectOp, List<SQLUpdateSetItem> items, SQLTableSource tableSource)
          throws JimException {
    this.clause = ClauseType.FIELDLIST;
    Table table = AnalyzerUtil.resolveTable(session, tableSource);

    ColumnExpr column;
    List<Assignment> updItems = new ArrayList<>(items.size());
    for (SQLUpdateSetItem item : items) {
      column = selectOp.getSchema().getColumn((SQLName) item.getColumn());
      if (column == null) {
        throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_BAD_FIELD_ERROR, ((SQLName) item.getColumn())
                .getSimpleName(), "update field list");
      }

      Tuple2<Optional<Expression>, RelOperator> rewriteExpr = this.analyzeExpression(selectOp, item.getValue(), null,
              false, null);
      Expression itemExpr = rewriteExpr.getT1().get();
      selectOp = rewriteExpr.getT2();
      itemExpr = CastFuncBuilder.build(session, column.getResultType(), itemExpr);
      updItems.add(new Assignment(column, itemExpr));
    }

    Update update = new Update(table, updItems.toArray(new Assignment[0]));
    update.setSchema(selectOp.getSchema());
    update.setSelect(selectOp);
    return update;
  }

  protected RelOperator analyzeSelect(final SQLSelectQueryBlock select, AnalyzerType analyzerType) throws JimException {
    RelOperator result;

    if (select.getFrom() == null) {
      result = this.createDualTable();
    } else {
      result = this.analyzeTableSource(select.getFrom(), analyzerType);
    }

    final List<SQLSelectItem> selectItems = this.expandStar(result, select.getSelectList());
    List<Expression> groupExprs = Collections.emptyList();
    if (select.getGroupBy() != null) {
      groupExprs = new ArrayList<>(select.getGroupBy().getItems().size());
      result = this.analyzeGroupBy(result, selectItems, select.getGroupBy(), groupExprs);
    }

    final Map<SQLAggregateExpr, Integer> orderMapper = new HashMap<>(4);
    final Map<SQLAggregateExpr, Integer> havingMapper = new HashMap<>(4);
    this.analyzeHavingAndOrder(result, select, selectItems, orderMapper, havingMapper);
    if (select.getWhere() != null) {
      result = this.analyzeFilter(result, select.getWhere(), null);
    }

    final Map<SQLAggregateExpr, Integer> aggMapper = new HashMap<>(4);
    if (hasAggregate(select)) {
      result = this.analyzeAggregate(result, selectItems, groupExprs, aggMapper);
    }

    result = this.analyzeProjection(result, selectItems, aggMapper);
    if (select.getGroupBy() != null && select.getGroupBy().getHaving() != null) {
      result = this.analyzeFilter(result, select.getGroupBy().getHaving(), havingMapper);
    }

    if (select.getDistionOption() == SQLSetQuantifier.DISTINCT) {
      result = this.analyzeDistinct(selectItems, result);
    }

    if (select.getOrderBy() != null) {
      result = this.analyzeOrder(result, select.getOrderBy(), orderMapper);
    }
    if (select.getLimit() != null) {
      result = this.analyzeLimit(result, select.getLimit());
    }

    return removeAuxiliary(selectItems, result);
  }

  protected RelOperator createDualTable() {
    return new DualTable(1);
  }

  protected RelOperator analyzeTableSource(SQLTableSource tableSource, AnalyzerType analyzerType) throws JimException {
    if (tableSource instanceof SQLExprTableSource) {
      if ("dual".equalsIgnoreCase(((SQLExprTableSource) tableSource).getName().getSimpleName())) {
        return this.createDualTable();
      }
      return this.analyzeExprTableSource((SQLExprTableSource) tableSource, analyzerType);
    }

    if (tableSource instanceof SQLJoinTableSource) {
      throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_NOT_SUPPORTED_YET, "join query");
    }
    if (tableSource instanceof SQLUnionQueryTableSource) {
      throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_NOT_SUPPORTED_YET, "union query");
    }
    if (tableSource instanceof SQLSubqueryTableSource) {
      throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_NOT_SUPPORTED_YET, "subquery");
    }
    throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_NOT_SUPPORTED_YET, "unknown query");
  }

  protected TableSource analyzeExprTableSource(SQLExprTableSource sqlTableSource, AnalyzerType analyzerType) {
    if (sqlTableSource.getPartitions() != null && sqlTableSource.getPartitions().size() > 0) {
      throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_NOT_SUPPORTED_YET, "partition table");
    }

    String catalog = sqlTableSource.getSchema();
    if (StringUtils.isBlank(catalog)) {
      catalog = session.getVarContext().getDefaultCatalog();
    }
    String tblName = sqlTableSource.getName().getSimpleName();
    Table table = session.getTxnContext().getMetaData().getTable(catalog, tblName);

    if (table == null) {
      throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_NO_SUCH_TABLE, catalog, tblName);
    }

    final String aliasTable;
    if (StringUtils.isNotBlank(sqlTableSource.getAlias())) {
      aliasTable = sqlTableSource.getAlias();
    } else {
      aliasTable = table.getName();
    }

    final List<IndexHint> hints = this.analyzeHints(sqlTableSource.getHints());
    final Column[] columns = analyzerType == AnalyzerType.SELECT ? table.getReadableColumns() : table.getWritableColumns();
    List<ColumnExpr> columnExprs = new ArrayList<>(columns.length);
    Arrays.stream(columns).forEach(column -> columnExprs.add(new ColumnExpr(session.allocColumnID(), column)
            .setAliasTable(aliasTable)));
    final Schema newSchema = new Schema(columnExprs);
    session.getStmtContext().addRetrievedTableAndSchema(table, newSchema);

    final List<TableAccessPath> tableAccessPaths = getTableAccessPaths(hints, table, newSchema, analyzerType);
    return new TableSource(catalog, aliasTable, table, hints, columns, tableAccessPaths, newSchema);
  }

  protected List<IndexHint> analyzeHints(List<SQLHint> hints) {
    if (hints == null || hints.isEmpty()) {
      return null;
    }

    List<IndexHint> indexHints = new ArrayList<>(hints.size());
    for (SQLHint sqlHint : hints) {
      if (!(sqlHint instanceof MySqlIndexHintImpl)) {
        continue;
      }
      MySqlIndexHintImpl hint = (MySqlIndexHintImpl) sqlHint;
      List<SQLName> indexList = hint.getIndexList();
      List<String> indexNames = new ArrayList<>(indexList.size());
      indexList.forEach(index -> indexNames.add(index.getSimpleName()));
      IndexHint.HintScope hintScope = IndexHint.HintScope.SCAN_INDEX_HINT;
      MySqlIndexHint.Option option = hint.getOption();
      if (option != null) {
        switch (option) {
          case JOIN:
            hintScope = IndexHint.HintScope.JOIN_INDEX_HINT;
            break;
          case ORDER_BY:
            hintScope = IndexHint.HintScope.ORDER_BY_INDEX_HINT;
            break;
          case GROUP_BY:
            hintScope = IndexHint.HintScope.GROUP_BY_INDEX_HINT;
            break;
          default:
            break;
        }
      }

      IndexHint.HintType hintType = null;
      if (hint instanceof MySqlUseIndexHint) {
        hintType = IndexHint.HintType.USE_INDEX_HINT;
      } else if (hint instanceof MySqlIgnoreIndexHint) {
        hintType = IndexHint.HintType.IGNORE_INDEX_HINT;
      } else if (hint instanceof MySqlForceIndexHint) {
        hintType = IndexHint.HintType.FORCE_INDEX_HINT;
      }

      indexHints.add(new IndexHint(indexNames, hintType, hintScope));
    }

    return indexHints;
  }

  protected List<SQLSelectItem> expandStar(final RelOperator op, final List<SQLSelectItem> selectItems) throws
          JimException {
    List<ColumnExpr> columnExps = op.getSchema().getColumns();
    List<SQLSelectItem> resultItems = new ArrayList<>(columnExps.size());

    for (SQLSelectItem item : selectItems) {
      SQLExpr sqlExpr = item.getExpr();
      if (sqlExpr instanceof SQLAllColumnExpr) {
        // select * from app
        for (ColumnExpr column : columnExps) {
          SQLIdentifierExpr sqlIdentifierExpr = new SQLIdentifierExpr(column.getOriCol());
          resultItems.add(new SQLSelectItem(sqlIdentifierExpr));
        }
      } else if (sqlExpr instanceof SQLPropertyExpr) {
        // select a.* from app a
        SQLPropertyExpr propertyExpr = (SQLPropertyExpr) sqlExpr;
        if ("*".equals(propertyExpr.getName())) {
          for (ColumnExpr column : columnExps) {
            SQLPropertyExpr sqlPropertyExpr = new SQLPropertyExpr(propertyExpr.getOwner(), column.getOriCol());
            resultItems.add(new SQLSelectItem(sqlPropertyExpr));
          }
        } else {
          resultItems.add(item);
        }
      } else {
        resultItems.add(item);
      }
    }

    return resultItems;
  }

  protected RelOperator analyzeProjection(final RelOperator op, final List<SQLSelectItem> selectItems,
                                          final Map<SQLAggregateExpr, Integer> aggMapper) throws JimException {

    this.optimizationFlag |= OptimizeFlag.ELIMINATEPROJECTION;
    this.clause = ClauseType.FIELDLIST;
    List<ColumnExpr> columns = new ArrayList<>(selectItems.size());
    List<Expression> exps = new ArrayList<>(selectItems.size());
    RelOperator operator = op;
    for (SQLSelectItem item : selectItems) {
      Tuple2<Optional<Expression>, RelOperator> newExp = analyzeExpression(operator, item.getExpr(), aggMapper, true,
              null);
      operator = newExp.getT2();
      newExp.getT1().ifPresent(expression -> {
        exps.add(expression.clone());
        ColumnExpr column = buildColumnByExpression(item, expression);
        columns.add(column);
      });
    }

    return new Projection(exps.toArray(new Expression[0]), new Schema(columns), operator);
  }

  protected ColumnExpr buildColumnByExpression(SQLSelectItem sqlItem, Expression expr) {
    ColumnExpr result = new ColumnExpr(session.allocColumnID());
    result.setResultType(expr.getResultType());
    if (expr.getExprType() == ExpressionType.COLUMN) {
      this.buildProjectionName(sqlItem, (ColumnExpr) expr, result);
    } else if (StringUtils.isNotBlank(sqlItem.getAlias())) {
      result.setAliasCol(sqlItem.getAlias());
    } else {
      this.buildProjectionName(sqlItem, result);
    }
    return result;
  }

  protected void buildProjectionName(SQLSelectItem sqlItem, ColumnExpr outExpr) {
    SQLExpr sqlExpr = this.getUnaryPlus(sqlItem.getExpr());
    if (!(sqlExpr instanceof SQLValuableExpr)) {
      outExpr.setAliasCol(sqlExpr.toString());
      return;
    }

    final String aliasCol;
    final Class clazz = sqlExpr.getClass();
    if (clazz == MySqlCharExpr.class) {
      aliasCol = ((MySqlCharExpr) sqlExpr).getText();
    } else if (clazz == SQLNullExpr.class) {
      aliasCol = "NULL";
    } else if (clazz == SQLBooleanExpr.class) {
      aliasCol = ((SQLBooleanExpr) sqlExpr).getValue() ? "TRUE" : "FALSE";
    } else {
      aliasCol = String.valueOf(((SQLValuableExpr) sqlExpr).getValue());
    }
    outExpr.setAliasCol(aliasCol);
  }

  protected void buildProjectionName(SQLSelectItem sqlItem, ColumnExpr columnExpr, ColumnExpr outExpr) {
    String oriCol = null;
    String aliasCol = null;
    String aliasTable = null;
    String dbName = null;

    SQLExpr sqlExpr = this.getUnaryPlus(sqlItem.getExpr());
    if (sqlExpr instanceof SQLIdentifierExpr) {
      oriCol = ((SQLIdentifierExpr) sqlExpr).getSimpleName();
    } else if (sqlExpr instanceof SQLPropertyExpr) {
      oriCol = ((SQLPropertyExpr) sqlExpr).getSimpleName();
      SQLExpr ownerExpr = ((SQLPropertyExpr) sqlExpr).getOwner();
      if (ownerExpr instanceof SQLIdentifierExpr) {
        aliasTable = ((SQLIdentifierExpr) ownerExpr).getSimpleName();
      } else {
        aliasTable = ((SQLPropertyExpr) ownerExpr).getSimpleName();
        dbName = ((SQLPropertyExpr) ownerExpr).getOwnernName();
      }
    } else {
      oriCol = sqlItem.toString();
    }

    if (StringUtils.isBlank(sqlItem.getAlias())) {
      aliasCol = oriCol;
    } else {
      aliasCol = sqlItem.getAlias();
    }
    if (StringUtils.isBlank(aliasTable)) {
      aliasTable = columnExpr.getAliasTable();
    }
    if (StringUtils.isBlank(dbName)) {
      dbName = columnExpr.getCatalog();
    }

    outExpr.setId(columnExpr.getId());
    outExpr.setOriCol(oriCol);
    outExpr.setAliasCol(aliasCol);
    outExpr.setOriTable(columnExpr.getOriTable());
    outExpr.setAliasTable(aliasTable);
    outExpr.setCatalog(dbName);
  }

  protected SQLExpr getUnaryPlus(SQLExpr sqlExpr) {
    if ((sqlExpr instanceof SQLUnaryExpr) && ((SQLUnaryExpr) sqlExpr).getOperator() == SQLUnaryOperator.Plus) {
      return getUnaryPlus(((SQLUnaryExpr) sqlExpr).getExpr());
    }
    return sqlExpr;
  }

  protected RelOperator analyzeFilter(final RelOperator op, final SQLExpr where, final Map<SQLAggregateExpr, Integer> havingMapper) throws JimException {
    RelOperator operator = op;
    this.optimizationFlag |= OptimizeFlag.PREDICATEPUSHDOWN;
    if (this.clause != ClauseType.HAVING) {
      this.clause = ClauseType.WHERE;
    }
    List<SQLExpr> exprList = splitWhere(where);
    List<Expression> whereExpression = new ArrayList<>(exprList.size());

    for (SQLExpr sqlExpr : exprList) {
      Tuple2<Optional<Expression>, RelOperator> rewrite = analyzeExpression(operator, sqlExpr, havingMapper, false, null);
      operator = rewrite.getT2();
      if (!rewrite.getT1().isPresent()) {
        continue;
      }

      Expression expression = rewrite.getT1().get();
      List<Expression> items = ExpressionUtil.splitCNFItems(expression);
      for (Expression item : items) {
        if (item.getExprType() == ExpressionType.CONST) {
          ValueExpr constantExpr = (ValueExpr) item;
          if (constantExpr.getLazyExpr() == null) {
            Tuple2<Boolean, Boolean> result = ExpressionUtil.execBoolean(session, new RowValueAccessor(new Value[0]),
                    item);
            if (result.getT1()) {
              continue;
            }
            RelOperator dualTable = createDualTable();
            dualTable.setSchema(operator.getSchema());
            return dualTable;
          }
        }
        whereExpression.add(item);
      }
    }

    if (whereExpression.isEmpty()) {
      return operator;
    }

    return new Selection(whereExpression, operator);
  }

  protected boolean hasAggregate(final SQLSelectQueryBlock select) {
    if (select == null) {
      throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_NOT_SUPPORTED_YET, "select null");
    }
    if (select.getGroupBy() != null && !select.getGroupBy().getItems().isEmpty()) {
      return true;
    }
    for (SQLSelectItem selectItem : select.getSelectList()) {
      if (selectItem.getExpr() instanceof SQLAggregateExpr) {
        return true;
      }
      if (selectItem.getExpr() instanceof SQLBinaryOpExpr) {
        return hasAggregate(selectItem.getExpr());
      }
    }

    if (select.getGroupBy() != null) {
      List<SQLExpr> items = select.getGroupBy().getItems();
      for (SQLExpr group : items) {
        if (group instanceof SQLAggregateExpr) {
          return true;
        }
        if (group instanceof SQLBinaryOpExpr) {
          return hasAggregate(group);
        }
      }
    }

    if (select.getOrderBy() != null) {
      List<SQLSelectOrderByItem> items = select.getOrderBy().getItems();
      for (SQLSelectOrderByItem order : items) {
        if (order.getExpr() instanceof SQLAggregateExpr) {
          return true;
        }
        if (order.getExpr() instanceof SQLBinaryOpExpr) {
          return hasAggregate(order.getExpr());
        }
      }
    }

    return false;
  }

  private boolean hasAggregate(SQLExpr expr) {
    if (expr instanceof SQLBinaryOpExpr) {
      SQLBinaryOpExpr binaryOpExpr = (SQLBinaryOpExpr) expr;
      if (hasAggregate(binaryOpExpr.getLeft())) {
        return true;
      }
      if (hasAggregate(binaryOpExpr.getRight())) {
        return true;
      }
    }
    return expr instanceof SQLAggregateExpr;
  }

  protected RelOperator analyzeAggregate(RelOperator op, final List<SQLSelectItem> selectItems,
                                         final List<Expression> groupExprs, final Map<SQLAggregateExpr, Integer>
                                                 aggMapper) throws JimException {

    List<SQLAggregateExpr> orgAggFuncList = new ArrayList<>(selectItems.size());
    AggregateExprAnalyzer aggregateExprAnalyzer = new AggregateExprAnalyzer(orgAggFuncList, aggMapper);
    for (SQLSelectItem selectItem : selectItems) {
      selectItem.getExpr().accept(aggregateExprAnalyzer);
    }
//    if (orgAggFuncList.isEmpty()) {
//      return op;
//    }

    List<ColumnExpr> columnExprs = new ArrayList<>(selectItems.size());
    List<AggregateExpr> combinedAggregateExprs = new ArrayList<>(selectItems.size());
    HashMap<Integer, Integer> aggIndex = new HashMap<>();

    this.optimizationFlag = this.optimizationFlag
            | OptimizeFlag.BUILDKEYINFO | OptimizeFlag.PUSHDOWNAGG | OptimizeFlag.MAXMINELIMINATE
            | OptimizeFlag.PUSHDOWNTOPN | OptimizeFlag.PREDICATEPUSHDOWN
            | OptimizeFlag.ELIMINATEAGG | OptimizeFlag.ELIMINATEPROJECTION;

    for (int i = 0; i < orgAggFuncList.size(); i++) {
      List<Expression> newArgList = new ArrayList<>(orgAggFuncList.size());
      SQLAggregateExpr sqlAggregateExpr = orgAggFuncList.get(i);
      List<SQLExpr> arguments = sqlAggregateExpr.getArguments();
      for (SQLExpr sqlExpr : arguments) {
        Tuple2<Optional<Expression>, RelOperator> result = analyzeExpression(op, sqlExpr, null, true, null);
        op = result.getT2();
        result.getT1().ifPresent(newArgList::add);
      }

      boolean distinct = sqlAggregateExpr.getOption() == SQLAggregateOption.DISTINCT;
      AggregateExpr aggregateExpr = new AggregateExpr(session, sqlAggregateExpr.getMethodName(), newArgList.toArray(new Expression[0]), distinct);
      boolean combined = false;

      for (int k = 0; k < combinedAggregateExprs.size(); k++) {
        if (combinedAggregateExprs.get(k).equals(aggregateExpr)) {
          aggIndex.put(i, k);
          combined = true;
          break;
        }
      }

      if (!combined) {
        int pos = combinedAggregateExprs.size();
        aggIndex.put(i, pos);
        combinedAggregateExprs.add(aggregateExpr);
        String colName = "agg_col_" + pos;
        ColumnExpr columnExpr = new ColumnExpr(session.allocColumnID())
                .setOriCol(colName)
                .setInSubQuery(true);
        columnExpr.setResultType(aggregateExpr.getType());
        columnExprs.add(columnExpr);
      }
    }

    for (ColumnExpr columnExpr : op.getSchema().getColumns()) {
      AggregateExpr distinctExpr = new AggregateExpr(session, AggregateType.DISTINCT, new Expression[]{ columnExpr }, false);
      combinedAggregateExprs.add(distinctExpr);
      ColumnExpr newColExp = columnExpr.clone();
      newColExp.setResultType(distinctExpr.getType());
      columnExprs.add(newColExp);
    }

    aggMapper.replaceAll((k, v) -> aggIndex.get(v));

    final AggregateExpr[] aggregateExprs = combinedAggregateExprs.toArray(new AggregateExpr[0]);

    return new Aggregation(aggregateExprs, groupExprs.toArray(new Expression[0]), new Schema(columnExprs), op);
  }

  protected RelOperator analyzeDistinct(final List<SQLSelectItem> selectItems,
                                        final RelOperator op) throws JimException {
    this.optimizationFlag = this.optimizationFlag | OptimizeFlag.BUILDKEYINFO | OptimizeFlag.PUSHDOWNAGG;
    List<ColumnExpr> columns = op.getSchema().clone().getColumns();
    int newLen = getNewLenFromSelectItems(selectItems);
    if (newLen > 0) {
      columns = columns.subList(0, columns.size() - newLen);
    }

    final Expression[] groupByExprs = columns.toArray(new ColumnExpr[0]);

    List<ColumnExpr> childColumns = op.getSchema().getColumns();
    AggregateExpr[] distinctExprs = new AggregateExpr[childColumns.size()];
    for (int i = 0; i < childColumns.size(); i++) {
      ColumnExpr distinctColumn = childColumns.get(i);
      AggregateExpr distinctExpr = new AggregateExpr(session, AggregateType.DISTINCT, new Expression[]{ distinctColumn }, false);
      distinctExprs[i] = distinctExpr;
    }

    final Schema schema = op.getSchema().clone();
    List<ColumnExpr> resultCols = schema.getColumns();
    for (int i = 0; i < resultCols.size(); i++) {
      ColumnExpr col = resultCols.get(i);
      col.setResultType(distinctExprs[i].getType());
    }

    return new Aggregation(distinctExprs, groupByExprs, schema, op);
  }

  protected RelOperator analyzeOrder(final RelOperator op, final SQLOrderBy order, final Map<SQLAggregateExpr,
          Integer> orderMapper) throws JimException {
    // todo union
    this.clause = ClauseType.ORDER;
    List<SQLSelectOrderByItem> orderByItems = order.getItems();
    List<Order.OrderExpression> orderExpressions = new ArrayList<>(orderByItems.size());
    RelOperator operator = op;
    for (SQLSelectOrderByItem item : orderByItems) {
      Tuple2<Optional<Expression>, RelOperator> rewriteTuple = analyzeExpression(operator, item.getExpr(),
              orderMapper, true, null);
      operator = rewriteTuple.getT2();
      rewriteTuple.getT1().ifPresent(expression -> {
        orderExpressions.add(new Order.OrderExpression(expression, item.getType()));
      });
    }

    return new Order(orderExpressions.toArray(new Order.OrderExpression[0]), operator);
  }

  protected RelOperator analyzeLimit(final RelOperator op, SQLLimit limit) throws JimException {
    this.optimizationFlag |= OptimizeFlag.PUSHDOWNTOPN;
    long offset = AnalyzerUtil.resolveLimt(limit.getOffset());
    long count = AnalyzerUtil.resolveLimt(limit.getRowCount());
    long maxCnt = Long.MAX_VALUE - offset;

    if (count > maxCnt) {
      count = maxCnt;
    }
    if (offset + count == 0) {
      RelOperator dualTable = createDualTable();
      dualTable.setSchema(op.getSchema());
      return dualTable;
    }
    Limit limitOperator = new Limit(offset, count);
    limitOperator.setChildren(op);
    return limitOperator;
  }

  protected RelOperator analyzeGroupBy(final RelOperator op, final List<SQLSelectItem> selects,
                                       final SQLSelectGroupByClause group, final List<Expression> outExprs) throws JimException {

    this.clause = ClauseType.GROUP;
    RelOperator operator = op;
    List<SQLExpr> groupItems = group.getItems();
    GroupByAnalyzer groupByAnalyzer = new GroupByAnalyzer(selects, operator.getSchema());

    for (int i = 0; i < groupItems.size(); i++) {
      SQLExpr groupItem = groupItems.get(i);
      groupByAnalyzer.setInExpr(false);
      groupItem.accept(groupByAnalyzer);

      SQLObject outExpr = groupByAnalyzer.getOutExpr();
      if (!groupByAnalyzer.isParam()) {
        if (outExpr instanceof SQLExpr) {
          groupItems.set(i, (SQLExpr) outExpr);
        }
      }
      Tuple2<Optional<Expression>, RelOperator> expression = analyzeExpression(operator,
              outExpr, null, true, null);
      expression.getT1().ifPresent(outExprs::add);
      operator = expression.getT2();
    }

    return operator;
  }

  @SuppressFBWarnings({ "ACEM_ABSTRACT_CLASS_EMPTY_METHODS", "CE_CLASS_ENVY" })
  protected void analyzeHavingAndOrder(final RelOperator op, final SQLSelectQueryBlock select, List<SQLSelectItem> selectItems, final Map<SQLAggregateExpr, Integer> aggMapper,
                                       final Map<SQLAggregateExpr, Integer> havingMapper) throws JimException {

    OrderByAnalyzer orderByAnalyzer = new OrderByAnalyzer();
    orderByAnalyzer.setOperator(op);
    orderByAnalyzer.setSelectItems(selectItems);
    orderByAnalyzer.setAggMapper(aggMapper);

    if (select.getGroupBy() != null) {
      if (select.getGroupBy().getItems() != null && !select.getGroupBy().getItems().isEmpty()) {
        orderByAnalyzer.setGroupByCols(select.getGroupBy().getItems());
      }

      if (select.getGroupBy().getHaving() != null) {
        orderByAnalyzer.clause = ClauseType.HAVING;

        select.getGroupBy().getHaving().accept(orderByAnalyzer);
        if (orderByAnalyzer.getSqlExpr() == null) {
          //TODO throw exception
        }
      }
    }

    havingMapper.putAll(orderByAnalyzer.getAggMapper());
    orderByAnalyzer.getAggMapper().clear();

    orderByAnalyzer.setOrderBy(true);
    orderByAnalyzer.setInExpr(false);

    if (select.getOrderBy() != null) {
      orderByAnalyzer.setClause(ClauseType.ORDER);
      for (SQLSelectOrderByItem sqlSelectOrderByItem : select.getOrderBy().getItems()) {
        //TODO windows function

        sqlSelectOrderByItem.getExpr().accept(orderByAnalyzer);
        if (orderByAnalyzer.getSqlExpr() != null) {
          sqlSelectOrderByItem.setExpr(orderByAnalyzer.getSqlExpr());
        }
      }
    }
  }

  private int getNewLenFromSelectItems(List<SQLSelectItem> selectItems) {
    if (selectItems == null) {
      return 0;
    }
    int newLen = 0;
    for (SQLSelectItem item : selectItems) {
      if (item instanceof OrderByAnalyzer.SQLSelectItemWrapper) {
        newLen++;
      }
    }

    return newLen;
  }

  protected RelOperator removeAuxiliary(List<SQLSelectItem> selectItems, RelOperator relOperator) {
    int newLen = getNewLenFromSelectItems(selectItems);
    if (newLen == 0) {
      return relOperator;
    }

    List<ColumnExpr> originalCols = relOperator.getSchema().getColumns();
    List<ColumnExpr> expressions = originalCols.subList(0, originalCols.size() - newLen);

    List<ColumnExpr> columns = relOperator.getSchema().clone().getColumns();
    List<ColumnExpr> oldColumns = columns.subList(0, columns.size() - newLen);
    oldColumns.forEach(col -> col.setUid(session.allocColumnID()));
    return new Projection(expressions.toArray(new ColumnExpr[0]), new Schema(oldColumns), relOperator);
  }

  protected List<SQLExpr> splitWhere(SQLExpr sqlExpr) {
    List<SQLExpr> expressions = new ArrayList<>();
    if (sqlExpr instanceof SQLBinaryOpExpr) {
      SQLBinaryOpExpr binaryOpExpr = (SQLBinaryOpExpr) sqlExpr;
      if (binaryOpExpr.getOperator() == SQLBinaryOperator.BooleanAnd) {
        expressions.addAll(splitWhere(binaryOpExpr.getLeft()));
        expressions.addAll(splitWhere(binaryOpExpr.getRight()));
      } else {
        expressions.add(binaryOpExpr);
      }
    } else {
      expressions.add(sqlExpr);
    }
    return expressions;
  }

  private List<TableAccessPath> getTableAccessPaths(List<IndexHint> hints, Table table, Schema newSchema, AnalyzerType analyzerType) throws JimException {
    final Index[] tableIndexes;
    switch (analyzerType) {
      case WRITE:
        tableIndexes = table.getWritableIndices();
        break;
      case DELETE:
        tableIndexes = table.getDeletableIndices();
        break;
      default:
        tableIndexes = table.getReadableIndices();
    }
    List<TableAccessPath> publicPaths = new ArrayList<>(null == tableIndexes ? 1 : tableIndexes.length + 1);

    if (tableIndexes != null && tableIndexes.length > 0) {
      Arrays.stream(tableIndexes)
              .forEach(index -> publicPaths.add(new TableAccessPath(index, newSchema)));
    }

    List<TableAccessPath> availablePaths = new ArrayList<>(publicPaths.size());
    List<TableAccessPath> ignoredPaths = new ArrayList<>(publicPaths.size());
    boolean existScanHint = false;
    boolean existUseOrForceHint = false;
    if (hints != null && !hints.isEmpty()) {
      for (IndexHint hint : hints) {
        IndexHint.HintScope hintScope = hint.getHintScope();
        if (hintScope != IndexHint.HintScope.SCAN_INDEX_HINT) {
          continue;
        }

        existScanHint = true;
        List<String> indexNames = hint.getIndexNames();
        for (String indexName : indexNames) {
          TableAccessPath path = getPathByIndexName(publicPaths, indexName, table);
          if (path == null) {
            throw DBException.get(ErrorModule.META, ErrorCode.ER_KEY_COLUMN_DOES_NOT_EXISTS, indexName);
          }
          if (hint.getHintType() == IndexHint.HintType.IGNORE_INDEX_HINT) {
            ignoredPaths.add(path);
            continue;
          }

          existUseOrForceHint = true;
          availablePaths.add(path.setUseOrForceHint(true));
        }
      }
    }

    if (!(existScanHint && existUseOrForceHint)) {
      availablePaths = publicPaths;
    }

    return pruneIgnoredPaths(availablePaths, ignoredPaths, table);
  }

  private TableAccessPath getPathByIndexName(List<TableAccessPath> tableAccessPaths, String indexName, Table table) {
    TableAccessPath result = null;
    for (TableAccessPath path : tableAccessPaths) {
      if (path.isTablePath()) {
        result = path;
        continue;
      }

      if (indexName.equals(path.getIndex().getName())) {
        return path;
      }
    }

    if ("primary".equals(indexName) && table.getPrimary().length > 0) {
      return result;
    }

    return null;
  }

  private List<TableAccessPath> pruneIgnoredPaths(List<TableAccessPath> publicPaths, List<TableAccessPath>
          ignoredPaths, Table table) {
    if (ignoredPaths.isEmpty()) {
      return publicPaths;
    }
    List<TableAccessPath> resultPaths = new ArrayList<>(publicPaths.size());
    publicPaths.forEach(path -> {
      if (path.isTablePath() || getPathByIndexName(ignoredPaths, path.getIndex().getName(), table) == null) {
        resultPaths.add(path);
      }
    });

    return resultPaths;
  }

  protected Tuple2<Optional<Expression>, RelOperator> analyzeExpression(final RelOperator op, final SQLObject expr,
                                                                        final Map<SQLAggregateExpr, Integer> aggMapper,
                                                                        final boolean isScalar, final
                                                                        Function<SQLObject, SQLObject> preFunc)
          throws JimException {
    final StatementContext stmtCtx = session.getStmtContext();
    try {
      final ExpressionAnalyzer exprAnalyzer = (ExpressionAnalyzer) stmtCtx.retainAnalyzer
              (this::buildExpressionAnalyzer);
      exprAnalyzer.isExprAnalyzer = true;
      exprAnalyzer.expr = expr;
      exprAnalyzer.aggMapper = aggMapper;
      exprAnalyzer.isScalar = isScalar;
      exprAnalyzer.preFunc = preFunc;
      exprAnalyzer.session = session;
      exprAnalyzer.clause = this.clause;
      exprAnalyzer.setOperator(op);
      return exprAnalyzer.doAnalyze();
    } finally {
      stmtCtx.releaseAnalyzer();
    }
  }
}
