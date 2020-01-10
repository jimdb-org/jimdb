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
package io.jimdb.sql.optimizer.logical;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import io.jimdb.core.Session;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.ExpressionType;
import io.jimdb.core.expression.ExpressionUtil;
import io.jimdb.core.expression.Schema;
import io.jimdb.core.expression.ValueExpr;
import io.jimdb.core.expression.aggregate.AggregateExpr;
import io.jimdb.core.model.meta.Column;
import io.jimdb.pb.Basepb;
import io.jimdb.sql.operator.Aggregation;
import io.jimdb.sql.operator.DualTable;
import io.jimdb.sql.operator.Order;
import io.jimdb.sql.operator.Projection;
import io.jimdb.sql.operator.RelOperator;
import io.jimdb.sql.operator.Selection;
import io.jimdb.sql.operator.TableSource;
import io.jimdb.sql.optimizer.OptimizationUtil;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Column pruning optimizer removes the unused table columns during query,
 * which reduces the IO cost of reading unneeded data from the data source.
 */
@SuppressFBWarnings("CFS_CONFUSING_FUNCTION_SEMANTICS")
public class ColumnPruneOptimizer implements IRuleOptimizer {

  @Override
  public RelOperator optimize(RelOperator relOperator, Session session) {

    // initial column list is from schema
    List<ColumnExpr> columnExprs = new ArrayList<>(relOperator.getSchema().getColumns());
    return relOperator.acceptVisitor(new ColumnPruningVisitor(columnExprs, session));
  }

  /**
   * Implementation class for column pruning visitor.
   */
  @SuppressFBWarnings({ "FCBL_FIELD_COULD_BE_LOCAL", "LII_LIST_INDEXED_ITERATING", "URF_UNREAD_FIELD" })
  private static class ColumnPruningVisitor extends LogicalOptimizationVisitor {

    //TODO add newList
    private List<ColumnExpr> parentColumns;
    private Session session;

    ColumnPruningVisitor(List<ColumnExpr> parentColumns, Session session) {
      this.parentColumns = parentColumns;
      this.session = session;
    }

    @Override
    public RelOperator visitOperator(Projection projection) {
      Schema schema = projection.getSchema();
      boolean[] usedIndex = findUsedPosition(parentColumns, schema);
      if (usedIndex == null) {
        return projection;
      }

      Expression[] currentExprs = projection.getExpressions();
      List<ColumnExpr> currentColumns = schema.getColumns();
      List<ColumnExpr> newColumns = new ArrayList<>(currentColumns.size());
      List<Expression> newExprs = new ArrayList<>();

      for (int i = 0; i < usedIndex.length; i++) {
        if (usedIndex[i] || OptimizationUtil.hasVarSet(currentExprs[i])) {
          newExprs.add(currentExprs[i]);
          newColumns.add(currentColumns.get(i));
        }
      }

      projection.setExpressions(newExprs.toArray(new Expression[newExprs.size()]));
      projection.setSchema(new Schema(newColumns));

      List<ColumnExpr> usedColumns = new ArrayList<>(newExprs.size());
      ExpressionUtil.extractColumns(usedColumns, newExprs, null);
      this.parentColumns = usedColumns;

      return visitChildren(projection);
    }

    @Override
    public RelOperator visitOperator(Selection selection) {
      List<Expression> conditions = selection.getConditions();
      ExpressionUtil.extractColumns(parentColumns, conditions, null);
      return visitChildren(selection);
    }

    @Override
    public RelOperator visitOperator(TableSource tableSource) {
      Schema schema = tableSource.getSchema();
      boolean[] usedIndex = findUsedPosition(parentColumns, schema);
      if (usedIndex == null) {
        return tableSource;
      }

      List<ColumnExpr> currentColumnExprs = schema.getColumns();
      List<ColumnExpr> newColumnExprs = new ArrayList<>(currentColumnExprs.size());

      Column[] currentColumns = tableSource.getColumns();
      List<Column> newColumns = new ArrayList<>();

      Column primaryKey = null;
      ColumnExpr primaryKeyExpr = null;

      for (int i = 0, j = 0; i < usedIndex.length; i++) {
        if (currentColumns[i].isPrimary()) {
          primaryKey = currentColumns[i];
          primaryKeyExpr = currentColumnExprs.get(i);
        }

        if (usedIndex[i]) {
          newColumnExprs.add(currentColumnExprs.get(i));
          newColumns.add(currentColumns[i]);
        }
      }
      tableSource.setColumns(newColumns.toArray(new Column[newColumns.size()]));
      tableSource.setSchema(new Schema(newColumnExprs));

      if (tableSource.getSchema().getColumns() == null || tableSource.getSchema().getColumns().isEmpty()) {
        newColumnExprs.add(primaryKeyExpr);
        Column[] tableSourceColumns = tableSource.getColumns();
        Column[] newCols = Arrays.copyOf(tableSourceColumns, tableSourceColumns.length + 1);
        newCols[newCols.length - 1] = primaryKey;
        tableSource.setColumns(newCols);

        currentColumnExprs.add(primaryKeyExpr);
        tableSource.setSchema(new Schema(newColumnExprs));
      }

      return tableSource;
    }

    @Override
    public RelOperator visitOperator(Aggregation aggregation) {
      Schema schema = aggregation.getSchema();
      boolean[] usedIndex = findUsedPosition(parentColumns, schema);
      if (usedIndex == null) {
        return aggregation;
      }

      List<ColumnExpr> currentColumns = schema.getColumns();
      List<ColumnExpr> newColumnExprs = new ArrayList<>(currentColumns.size());

      AggregateExpr[] currentAggs = aggregation.getAggregateExprs();
      List<AggregateExpr> newAggs = new ArrayList<>();

      for (int i = 0; i < usedIndex.length; i++) {
        if (usedIndex[i]) {
          newColumnExprs.add(currentColumns.get(i));
          newAggs.add(currentAggs[i]);
        }
      }
      aggregation.setAggregateExprs(newAggs.toArray(new AggregateExpr[newAggs.size()]));
      aggregation.setSchema(new Schema(newColumnExprs));

      List<ColumnExpr> usedColumns = new ArrayList<>();
      for (AggregateExpr agg : newAggs) {
        ExpressionUtil.extractColumns(usedColumns, Arrays.asList(agg.getArgs()), null);
      }

      List<Expression> newGroupByExprs = new ArrayList<>();
      if (aggregation.getGroupByExprs() != null && aggregation.getGroupByExprs().length > 0) {
        for (Expression expression : aggregation.getGroupByExprs()) {
          List<ColumnExpr> columnExprs = new ArrayList<>();
          ExpressionUtil.extractColumns(columnExprs, expression, null);
          if (!columnExprs.isEmpty()) {
            usedColumns.addAll(columnExprs);
            newGroupByExprs.add(expression);
          }
        }
        aggregation.setGroupByExprs(newGroupByExprs.toArray(new Expression[newGroupByExprs.size()]));
        if (aggregation.getGroupByExprs().length == 0) {
          aggregation.setGroupByExprs(new Expression[]{ ValueExpr.ONE });
        }
      }
      this.parentColumns = usedColumns;

      return visitChildren(aggregation);
    }

    @Override
    public RelOperator visitOperator(Order order) {
      List<ColumnExpr> extList = new ArrayList<>();
      List<Order.OrderExpression> newExprs = new ArrayList<>();

      for (Order.OrderExpression orderExpression : order.getOrderExpressions()) {
        ExpressionUtil.extractColumns(extList, orderExpression.getExpression(), null);
        if (extList.isEmpty() && orderExpression.getExpression().getExprType() != ExpressionType.CONST) {
          continue;
        } else if (orderExpression.getExpression().getResultType().getType() == Basepb.DataType.Null) {
          continue;
        } else {
          newExprs.add(orderExpression);
          parentColumns.addAll(extList);
        }
      }

      order.setOrderExpressions(newExprs.toArray(new Order.OrderExpression[newExprs.size()]));

      return visitChildren(order);
    }

    @Override
    public RelOperator visitOperator(DualTable dualTable) {
      Schema schema = dualTable.getSchema();
      boolean[] usedIndex = findUsedPosition(parentColumns, schema);
      if (usedIndex == null) {
        return dualTable;
      }
      List<ColumnExpr> currentColumnExprs = schema.getColumns();
      List<ColumnExpr> newColumnExprs = new ArrayList<>(currentColumnExprs.size());

      for (int i = 0; i < usedIndex.length; i++) {
        if (usedIndex[i]) {
          newColumnExprs.add(currentColumnExprs.get(i));
        }
      }
      dualTable.setSchema(new Schema(newColumnExprs));


      return dualTable;
    }

    protected boolean[] findUsedPosition(List<ColumnExpr> parentColumns, Schema schema) {
      boolean[] position = new boolean[schema.getColumns().size()];
      Arrays.fill(position, false);
      for (int i = 0; i < parentColumns.size(); i++) {
        ColumnExpr column = parentColumns.get(i);
        int columnOffset = schema.getColumnIndex(column);
        if (columnOffset == -1) {
          return null;
        }
        position[columnOffset] = true;
      }

      return position;
    }
  }
}
