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
package io.jimdb.sql.optimizer.logical;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import io.jimdb.core.Session;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.ExpressionType;
import io.jimdb.core.expression.ExpressionUtil;
import io.jimdb.core.expression.FuncExpr;
import io.jimdb.core.expression.KeyColumn;
import io.jimdb.core.expression.Schema;
import io.jimdb.core.expression.TableAccessPath;
import io.jimdb.core.expression.functions.FuncType;
import io.jimdb.core.model.meta.Column;
import io.jimdb.core.model.meta.Index;
import io.jimdb.sql.operator.Aggregation;
import io.jimdb.sql.operator.DualTable;
import io.jimdb.sql.operator.Limit;
import io.jimdb.sql.operator.Order;
import io.jimdb.sql.operator.Projection;
import io.jimdb.sql.operator.RelOperator;
import io.jimdb.sql.operator.Selection;
import io.jimdb.sql.operator.TableSource;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Class to collect key information for later optimization.
 */

@SuppressFBWarnings("CFS_CONFUSING_FUNCTION_SEMANTICS")
public class BuildKeyOptimizer implements IRuleOptimizer {
  @Override
  public RelOperator optimize(RelOperator relOperator, Session session) {
    return relOperator.acceptVisitor(new BuildKeyVisitor(session));
  }

  /**
   * Implementation class for build key visitor.
   */
  private static class BuildKeyVisitor extends LogicalOptimizationVisitor {
    private Session session;

    BuildKeyVisitor(Session session) {
      this.session = session;
    }

    @Override
    public RelOperator visitOperator(Aggregation aggregation) {
      Schema schema = aggregation.getSchema();
      schema.resetKeyColumn();
      visitChildren(aggregation);

      List<KeyColumn> childIndexes = aggregation.getChildren()[0].getSchema().getKeyColumns();
      if (childIndexes == null) {
        return aggregation;
      }
      for (KeyColumn index : childIndexes) {
        List<Integer> posList = schema.indexPosInSchema(index.getColumnExprs());
        if (posList == null) {
          continue;
        }
        KeyColumn keyColumn = new KeyColumn(posList.size());
        posList.forEach(pos -> keyColumn.addColumnExpr(schema.getColumn(pos)));
        schema.addKeyColumn(keyColumn);
      }

      if (aggregation.getGroupByColumnExprs().length == aggregation.getGroupByExprs().length
              && aggregation.getGroupByExprs().length > 0) {
        List<Integer> posList = schema.indexPosInSchema(Arrays.asList(aggregation.getGroupByColumnExprs()));
        if (posList != null) {
          KeyColumn keyColumn = new KeyColumn(posList.size());
          posList.forEach(pos -> keyColumn.addColumnExpr(schema.getColumn(pos)));
          schema.addKeyColumn(keyColumn);
        }
      }

      if (aggregation.getGroupByExprs().length == 0) {
        aggregation.setIsMaxOneRow(true);
      }

      return aggregation;
    }

    @Override
    public RelOperator visitOperator(Projection projection) {
      projection.getSchema().resetKeyColumn();
      visitChildren(projection);

      Schema schema = buildSchemaFromExprs(projection);
      List<KeyColumn> childIndexes = projection.getChildren()[0].getSchema().getKeyColumns();
      if (childIndexes == null) {
        return projection;
      }
      for (KeyColumn index : childIndexes) {
        List<Integer> posList = schema.indexPosInSchema(index.getColumnExprs());
        if (posList == null) {
          continue;
        }
        KeyColumn keyColumn = new KeyColumn(posList.size());
        posList.forEach(pos -> keyColumn.addColumnExpr(projection.getSchema().getColumn(pos)));
        projection.getSchema().addKeyColumn(keyColumn);
      }

      return projection;
    }

    @Override
    public RelOperator visitOperator(Limit limit) {
      visitChildren(limit);

      if (limit.getCount() == 1) {
        limit.setIsMaxOneRow(true);
      }

      return limit;
    }

    @Override
    public RelOperator visitOperator(Order order) {
      return visitChildren(order);
    }

    @SuppressFBWarnings("CLI_CONSTANT_LIST_INDEX")
    @Override
    public RelOperator visitOperator(Selection selection) {
      visitChildren(selection);

      List<Expression> conditions = selection.getConditions();
      if (conditions == null) {
        return selection;
      }
      for (Expression condition : conditions) {
        if (condition.getExprType() != ExpressionType.FUNC) {
          continue;
        }
        FuncExpr funcExpr = (FuncExpr) condition;
        if (funcExpr.getFuncType() == FuncType.Equality) {
          Expression[] args = funcExpr.getArgs();
          if (isMaxOneRow(selection, args[0], args[1]) || isMaxOneRow(selection, args[1], args[0])) {
            selection.setIsMaxOneRow(true);
            break;
          }
        }
      }

      return selection;
    }

    @Override
    public RelOperator visitOperator(TableSource tableSource) {
      tableSource.getSchema().resetKeyColumn();
      visitChildren(tableSource);

      List<TableAccessPath> accessPaths = tableSource.getTableAccessPaths();
      if (accessPaths == null) {
        return tableSource;
      }
      for (TableAccessPath accessPath : accessPaths) {
        if (accessPath.isTablePath()) {
          continue;
        }
        Index index = accessPath.getIndex();
        if (null == index || !index.isUnique()) {
          continue;
        }
        Column[] columns = index.getColumns();
        KeyColumn keyColumn = new KeyColumn(columns.length);
        List<ColumnExpr> columnExprs = tableSource.getSchema().getColumns();

        boolean append = true;
        for (Column column : columns) {
          boolean found = false;
          for (int i = 0; i < columnExprs.size(); i++) {
            ColumnExpr columnExpr = columnExprs.get(i);
            if (column.getName().equals(columnExpr.getAliasCol())) {
              if (!tableSource.getColumns()[i].getType().getNotNull()) {
                break;
              }
              keyColumn.addColumnExpr(columnExpr);
              found = true;
              break;
            }
          }
          if (!found) {
            append = false;
            break;
          }
        }

        if (append) {
          tableSource.getSchema().addKeyColumn(keyColumn);
        }
      }

      Column[] primaryCols = tableSource.getTable().getPrimary();
      if (primaryCols != null) {
        KeyColumn keyColumn = new KeyColumn(primaryCols.length);
        for (Column primaryCol : primaryCols) {
          keyColumn.addColumnExpr(ExpressionUtil.columnToColumnExpr(primaryCol, tableSource.getSchema().getColumns()));
//          if (primaryCol.isPrimary()) {
//            tableSource.getSchema().addKeyColumn(
//                    new KeyColumn(tableSource.getSchema().getColumn(primaryCol.getOffset())));
//            ExpressionUtil.columnToColumnExpr(primaryCol, tableSource.getSchema().getColumns());
//            break;
//          }
        }
        if (!keyColumn.getColumnExprs().isEmpty()) {
          tableSource.getSchema().addKeyColumn(keyColumn);
        }
      }

      return tableSource;
    }

    @Override
    public RelOperator visitOperator(DualTable dualTable) {
      return dualTable;
    }

    private Schema buildSchemaFromExprs(Projection projection) {
      List<ColumnExpr> columnExprs = new ArrayList<>(projection.getSchema().getColumns().size());
      Expression[] expressions = projection.getExpressions();
      for (Expression expression : expressions) {
        if (expression.getExprType() == ExpressionType.COLUMN) {
          columnExprs.add((ColumnExpr) expression);
        } else {
          ColumnExpr columnExpr = new ColumnExpr(this.session.allocColumnID());
          columnExpr.setResultType(expression.getResultType());
          columnExprs.add(columnExpr);
        }
      }

      return new Schema(columnExprs);
    }

    private boolean isMaxOneRow(Selection selection, Expression left, Expression right) {
      if (left.getExprType() != ExpressionType.COLUMN) {
        return false;
      }

      ColumnExpr columnExpr = (ColumnExpr) left;
      boolean uniqueIndexCol = selection.getChildren()[0].getSchema().isUniqueIndexCol(columnExpr);
      if (!uniqueIndexCol) {
        return false;
      }

      if (right.getExprType() == ExpressionType.CONST) {
        return true;
      }

      return right.getExprType() == ExpressionType.COLUMN && ((ColumnExpr) right).isInSubQuery();
    }

    @Override
    public RelOperator visitChildren(RelOperator operator) {
      RelOperator[] childrenOperators = operator.getChildren();
      if (null != childrenOperators && childrenOperators.length > 0) {
        for (RelOperator childOperator : childrenOperators) {
          childOperator.acceptVisitor(this);
        }
      }
      if (operator instanceof Limit || operator instanceof Order
              || operator instanceof Selection || operator instanceof Projection) {
        operator.setIsMaxOneRow(operator.getChildren()[0].isMaxOneRow());
      }
      return operator;
    }
  }
}
