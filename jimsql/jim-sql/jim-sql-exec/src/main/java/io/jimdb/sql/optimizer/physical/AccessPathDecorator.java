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

package io.jimdb.sql.optimizer.physical;

import static io.jimdb.core.expression.ExpressionUtil.extractColumns;

import java.util.Collections;
import java.util.List;

import io.jimdb.core.Session;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.ExpressionUtil;
import io.jimdb.core.expression.TableAccessPath;
import io.jimdb.core.expression.ValueRange;
import io.jimdb.core.model.meta.Column;
import io.jimdb.sql.operator.RelOperator;
import io.jimdb.sql.operator.TableSource;
import io.jimdb.sql.optimizer.OperatorVisitor;

import com.google.common.collect.Lists;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuple4;
import reactor.util.function.Tuples;

/**
 * Class to update the access paths before building stats
 */
@Deprecated
@SuppressFBWarnings("UP_UNUSED_PARAMETER")
public class AccessPathDecorator extends OperatorVisitor<Boolean> {

  private Session session;

  public AccessPathDecorator(Session session) {
    this.session = session;
  }

  @Override
  public Boolean visitOperatorByDefault(RelOperator operator) {
    if (operator == null) {
      return Boolean.FALSE;
    }

    if (operator.getChildren() == null) {
      return Boolean.TRUE;
    }

    for (RelOperator relOperator : operator.getChildren()) {
      if (relOperator.acceptVisitor(this)) {
        // TODO log
      }
    }

    return Boolean.TRUE;
  }

  @Override
  public Boolean visitOperator(TableSource tableSource) {

    List<Expression> conditions = tableSource.getPushDownPredicates();

    for (TableAccessPath path : tableSource.getTableAccessPaths()) {

      if (path.isTablePath()) {
        boolean noIntervalInRanges = decorateTablePath(tableSource, path, conditions);
        if (noIntervalInRanges || path.getRanges().isEmpty()) {
          //tableSource.setTableAccessPaths(Collections.singletonList(path));
          //break;
        }
        continue;
      }

      boolean noIntervalInRanges = decorateIndexPath(tableSource, path, conditions);
      if ((noIntervalInRanges && path.getIndex().isUnique()) || path.getRanges().isEmpty()) {
        //tableSource.setTableAccessPaths(Collections.singletonList(path));
        //break;
      }
    }

    return Boolean.TRUE;
  }

  private boolean decorateTablePath(TableSource tableSource, TableAccessPath path, List<Expression> pushedDownConditions) {

    Column primaryKeyColumn = tableSource.getTable().getPrimary()[0];
    ColumnExpr primaryKeyColumnExpr = ExpressionUtil.columnToColumnExpr(primaryKeyColumn, tableSource.getSchema().getColumns());

    // TODO change to integer full range
    //path.setRanges(RangeBuilder.buildFullIntegerRange(true));
    path.setRanges(Collections.singletonList(RangeBuilder.fullRange()));

    path.setTableConditions(pushedDownConditions);

    if (pushedDownConditions == null || pushedDownConditions.isEmpty() || primaryKeyColumnExpr == null) {
      // TODO primaryKeyColumnExpr should never be null, add log here
      return false;
    }

    Tuple2<List<Expression>, List<Expression>> tuple2 =
            NFDetacher.detachConditions(session, pushedDownConditions, primaryKeyColumnExpr.getId());

    path.setAccessConditions(tuple2.getT1());
    path.setTableConditions(tuple2.getT2());

    // TODO If there's no access cond, we should figure out whether any expression contains correlated column that
    //  can be used to access data

    final List<ValueRange> ranges = RangeBuilder.buildPKRange(session, tuple2.getT1(), primaryKeyColumnExpr.getResultType());
    path.setRanges(ranges);

    for (ValueRange valueRange : path.getRanges()) {
      if (!valueRange.isPoint(session)) {
        return false;
      }
    }

    return true;
  }

  private boolean decorateIndexPath(TableSource tableSource, TableAccessPath path, List<Expression> pushedDownConditions) {

    path.setRanges(Collections.singletonList(RangeBuilder.fullRange()));
    final List<ColumnExpr> columnExprs = ExpressionUtil.indexToColumnExprs(path.getIndex(), tableSource.getSchema().getColumns());
    path.setIndexColumns(columnExprs);

    if (columnExprs.size() > 0) {
      Tuple4<List<ValueRange>, List<Expression>, List<Expression>, Boolean> tuple4 =
              NFDetacher.detachConditionsAndBuildRangeForIndex(session, pushedDownConditions, columnExprs);

      List<ValueRange> ranges = tuple4.getT1();
      path.setRanges(ranges);

      // access conditions are the ones used to build the range
      path.setAccessConditions(tuple4.getT2());

      // table conditions are the remaining conditions
      path.setTableConditions(tuple4.getT3());
    } else {
      path.setTableConditions(pushedDownConditions);
    }

    // update table and index conditions
    Tuple2<List<Expression>, List<Expression>> tuple2 = extractIndexConditions(path.getTableConditions(), path.getIndex().getColumns());
    path.setIndexConditions(tuple2.getT1());
    path.setTableConditions(tuple2.getT2());

    return checkRanges(path.getRanges(), path.getIndex().getColumns().length);
  }

  private Tuple2<List<Expression>, List<Expression>> extractIndexConditions(List<Expression> tableConditions, Column[] indexColumns) {
    if (tableConditions == null) {
      return Tuples.of(Collections.emptyList(), Collections.emptyList());
    }

    List<Expression> indexConditions = Lists.newArrayListWithCapacity(tableConditions.size());
    List<Expression> newTableConditions = Lists.newArrayListWithCapacity(tableConditions.size());

    for (Expression condition: tableConditions) {
      List<ColumnExpr> extractedColumns = extractColumns(condition);

//      if (OptimizationUtil.columnsCoveredByIndex(extractedColumns, indexColumns)) {
//        indexConditions.add(condition);
//      } else {
//        newTableConditions.add(condition);
//      }
    }

    return Tuples.of(indexConditions, newTableConditions);
  }

  // Check if these ranges only have point query
  private boolean checkRanges(List<ValueRange> ranges, int indexColumnCount) {
    for (ValueRange range : ranges) {
      if (!range.isPoint(session) || range.getEnds().size() != indexColumnCount) {
        return false;
      }

      for (int i = 0; i < indexColumnCount; i++) {
        if (range.getEnds().get(i).isNull()) {
          return false;
        }
      }
    }

    return true;
  }
}

