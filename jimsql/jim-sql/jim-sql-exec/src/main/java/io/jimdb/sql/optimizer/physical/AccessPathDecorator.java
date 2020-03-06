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
import static io.jimdb.sql.optimizer.statistics.StatsUtils.SELECTION_FACTOR;

import java.util.Collections;
import java.util.List;

import io.jimdb.core.Session;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.TableAccessPath;
import io.jimdb.core.expression.ValueRange;
import io.jimdb.core.model.meta.Column;
import io.jimdb.core.model.meta.Table;
import io.jimdb.sql.operator.RelOperator;
import io.jimdb.sql.operator.TableSource;
import io.jimdb.sql.optimizer.OptimizationUtil;
import io.jimdb.sql.optimizer.statistics.OperatorStatsInfo;

import com.google.common.collect.Lists;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuple4;
import reactor.util.function.Tuples;

/**
 * Class to update the access paths before building stats
 */
@SuppressFBWarnings("CE_CLASS_ENVY")
public class AccessPathDecorator {

  public static void decorateAccessPath(Session session, RelOperator operator) {
    if (operator == null) {
      return;
    }

    if (operator instanceof TableSource) {
      decorate(session, (TableSource) operator);
      return;
    }

    if (operator.hasChildren()) {
      for (RelOperator relOperator : operator.getChildren()) {
        decorateAccessPath(session, relOperator);
      }
    }
  }

  private static void decorate(Session session, TableSource tableSource) {
    List<Expression> conditions = tableSource.getPushDownPredicates();

    for (TableAccessPath path : tableSource.getTableAccessPaths()) {

      if (path.isTablePath()) {
        boolean noIntervalInRanges = decorateTablePath(session, tableSource, path, conditions);
        if (noIntervalInRanges || path.getRanges().isEmpty()) {
          tableSource.setTableAccessPaths(Collections.singletonList(path));
          break;
        }
        continue;
      }

      boolean noIntervalInRanges = decorateIndexPath(session, tableSource, path, conditions);
      if ((noIntervalInRanges && path.getIndex().isUnique()) || path.getRanges().isEmpty()) {
        tableSource.setTableAccessPaths(Collections.singletonList(path));
        break;
      }
    }
  }

  private static boolean decorateTablePath(Session session, TableSource tableSource, TableAccessPath path,
                                           List<Expression> pushedDownConditions) {
    OperatorStatsInfo operatorStatsInfo = tableSource.getStatInfo();
    final double rowCount = operatorStatsInfo.getEstimatedRowCount();
    path.setCountOnAccess(rowCount);

    if (pushedDownConditions == null || pushedDownConditions.isEmpty() || path.getIndexColumns().isEmpty()) {
      path.setRanges(RangeBuilder.fullRangeList());
      path.setTableConditions(pushedDownConditions);
      return false;
    }

    Tuple4<List<ValueRange>, List<Expression>, List<Expression>, Boolean> cnfResult =
            NFDetacher.detachConditionsAndBuildRangeForIndex(session, pushedDownConditions, path.getIndexColumns());
    path.setAccessConditions(cnfResult.getT2());
    path.setTableConditions(cnfResult.getT3());
    path.setRanges(cnfResult.getT1());

    // TODO If there's no access cond, we should figure out whether any expression contains correlated column that
    //  can be used to access data

    double count = operatorStatsInfo.estimateRowCountByIndexedRanges(session,
            tableSource.getTable().getPrimaryIndex().getId(),
            path.getRanges());
    if (count > 0) {
      // when the pseudo count is ready, this count should not be 0
      double rowCountInStatInfo = tableSource.getStatInfo().getEstimatedRowCount();

      // if this happens, it means there must be some inconsistent stats info. We prefer the statInfoRowCount because it
      // could use more stats info to calculate the selectivity.
      if (count < rowCountInStatInfo) {
        count = Math.min(rowCountInStatInfo / SELECTION_FACTOR, path.getCountOnAccess());
      }
      path.setCountOnAccess(count);
    }

    for (ValueRange valueRange : path.getRanges()) {
      if (!valueRange.isPoint(session)) {
        return false;
      }
    }

    return true;
  }

  private static boolean decorateIndexPath(Session session, TableSource tableSource, TableAccessPath path,
                                           List<Expression> pushedDownConditions) {
    OperatorStatsInfo operatorStatsInfo = tableSource.getStatInfo();
    final double rowCount = operatorStatsInfo.getEstimatedRowCount();
    path.setCountOnAccess(rowCount);

    if (path.getIndexColumns().size() > 0) {
      Tuple4<List<ValueRange>, List<Expression>, List<Expression>, Boolean> tuple4 =
              NFDetacher.detachConditionsAndBuildRangeForIndex(session, pushedDownConditions, path.getIndexColumns());

      List<ValueRange> ranges = tuple4.getT1();
      path.setRanges(ranges);

      // access conditions are the ones used to build the range
      path.setAccessConditions(tuple4.getT2());

      // table conditions are the remaining conditions
      path.setTableConditions(tuple4.getT3());
      double countOnAccess = operatorStatsInfo.estimateRowCountByIndexedRanges(session, path.getIndex().getId(),
              ranges);
      path.setCountOnAccess(countOnAccess);
    } else {
      path.setTableConditions(pushedDownConditions);
      path.setRanges(RangeBuilder.fullRangeList());
    }

    // update table and index conditions
    Tuple2<List<Expression>, List<Expression>> tuple2 = extractIndexConditions(tableSource.getTable(),
            path.getTableConditions(), path.getIndex().getColumns());
    path.setIndexConditions(tuple2.getT1());
    path.setTableConditions(tuple2.getT2());

    double rowCountInStatInfo = operatorStatsInfo.getEstimatedRowCount();

    // it seems more reasonable that CountOnAccess is smaller than CountInStatInfo
    if (path.getCountOnAccess() > rowCountInStatInfo) {
      double newCount = Math.min(rowCountInStatInfo / SELECTION_FACTOR, path.getCountOnAccess());
      path.setCountOnAccess(newCount);
    }

    if (path.getIndexConditions() != null && !path.getIndexConditions().isEmpty()) {
      double selectivity = operatorStatsInfo.calculateSelectivity(session, path.getIndexConditions());
      double countOnIndex = Math.max(path.getCountOnAccess() * selectivity, rowCountInStatInfo);
      path.setCountOnIndex(countOnIndex);
    }

    return checkRanges(session, path.getRanges(), path.getIndex().getColumns().length);
  }

  private static Tuple2<List<Expression>, List<Expression>> extractIndexConditions(Table table,
                                                                                   List<Expression> tableConditions,
                                                                                   Column[] indexColumns) {
    if (tableConditions == null) {
      return Tuples.of(Collections.emptyList(), Collections.emptyList());
    }

    List<Expression> indexConditions = Lists.newArrayListWithCapacity(tableConditions.size());
    List<Expression> newTableConditions = Lists.newArrayListWithCapacity(tableConditions.size());

    for (Expression condition : tableConditions) {
      List<ColumnExpr> extractedColumns = extractColumns(condition);

      if (OptimizationUtil.columnsCoveredByIndex(table.getPrimary(), extractedColumns, indexColumns)) {
        indexConditions.add(condition);
      } else {
        newTableConditions.add(condition);
      }
    }

    return Tuples.of(indexConditions, newTableConditions);
  }

  // Check if these ranges only have point query
  private static boolean checkRanges(Session session, List<ValueRange> ranges, int indexColumnCount) {
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

