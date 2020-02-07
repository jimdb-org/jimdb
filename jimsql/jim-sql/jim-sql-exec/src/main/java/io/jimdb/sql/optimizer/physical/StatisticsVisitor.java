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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import io.jimdb.core.Session;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.Schema;
import io.jimdb.core.expression.TableAccessPath;
import io.jimdb.core.expression.ValueRange;
import io.jimdb.core.model.meta.Column;
import io.jimdb.core.model.meta.Table;
import io.jimdb.sql.operator.Aggregation;
import io.jimdb.sql.operator.DualTable;
import io.jimdb.sql.operator.Limit;
import io.jimdb.sql.operator.Projection;
import io.jimdb.sql.operator.RelOperator;
import io.jimdb.sql.operator.Selection;
import io.jimdb.sql.operator.TableSource;
import io.jimdb.sql.operator.TopN;
import io.jimdb.sql.optimizer.OptimizationUtil;
import io.jimdb.sql.optimizer.ParameterizedOperatorVisitor;
import io.jimdb.sql.optimizer.statistics.OperatorStatsInfo;
import io.jimdb.sql.optimizer.statistics.StatsUtils;
import io.jimdb.sql.optimizer.statistics.TableSourceStatsInfo;
import io.jimdb.sql.optimizer.statistics.TableStats;
import io.jimdb.sql.optimizer.statistics.TableStatsManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuple4;
import reactor.util.function.Tuples;

/**
 * This visitor attaches statistics information to operators.
 */
@SuppressFBWarnings({ "FCBL_FIELD_COULD_BE_LOCAL", "LII_LIST_INDEXED_ITERATING", "URF_UNREAD_FIELD",
        "CE_CLASS_ENVY", "ITC_INHERITANCE_TYPE_CHECKING", "SPP_USE_ISEMPTY",
        "ICAST_IDIV_CAST_TO_DOUBLE", "PCAIL_POSSIBLE_CONSTANT_ALLOCATION_IN_LOOP",
        "RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE" })
public class StatisticsVisitor extends ParameterizedOperatorVisitor<OperatorStatsInfo[], OperatorStatsInfo> {
  private static final Logger LOG = LoggerFactory.getLogger(StatisticsVisitor.class);

  public StatisticsVisitor() {
  }

  @VisibleForTesting
  public OperatorStatsInfo deriveStatInfo(Session session, @NonNull RelOperator operator) {
    if (operator.getStatInfo() != null) {
      return operator.getStatInfo();
    }

    OperatorStatsInfo[] childStats = new OperatorStatsInfo[0];
    RelOperator[] children = operator.getChildren();
    if (null != children) {
      childStats = new OperatorStatsInfo[children.length];
      int i = 0;
      for (RelOperator childOperator : children) {
        OperatorStatsInfo childStat = deriveStatInfo(session, childOperator);
        if (childStat == null) {
          return null;
        }
        childStats[i++] = childStat;
      }
    }

    return operator.acceptVisitor(session, this, childStats);
  }

  @Override
  public OperatorStatsInfo visitOperatorByDefault(Session session, RelOperator operator, OperatorStatsInfo[] childStats) {
    if (childStats.length > 1) {
      return null;
    }

    if (childStats.length == 1) {
      operator.setStatInfo(childStats[0]);
      return childStats[0];
    }

    int columnNum = operator.getSchema().getColumns().size();
    double[] cardinality = new double[columnNum];
    for (int i = 0; i < columnNum; i++) {
      cardinality[i] = 1;
    }
    OperatorStatsInfo statInfo = new OperatorStatsInfo(1, cardinality);
    operator.setStatInfo(statInfo);
    return statInfo;
  }

  private double calculateMaxCardinalityValue(List<ColumnExpr> columnExprs, Schema schema, @NonNull OperatorStatsInfo statInfo) {

    // get the column index in the original table for each column expression
    final List<Integer> positions = schema.indexPosInSchema(columnExprs);

    if (positions == null) {
      LOG.error("Could not find indices for the given columnExprs, return default Cardinality 1.0");
      return 1.0;
    }

    final double maxCardinality = positions.stream()
                                          .map(statInfo::getCardinality)
                                          .max(Double::compareTo)
                                          .orElse(1.0);

    return Math.max(maxCardinality, 1.0);
  }

  @Override
  public OperatorStatsInfo visitOperator(Session session, Projection projection, OperatorStatsInfo[] childStats) {
    OperatorStatsInfo childStat = childStats[0];
    Schema childSchema = projection.getChildren()[0].getSchema();
    double rowCount = childStat.getEstimatedRowCount();
    Expression[] expressions = projection.getExpressions();
    double[] cardinality = new double[expressions.length];
    for (int i = 0; i < expressions.length; i++) {
      List<ColumnExpr> columns = new ArrayList<>();
      extractColumns(columns, expressions[i], null);
      cardinality[i] = calculateMaxCardinalityValue(columns, childSchema, childStat);
    }

    OperatorStatsInfo statInfo = new OperatorStatsInfo(rowCount, cardinality);
    projection.setStatInfo(statInfo);
    return statInfo;
  }

  private OperatorStatsInfo retrieveStatInfoFromChildren(OperatorStatsInfo[] childStats, long count) {
    OperatorStatsInfo childStat = childStats[0];
    double rowCount = Math.min(count, childStat.getEstimatedRowCount());
    double[] cardinality = childStat.newCardinalityList(rowCount);

    return new OperatorStatsInfo(rowCount, cardinality);
  }

  @Override
  public OperatorStatsInfo visitOperator(Session session, Limit limit, OperatorStatsInfo[] childStats) {
    OperatorStatsInfo statInfo = retrieveStatInfoFromChildren(childStats, limit.getCount());
    limit.setStatInfo(statInfo);

    return statInfo;
  }

  @Override
  public OperatorStatsInfo visitOperator(Session session, TopN topn, OperatorStatsInfo[] childStats) {
    OperatorStatsInfo statInfo = retrieveStatInfoFromChildren(childStats, topn.getCount());
    topn.setStatInfo(statInfo);

    return statInfo;
  }

  @Override
  public OperatorStatsInfo visitOperator(Session session, DualTable dual, OperatorStatsInfo[] childStats) {
    double rowCount = dual.getRowCount();
    int columnNum = dual.getSchema().getColumns().size();
    double[] cardinality = new double[columnNum];
    for (int i = 0; i < columnNum; i++) {
      cardinality[i] = rowCount;
    }

    OperatorStatsInfo statInfo = new OperatorStatsInfo(rowCount, cardinality);
    dual.setStatInfo(statInfo);
    return statInfo;
  }

  @Override
  public OperatorStatsInfo visitOperator(Session session, Selection selection, OperatorStatsInfo[] childStats) {
    OperatorStatsInfo statInfo = childStats[0].adjust(StatsUtils.SELECTIVITY_DEFAULT);
    selection.setStatInfo(statInfo);
    return statInfo;
  }

  @Override
  public OperatorStatsInfo visitOperator(Session session, Aggregation aggregation, OperatorStatsInfo[] childStats) {
    OperatorStatsInfo childStat = childStats[0];
    ColumnExpr[] groupByColumns = aggregation.getGroupByColumnExprs();
    final double maxCardinalityValue = calculateMaxCardinalityValue(Arrays.asList(groupByColumns), aggregation.getChildren()[0].getSchema(), childStat);
    final int size = aggregation.getSchema().getColumns().size();
    double[] cardinalityList = new double[size];

    Arrays.fill(cardinalityList, maxCardinalityValue);

    OperatorStatsInfo statInfo = new OperatorStatsInfo(maxCardinalityValue, cardinalityList);
    statInfo.setEstimatedRowCount(childStat.getEstimatedRowCount());
    aggregation.setStatInfo(statInfo);
    aggregation.setInputCount(childStat.getEstimatedRowCount());
    return statInfo;
  }

  private boolean deriveTablePathStats(Session session, TableSource tableSource, TableSourceStatsInfo tableSourceStatInfo,
                                       TableAccessPath path, List<Expression> pushedDownConditions) {

    final double rowCount = tableSourceStatInfo.getRawRowCount();
    path.setCountOnAccess(rowCount);
    ColumnExpr primaryKeyColumnExpr = path.getIndexColumns().get(0);

    path.setRanges(RangeBuilder.buildFullIntegerRange(true));
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

    final List<ValueRange> ranges = RangeBuilder.buildPKRange(session, tuple2.getT1(), primaryKeyColumnExpr
            .getResultType());
    path.setRanges(ranges);

    double count = tableSourceStatInfo.estimateRowCountByColumnRanges(session, primaryKeyColumnExpr.getId(),
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

  private boolean deriveIndexPathStats(Session session, TableSource tableSource, TableSourceStatsInfo tableSourceStatInfo, TableAccessPath path,
                                       List<Expression> pushedDownConditions) {

    final double rowCount = tableSourceStatInfo.getEstimatedRowCount();
    path.setCountOnAccess(rowCount);
//    path.setRanges(Collections.singletonList(RangeBuilder.fullRange()));
    final List<ColumnExpr> columnExprs = path.getIndexColumns();

    if (columnExprs.size() > 0) {
      Tuple4<List<ValueRange>, List<Expression>, List<Expression>, Boolean> tuple4 =
              NFDetacher.detachConditionsAndBuildRangeForIndex(session, pushedDownConditions, columnExprs);

      List<ValueRange> ranges = tuple4.getT1();
      path.setRanges(ranges);

      // access conditions are the ones used to build the range
      path.setAccessConditions(tuple4.getT2());

      // table conditions are the remaining conditions
      path.setTableConditions(tuple4.getT3());
      double countOnAccess = tableSourceStatInfo.estimateRowCountByIndexedRanges(session, path.getIndex().getId(),
              ranges);
      path.setCountOnAccess(countOnAccess);
    } else {
      path.setTableConditions(pushedDownConditions);
    }

    // update table and index conditions
    Tuple2<List<Expression>, List<Expression>> tuple2 = extractIndexConditions(tableSource.getTable(),
            path.getTableConditions(), path.getIndex().getColumns());
    path.setIndexConditions(tuple2.getT1());
    path.setTableConditions(tuple2.getT2());

    double rowCountInStatInfo = tableSourceStatInfo.getEstimatedRowCount();

    // it seems more reasonable that CountOnAccess is smaller than CountInStatInfo
    if (path.getCountOnAccess() > rowCountInStatInfo) {
      double newCount = Math.min(rowCountInStatInfo / SELECTION_FACTOR, path.getCountOnAccess());
      path.setCountOnAccess(newCount);
    }

    if (path.getIndexConditions() != null && !path.getIndexConditions().isEmpty()) {
      double selectivity = tableSourceStatInfo.calculateSelectivity(session, path.getIndexConditions());
      double countOnIndex = Math.max(path.getCountOnAccess() * selectivity, rowCountInStatInfo);
      path.setCountOnIndex(countOnIndex);
    }

    return checkRanges(session, path.getRanges(), path.getIndex().getColumns().length);
  }

  private Tuple2<List<Expression>, List<Expression>> extractIndexConditions(Table table, List<Expression> tableConditions,
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
  private boolean checkRanges(Session session, List<ValueRange> ranges, int indexColumnCount) {
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

  @Override
  public OperatorStatsInfo visitOperator(Session session, TableSource tableSource, OperatorStatsInfo[] childStats) {
    // TODO convert query not '(x != 1)' to 'x = 1' here

    // TODO one optimization we can do here is to check if the table source has the table stats with exactly the
    //  same expression set (push-down predicates). If so, we can simply return the cached one.

    // TODO add PseudoTableStats if we cannot find the table stats in tableStatsMap
    TableStats tableStats = TableStatsManager.getTableStats(tableSource.getTable());

    if (tableStats == null) {
      OperatorStatsInfo statInfo = new OperatorStatsInfo(0);
      tableSource.setStatInfo(statInfo);

      TableStatsManager.addToCache(tableSource.getTable(), new TableStats(tableSource.getTable(), 0));
      return statInfo;
    }

    TableSourceStatsInfo tableSourceStatInfo =
            new TableSourceStatsInfo(tableStats, tableSource.getColumns(), tableSource.getSchema().getColumns());

    double selectivity = tableSourceStatInfo.calculateSelectivity(session, tableSource.getPushDownPredicates());
    OperatorStatsInfo operatorStatsInfo = tableSourceStatInfo.adjust(selectivity);

    // We first set the table source stat and then estimate the path cost
    tableSource.setStatInfo(operatorStatsInfo);
//    List<Expression> conditions = tableSource.getPushDownPredicates();

//    for (TableAccessPath path : tableSource.getTableAccessPaths()) {
//
//      if (path.isTablePath()) {
//        boolean noIntervalInRanges = deriveTablePathStats(session, tableSource, tableSourceStatInfo, path, conditions);
//        if (noIntervalInRanges || path.getRanges().isEmpty()) {
//          tableSource.setTableAccessPaths(Collections.singletonList(path));
//          break;
//        }
//        continue;
//      }
//
//      boolean noIntervalInRanges = deriveIndexPathStats(session, tableSource, tableSourceStatInfo, path, conditions);
//      if ((noIntervalInRanges && path.getIndex().isUnique()) || path.getRanges().isEmpty()) {
//        tableSource.setTableAccessPaths(Collections.singletonList(path));
//        break;
//      }
//    }

    // FIXME from TiDB: 'if ds.ctx.GetSessionVars().OptimizerSelectivityLevel >= 1 {
    //  ds.stats.HistColl = ds.stats.HistColl.NewHistCollBySelectivity(ds.ctx.GetSessionVars().StmtCtx, nodes)
    // }'

    return operatorStatsInfo;
  }
}

