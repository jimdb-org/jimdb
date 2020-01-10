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

package io.jimdb.sql.optimizer.statistics;

import static io.jimdb.sql.optimizer.statistics.StatsUtils.DEFAULT_ROW_COUNT;
import static io.jimdb.sql.optimizer.statistics.StatsUtils.OUT_OF_RANGE_BETWEEN_RATE;
import static io.jimdb.sql.optimizer.statistics.StatsUtils.SELECTION_FACTOR;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.jimdb.core.Session;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.ConditionChecker;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.ExpressionType;
import io.jimdb.core.expression.FuncExpr;
import io.jimdb.core.expression.ValueRange;
import io.jimdb.core.expression.functions.FuncType;
import io.jimdb.core.model.meta.Column;
import io.jimdb.core.model.meta.Table;
import io.jimdb.sql.optimizer.physical.NFDetacher;
import io.jimdb.sql.optimizer.physical.RangeBuilder;
import io.jimdb.core.values.Value;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Longs;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.util.function.Tuple4;

/**
 * Statistics information for a table source operator. Note that this is different from the global TableStats which
 * contains all the column and index stats for a given table.
 */
@SuppressFBWarnings()
public class TableSourceStatsInfo extends OperatorStatsInfo {
  private Map<Long, ColumnStats> nonIndexedColumnStatsMap; // key is uid

  private Map<Long, IndexStats> indexStatsMap; // key is index id

  private Map<Long, List<Long>> indexToColumnIds; // the map from the index id to its column ids.

  private Map<Long, Long> columnIdToIndexId; // the map from column id to index id of the first column.

  private long modifiedCount; // modified row count in the table (from table stats)

  private long rawRowCount; // row count obtained from table stats

  private List<Selectivity> selectivityList;

  // To be used to update table stats
  private Table table;

  public TableSourceStatsInfo(@NonNull TableStats tableStats, Column[] columnInfos, List<ColumnExpr> columnExprs) {

    super(tableStats.getEstimatedRowCount(),
            Arrays.stream(columnInfos).mapToDouble(column -> calculateColumnNdv(tableStats, column.getId())).toArray());

//    // initialize the estimated row count the same as the raw row count
//    this.estimatedRowCount = tableStats.getEstimatedRowCount();
//
//    // initialize cardinality
//    this.cardinalityList = new double[columnInfos.length];
//    for (int i = 0; i < columnInfos.length; i++) {
//      this.cardinalityList[i] = calculateColumnNdv(tableStats, columnInfos[i].getId());
//    }

    // TODO simplify these maps.
    Map<Integer, Long> colIdToUniqueId =
            columnExprs.stream().collect(Collectors.toMap(ColumnExpr::getId, ColumnExpr::getUid));

    this.nonIndexedColumnStatsMap = Maps.newHashMap();

    this.indexStatsMap = Maps.newHashMap();

    for (Map.Entry<Integer, ColumnStats> entry : tableStats.getNonIndexedColumnStatsMap().entrySet()) {
      Long uid = colIdToUniqueId.get(entry.getKey());

      if (uid != null) {
        this.nonIndexedColumnStatsMap.put(uid, entry.getValue());
      }
    }

    this.indexToColumnIds = Maps.newHashMap();
    this.columnIdToIndexId = Maps.newHashMap();

    for (IndexStats indexStats : tableStats.getIndexStatsMap().values()) {
      List<Long> uids = Lists.newArrayList();
      for (Column columnInfo : indexStats.getIndexInfo().getColumns()) {
        Long uid = colIdToUniqueId.get(columnInfo.getId());

        if (uid != null) {
          uids.add(uid);
        }
      }

      if (uids.isEmpty()) {
        continue;
      }

      final long indexId = indexStats.getIndexInfo().getId();
      this.columnIdToIndexId.put(uids.get(0), indexId);

      this.indexStatsMap.put(indexId, indexStats);

      this.indexToColumnIds.put(indexId, uids);
    }

    // initialize row count
    this.rawRowCount = tableStats.getEstimatedRowCount();
    this.modifiedCount = tableStats.getModifiedCount();

    this.selectivityList = Lists.newArrayList();
  }

  private static double calculateColumnNdv(TableStats tableStats, long columnId) {
    ColumnStats columnStats = tableStats.getColumnStats(columnId);
    if (null != columnStats && columnStats.getTotalRowCount() > 0) {
      double ratio = (double) tableStats.getEstimatedRowCount() / (double) columnStats.getTotalRowCount();
      return columnStats.getHistogram().getNdv() * ratio;
    } else {
      return tableStats.getEstimatedRowCount() * StatsUtils.DISTINCT_RATIO;
    }
  }

  /**
   * Calculate the selectivity of a list of expressions
   *
   * @param session        the session of the given query
   * @param expressionList the given expressions
   * @return the calculated selectivity
   */
  public double calculateSelectivity(Session session, List<Expression> expressionList) {
    if (this.estimatedRowCount == 0 || expressionList == null || expressionList.isEmpty()) {
      return 1;
    }

    double selectivityValue = 1.0;

    List<Expression> remainingExpressions = Lists.newArrayListWithCapacity(expressionList.size());

    // extract the correlated columns
    for (Expression expression : expressionList) {
      ColumnExpr columnExpr = extractCorrelatedColumnFromEqualFunction(expression);
      if (columnExpr == null) {
        remainingExpressions.add(expression);
        continue;
      }

      ColumnStats columnStats = this.nonIndexedColumnStatsMap.get(columnExpr.getUid());

      if (columnStats == null || columnStats.isInValid(session)) {
        selectivityValue *= 1.0 / DEFAULT_ROW_COUNT;
        continue;
      }

      long ndv = columnStats.getHistogram().getNdv();
      if (ndv > 0) {
        selectivityValue *= 1 / (double) ndv;
      } else {
        selectivityValue *= 1.0 / DEFAULT_ROW_COUNT;
      }
    }

    List<ColumnExpr> extractedColumns = extractColumnsFromExpressions(remainingExpressions);

    // update selectivities for non-indexed columns
    calculateSelectivityForNonIndexedColumns(session, extractedColumns, remainingExpressions);

    // update selectivities for indexed columns
    calculateSelectivityForIndices(session, extractedColumns, remainingExpressions);

    long mask = ((long) 1 << remainingExpressions.size()) - 1;

    List<Selectivity> selectedSets = greedySelect(selectivityList);

    for (Selectivity set : selectedSets) {
      mask ^= set.getMask();
      selectivityValue *= set.getValue();

      if (set.isPartialCoverage()) {
        selectivityValue *= SELECTION_FACTOR;
      }
    }

    if (mask > 0) {
      selectivityValue *= SELECTION_FACTOR;
    }

    return selectivityValue;
  }

  /**
   * If the expression is an eq function that one side is correlated column and the other is the original column.
   * We extract the original column from it.
   *
   * @param expression the given expression
   * @return the extracted original column if found; otherwise null;
   */
  private ColumnExpr extractCorrelatedColumnFromEqualFunction(Expression expression) {
    if (expression.getExprType() != ExpressionType.FUNC) {
      return null;
    }

    if (((FuncExpr) expression).getFuncType() != FuncType.Equality) {
      return null;
    }

    final Expression[] args = ((FuncExpr) expression).getArgs();

    if (args.length < 2) {
      return null;
    }

    if (args[0].getExprType() != ExpressionType.COLUMN || args[1].getExprType() != ExpressionType.COLUMN) {
      return null;
    }

    final ColumnExpr columnExpr0 = (ColumnExpr) args[0];
    final ColumnExpr columnExpr1 = (ColumnExpr) args[1];

    if (columnExpr0.getId().equals(columnExpr1.getId())) {
      return columnExpr0.getAliasCol().equals(columnExpr0.getOriCol()) ? columnExpr0 : columnExpr1;
    }

    return null;
  }

  private List<ColumnExpr> extractColumnsFromExpressions(List<Expression> expressionList) {
    List<ColumnExpr> columnExprList = Lists.newArrayList();
    for (Expression expression : expressionList) {
      columnExprList.addAll(extractColumns(expression));
    }

    return columnExprList;
  }

  private List<ColumnExpr> extractColumns(Expression expression) {
    List<ColumnExpr> columnExprList = Lists.newArrayList();
    if (expression.getExprType() == ExpressionType.COLUMN) {
      columnExprList.add((ColumnExpr) expression);
    } else if (expression.getExprType() == ExpressionType.FUNC) {
      for (Expression expr : ((FuncExpr) expression).getArgs()) {
        List<ColumnExpr> columnExprs = extractColumns(expr);
        columnExprList.addAll(columnExprs);
      }
    }

    return columnExprList;
  }

  private void calculateSelectivityForNonIndexedColumns(Session session, List<ColumnExpr> extractedColumns,
                                                        List<Expression> remainingExpressions) {

    // We must use id instead of uid here because the id of a ColumnExpr is the same as the corresponding Column
    // stored in ColumnStats
    final Map<Integer, ColumnExpr> idToColumnExprMap = new HashMap<>();

    for (ColumnExpr columnExpr : extractedColumns) {
      idToColumnExprMap.putIfAbsent(columnExpr.getId(), columnExpr);
    }

    long mask = 0;
    for (Map.Entry<Long, ColumnStats> entry : nonIndexedColumnStatsMap.entrySet()) {
      // for each ColumnStatistics, we need to first find the corresponding column in the extracted columns
      ColumnExpr columnExpr = idToColumnExprMap.get(entry.getKey());
      if (columnExpr == null) {
        continue;
      }

      List<Expression> accessConditions = extractAccessConditionsForColumn(remainingExpressions, columnExpr.getUid());
      List<ValueRange> ranges = RangeBuilder.buildColumnRange(session, accessConditions, columnExpr.getResultType());

      for (int i = 0; i < remainingExpressions.size(); i++) {
        for (Expression accessCondition : accessConditions) {
          if (remainingExpressions.get(i).equals(accessCondition)) {
            mask |= 1 << (long) i;
            break;
          }
        }
      }

      final double count = estimateRowCountByNonIndexedRanges(session, columnExpr.getUid(), ranges);
      final double selectivityValue = count / this.estimatedRowCount;
      selectivityList.add(new Selectivity(columnExpr.getId(), SelectivityType.COLUMN, mask, ranges, 1,
              selectivityValue, false));
    }
  }

  public double estimateRowCountByIntColumnRanges(Session session, long id, List<ValueRange> ranges) {
    final ColumnStats columnStats = this.nonIndexedColumnStatsMap.get(id);
    if (columnStats == null || columnStats.isInValid(session)) {
      // TODO return pseudo count ?
      //  see TiDB GetRowCountByIntColumnRanges
      return 0;
    }

    return columnStats.estimateRowCount(session, ranges, this.modifiedCount)
            * columnStats.getHistogram().estimateIncreasingFactor(this.rawRowCount);
  }

  public double estimateRowCountByNonIndexedRanges(Session session, long uid, List<ValueRange> ranges) {
    final ColumnStats columnStats = this.nonIndexedColumnStatsMap.get(uid);

    if (columnStats == null || columnStats.isInValid(session)) {
      return 0;
    }

    return columnStats.estimateRowCount(session, ranges, this.modifiedCount)
            * columnStats.getHistogram().estimateIncreasingFactor(this.rawRowCount);
  }

  // Extract the access conditions used for range calculation
  // TODO put this function into detacher
  private List<Expression> extractAccessConditionsForColumn(List<Expression> conditions, long uid) {
    ConditionChecker conditionChecker = new ConditionChecker(uid);
    List<Expression> result = Lists.newArrayList();
    for (Expression condition : conditions) {
      if (conditionChecker.check(condition)) {
        result.add(condition);
      }
    }

    return result;
  }

  private void calculateSelectivityForIndices(Session session, List<ColumnExpr> extractedColumns,
                                              List<Expression> remainingExpressions) {
    long mask = 0;
    for (Map.Entry<Long, IndexStats> entry : indexStatsMap.entrySet()) {
      // for each ColumnStatistics, we need to first find the corresponding index columns in the extracted columns
      long[] columnIds = Longs.toArray(indexToColumnIds.get(entry.getKey()));
      List<ColumnExpr> indexedColumnExprs = extractIndexedColumns(extractedColumns, columnIds);
      if (indexedColumnExprs.isEmpty()) {
        continue;
      }

      final int columnCount = entry.getValue().getIndexInfo().getColumns().length;

      Tuple4<List<ValueRange>, List<Expression>, List<Expression>, Boolean> tuple4 =
              NFDetacher.detachConditionsAndBuildRangeForIndex(session, remainingExpressions, indexedColumnExprs);

      List<ValueRange> ranges = tuple4.getT1();
      List<Expression> accessConditions = tuple4.getT2();
      List<Expression> remainingConditions = tuple4.getT3();
      final Boolean isDNF = tuple4.getT4();
      boolean isPartialCoverage = false;
      if (isDNF && accessConditions.size() > 0) {
        mask |= 1;
        isPartialCoverage = remainingConditions.size() > 0;
      } else {
        for (int i = 0; i < remainingExpressions.size(); i++) {
          for (Expression accessCondition : accessConditions) {
            if (remainingExpressions.get(i).equals(accessCondition)) {
              mask |= 1 << (long) i;
              break;
            }
          }
        }
      }

      final double count = estimateRowCountByIndexedRanges(session, entry.getKey(), ranges);
      final double selectivityValue = count / this.estimatedRowCount;

      selectivityList.add(new Selectivity(entry.getKey(), SelectivityType.COLUMN, mask, ranges, columnCount,
              selectivityValue, isPartialCoverage));
    }
  }

  private List<ColumnExpr> extractIndexedColumns(List<ColumnExpr> extractedColumns, long[] indexColumnIds) {
    List<ColumnExpr> result = Lists.newArrayList();
    for (long columnId : indexColumnIds) {
      for (ColumnExpr columnExpr : extractedColumns) {
        if (columnExpr.getUid().equals(columnId)) {
          result.add(columnExpr);
        }
      }
    }
    return result;
  }

  private List<Selectivity> greedySelect(List<Selectivity> candidates) {
    boolean[] selected = new boolean[candidates.size()];

    long mask = Long.MAX_VALUE;

    List<Selectivity> result = Lists.newArrayList();
    while (true) {

      Selectivity optimal = new Selectivity(SelectivityType.COLUMN, 0, 0);
      int maxSetBitCount = 0;
      int optimalIndex = -1;

      for (int i = 0; i < candidates.size(); i++) {
        Selectivity candidate = candidates.get(i);
        if (selected[i]) {
          continue;
        }

        long curMask = candidate.getMask() & mask;

        final int setBitCount = countSetBits(curMask);

        if (setBitCount == 0) {
          continue;
        }

        if ((optimal.getType() == SelectivityType.COLUMN && candidate.getType() != SelectivityType.COLUMN)
                || (maxSetBitCount < setBitCount)
                || (maxSetBitCount == setBitCount && optimal.getNumColumns() > candidate.getNumColumns())) {
          optimal = new Selectivity(candidate.getType(), curMask, candidate.getNumColumns());
          maxSetBitCount = setBitCount;
          optimalIndex = i;
        }
      }

      if (maxSetBitCount == 0) {
        break;
      }

      // remove the bit that the optimal's mask has
      mask ^= optimal.getMask();

      result.add(candidates.get(optimalIndex));
      selected[optimalIndex] = true;
    }

    return result;
  }

  /**
   * Get number of set bits in binary representation of a positive integer
   *
   * @param mask the given positive integer
   * @return set bit count
   */
  private int countSetBits(long mask) {
    int count = 0;
    while (mask > 0) {
      count += mask & 1;
      mask >>= 1;
    }

    return count;
  }

  /**
   * Estimate the row count for an index key
   */
  private double estimateRowCountForIndex(Session session, List<ValueRange> ranges, long indexId) {
    IndexStats indexStats = this.indexStatsMap.get(indexId);
    double totalCount = 0;

    for (ValueRange range : ranges) {
      int position = getOrdinalOfRange(session, range);

      if (position == 0 || isSingleColumnIndexWithNullRange(indexStats, range)) {
        double count = indexStats.estimateRowCount(session, ranges, modifiedCount);

        totalCount += count;
        continue;
      }

      Value value = range.getStarts().get(position);
      double selectivity = 0;
      if (indexStats.getHistogram().isOutOfRange(session, value)) {
        final int rangeSize = range.getStarts().size();
        final int columnSize = indexStats.getIndexInfo().getColumns().length;
        final double ndv = (double) indexStats.getHistogram().getNdv();
        final double rowCount = indexStats.getHistogram().getTotalRowCount();
        if (indexStats.getHistogram().getNdv() > 0 && rangeSize == columnSize && position == rangeSize) {
          selectivity = (double) this.modifiedCount / ndv / rowCount;
        } else {
          selectivity = (double) this.modifiedCount / OUT_OF_RANGE_BETWEEN_RATE / rowCount;
        }
      } else {
        selectivity = (double) indexStats.getCms().queryValue(value);
      }

      // Use histogram to estimate
      if (position != range.getStarts().size()) {
        List<Long> columnIds = indexToColumnIds.get(indexId);
        long columnId = -1;
        double count = 0;
        if (position < columnIds.size()) {
          columnId = columnIds.get(position);
        }

        Long idx = columnIdToIndexId.get(columnId);
        if (idx != null) {
          count = estimateRowCountByIndexedRanges(session, idx, Lists.newArrayList(range));
        } else {
          count = estimateRowCountByNonIndexedRanges(session, columnId, Lists.newArrayList(range));
        }

        selectivity *= count / indexStats.getHistogram().getTotalRowCount();
      }

      totalCount += selectivity * indexStats.getHistogram().getTotalRowCount();
    }

    return Math.min(totalCount, indexStats.getHistogram().getTotalRowCount());
  }

  // See TiDB (coll *HistColl) GetRowCountByIndexRange
  public double estimateRowCountByIndexedRanges(Session session, long indexId, List<ValueRange> ranges) {
    final IndexStats indexStats = this.indexStatsMap.get(indexId);

    if (indexStats == null || indexStats.isInValid(session)) {
      // TODO return pseudo count ?
      return 0;
    }

    final double increasingFactor = indexStats.getHistogram().estimateIncreasingFactor(this.rawRowCount);
    if (indexStats.getCms() != null) {
      // TODO add version check here so that estimateRowCountForIndex can only be called at the first time
      // refer to TiDB table.go -> GetRowCountByIndexRanges
      // If we have cms & histogram already, use them to calculate selectivity and do the estimation
      return increasingFactor * this.estimateRowCountForIndex(session, ranges, indexId);
    }

    // If we do not cms or histogram, do range-based estimation
    return increasingFactor * indexStats.estimateRowCount(session, ranges, modifiedCount);
  }

  private int getOrdinalOfRange(Session session, ValueRange range) {
    List<Value> starts = range.getStarts();
    List<Value> ends = range.getEnds();
    for (int i = 0; i < starts.size(); i++) {
      Value start = starts.get(i);
      Value end = ends.get(i);

      if (start.compareTo(session, end) != 0) {
        return i;
      }
    }

    return starts.size() - 1;
  }

  private boolean isSingleColumnIndexWithNullRange(IndexStats columnStats, ValueRange range) {
    if (columnStats.getIndexInfo().getColumns().length > 1) {
      return false;
    }

    return range.getFirstStart().isNull() && range.getFirstEnd().isNull();
  }

  private void updateFromSelectivity(Session session, List<Selectivity> selectivityList) {

    Map<Long, ColumnStats> newNonIndexedColumnStatsMap = Maps.newHashMap();

    Map<Long, IndexStats> newIndexStatsMap = Maps.newHashMap();

    for (Selectivity selectivity : selectivityList) {
      if (selectivity.getType() == SelectivityType.INDEX) {
        IndexStats indexStats = this.indexStatsMap.get(selectivity.getId());

        if (indexStats == null) {
          continue;
        }

        IndexStats newIndexStats = indexStats.newIndexStatsFromSelectivity(session,
                selectivity);

        newIndexStatsMap.put(selectivity.getId(), newIndexStats);
        continue;
      }

      ColumnStats oldNonIndexedColStats = this.nonIndexedColumnStatsMap.get(selectivity.getId());
      if (oldNonIndexedColStats == null) {
        continue;
      }

      final long ndv = (long) ((double) oldNonIndexedColStats.getHistogram().getNdv() * selectivity.getValue());
      Histogram histogram = new Histogram(oldNonIndexedColStats.getColumnInfo().getId(), ndv, 0);
      CountMinSketch cms = oldNonIndexedColStats.getCms();

      ColumnStats newNonIndexedColStats = new ColumnStats(cms, histogram, oldNonIndexedColStats
              .getColumnInfo());

      List<ValueRange> ranges = oldNonIndexedColStats.getHistogram().splitRange(session, selectivity.getRanges());

      if (ranges == null) {
        continue;
      }

      if (ranges.size() > 0) {
        // TODO deal with null values
      }

      // update histogram in the new stats table
      histogram.updateByRanges(session, ranges);

      newNonIndexedColumnStatsMap.put(selectivity.getId(), newNonIndexedColStats);
    }

    for (Map.Entry<Long, IndexStats> entry : this.indexStatsMap.entrySet()) {
      IndexStats indexedColStats = entry.getValue();
      Long indexId = entry.getKey();

      newIndexStatsMap.putIfAbsent(indexId, indexedColStats);
    }

    for (Map.Entry<Long, ColumnStats> entry : this.nonIndexedColumnStatsMap.entrySet()) {
      ColumnStats nonIndexedColStats = entry.getValue();
      Long nonIndexedId = entry.getKey();

      newNonIndexedColumnStatsMap.putIfAbsent(nonIndexedId, nonIndexedColStats);
    }
  }

  public long getRawRowCount() {
    return rawRowCount;
  }
}
