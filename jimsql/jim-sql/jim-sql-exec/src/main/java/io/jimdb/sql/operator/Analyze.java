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

package io.jimdb.sql.operator;

import static io.jimdb.sql.optimizer.statistics.StatsUtils.STATS_TABLE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import io.jimdb.core.Session;
import io.jimdb.common.exception.JimException;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.RowValueAccessor;
import io.jimdb.core.expression.Schema;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.model.meta.Table;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.model.result.impl.QueryExecResult;
import io.jimdb.core.plugin.store.Engine;
import io.jimdb.core.plugin.store.Transaction;
import io.jimdb.sql.executor.AnalyzeColumnsExecutor;
import io.jimdb.sql.executor.AnalyzeExecutor;
import io.jimdb.sql.executor.AnalyzeIndexExecutor;
import io.jimdb.sql.optimizer.statistics.ColumnStats;
import io.jimdb.sql.optimizer.statistics.IndexStats;
import io.jimdb.sql.optimizer.statistics.TableStats;
import io.jimdb.sql.optimizer.statistics.TableStatsManager;
import io.jimdb.core.values.Value;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * Analyze statement
 */
public class Analyze extends Operator {
  private static final Logger LOGGER = LoggerFactory.getLogger(Analyze.class);
  private final List<TableSource> tableSourceList;
  // TODO put it into config file
  private static final int CONCURRENCY = 10;

  public Analyze(@Nonnull List<TableSource> tableSourceList) {
    Objects.requireNonNull(tableSourceList);
    this.tableSourceList = tableSourceList;
  }

  public List<TableSource> getTableSourceList() {
    return tableSourceList;
  }

  @Override
  public OperatorType getOperatorType() {
    return OperatorType.ANALYZE;
  }

  @Override
  public Flux<ExecResult> execute(Session session) throws JimException {

    if (TableStatsManager.isStatsPushDownEnabled()) {
      LOGGER.debug("Executing stats push-down...");
      return executeStatsPushDown(session);
    }

    final TableSource tableSource = tableSourceList.get(0);
    final Table table = tableSource.getTable();
    final Schema schema = tableSource.getSchema();
    final Transaction transaction = session.getTxn();

    List<ColumnExpr> columnExprs = schema.getColumns();
    List<Integer> outputOffsets = columnExprs.stream().map(ColumnExpr::getOffset)
            .collect(Collectors.toCollection(() -> new ArrayList<>(columnExprs.size())));

    ColumnExpr[] resultColumns = columnExprs.toArray(new ColumnExpr[0]);

    return transaction.select(table, tableSource.getProcessors(), resultColumns, outputOffsets).map(execResult -> {
      TableStatsManager.updateTableStatsCache(session, table, (QueryExecResult) execResult);
      return convertToExecResult(session, TableStatsManager.getTableStats(table));
    });
  }

  private Flux<ExecResult> executeStatsPushDown(Session session) throws JimException {
    final TableSource tableSource = tableSourceList.get(0);
    final Table table = tableSource.getTable();
    final Engine storeEngine = session.getStoreEngine();

    Flux<AnalyzeExecutor> analyzeColumnsExecutor = Flux.just(new AnalyzeColumnsExecutor(session, table, table.getReadableColumns(), storeEngine));
    Flux<AnalyzeExecutor> analyzeIndexExecutors = Flux.just(table.getReadableIndices()).map(index -> new AnalyzeIndexExecutor(session, table, index, storeEngine));

    return analyzeIndexExecutors
            .concatWith(analyzeColumnsExecutor)
            .parallel()
            .runOn(Schedulers.parallel())
            .flatMap(e -> Mono.just(Objects.requireNonNull(e.execute(session).blockFirst())))
            .sequential()
            .onErrorContinue((throwable, o) -> LOGGER.error("Error occurs during merging analyze results {}", o))
            .collectList()
            .map(this::mergeExecResults)
            .flux();
  }

  private ExecResult mergeExecResults(List<QueryExecResult> queryExecResults) {
    LOGGER.debug("number of query results to be merge is {}", queryExecResults.size());
    return QueryExecResult.merge(queryExecResults);
  }

  private ExecResult convertToExecResult(Session session, TableStats tableStats) {

    ColumnExpr[] columnExprs = Arrays.stream(STATS_TABLE.getReadableColumns())
                                   .map(column -> new ColumnExpr(session.allocColumnID(), column)).toArray(ColumnExpr[]::new);

    if (tableStats == null) {
      return new QueryExecResult(columnExprs, null);
    }

    // every non-indexed column and index becomes a row in the constructed result
    final int rowCount = tableStats.getIndexStatsMap().size() + tableStats.getNonIndexedColumnStatsMap().size();
    List<ValueAccessor> valueAccessorList = Lists.newArrayListWithExpectedSize(rowCount);

    // construct result for column stats
    Map<Integer, ColumnStats> columnStatsMap = tableStats.getNonIndexedColumnStatsMap();
    for (Map.Entry<Integer, ColumnStats> entry: columnStatsMap.entrySet()) {
      ColumnStats columnStats = entry.getValue();
      Value[] values = ColumnStats.convertToValues(entry.getKey(), false, columnStats.getTotalRowCount(), columnStats.getHistogram(), columnStats.getCms());

      RowValueAccessor rowValueAccessor = new RowValueAccessor(values);
      valueAccessorList.add(rowValueAccessor);
    }

    // construct result for index stats
    Map<Integer, IndexStats> indexStatsMap = tableStats.getIndexStatsMap();
    for (Map.Entry<Integer, IndexStats> entry: indexStatsMap.entrySet()) {
      IndexStats indexStats = entry.getValue();
      Value[] values = IndexStats.convertToValues(entry.getKey(), true, indexStats.getTotalRowCount(), indexStats.getHistogram(), indexStats.getCms());

      RowValueAccessor rowValueAccessor = new RowValueAccessor(values);
      valueAccessorList.add(rowValueAccessor);
    }

    ValueAccessor[] rows = valueAccessorList.toArray(new ValueAccessor[0]);

    return new QueryExecResult(columnExprs, rows);
  }
}
