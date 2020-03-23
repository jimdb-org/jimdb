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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.jimdb.common.exception.BaseException;
import io.jimdb.common.utils.lang.NamedThreadFactory;
import io.jimdb.core.Session;
import io.jimdb.core.config.JimConfig;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.Schema;
import io.jimdb.core.model.meta.Column;
import io.jimdb.core.model.meta.Index;
import io.jimdb.core.model.meta.MetaData;
import io.jimdb.core.model.meta.Table;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.model.result.impl.QueryExecResult;
import io.jimdb.core.plugin.PluginFactory;
import io.jimdb.core.plugin.store.Transaction;
import io.jimdb.meta.client.MasterClient;
import io.jimdb.pb.Exprpb;
import io.jimdb.pb.Mspb;
import io.jimdb.pb.Processorpb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.core.publisher.Flux;

/**
 * TableStatsManager manages the process for table statistics creation and update.
 */
@SuppressFBWarnings()
public class TableStatsManager {
  private static final Logger LOG = LoggerFactory.getLogger(TableStatsManager.class);

  private static ScheduledExecutorService statsCollectorService;
  private static boolean enableStatsPushDown;

  // is stats collector enabled or not
  private static boolean enableBackgroundStatsCollector;

  private static MasterClient masterClient;
  private static int retry;

  private static final double DEFAULT_SAMPLE_RATIO = 1.0;
  private static final double MAX_SAMPLE_SIZE = 10000.0;
  private static final int SAMPLE_WAIT_TIME = 2000;

  // if a table has not been accessed for 30 minutes, it will be removed from the active table map
  private static final int TABLE_EXPIRATION_TIME = 30;

  private static Session session;

  private static Cache<Table, TableStats> tableStatsCache = CacheBuilder.newBuilder()
          .expireAfterWrite(TABLE_EXPIRATION_TIME, TimeUnit.MINUTES)
          .build();

  public static void init(JimConfig jimConfig) {
    final String metaAddr = jimConfig.getMasterAddr();
    final long clusterId = jimConfig.getMetaCluster();
    masterClient = new MasterClient(metaAddr, clusterId);
    retry = jimConfig.getRetry();

    final int statsRefreshPeriod = jimConfig.getServerConfig().getStatsRefreshPeriod();
    statsCollectorService = new ScheduledThreadPoolExecutor(1,
            new NamedThreadFactory("Statistics-Collector", true));

    // call master to retrieve the metadata of all tables and update global stats maps and active table cache
    session = new Session(PluginFactory.getSqlEngine(), PluginFactory.getStoreEngine());

    enableBackgroundStatsCollector = jimConfig.isEnableBackgroundStatsCollector();
    if (enableBackgroundStatsCollector) {
      statsCollectorService.scheduleWithFixedDelay(
              TableStatsManager::initTableStatsCache, statsRefreshPeriod, statsRefreshPeriod, TimeUnit.MILLISECONDS);
    }

    enableStatsPushDown = jimConfig.isEnableStatsPushDown();
  }

  public static boolean isStatsPushDownEnabled() {
    return enableStatsPushDown;
  }

  // Test purpose only
  public static void setEnableStatsPushDown(boolean isEnabled) {
    enableStatsPushDown = isEnabled;
  }

  @SuppressFBWarnings({ "PSC_PRESIZE_COLLECTIONS", "UEC_USE_ENUM_COLLECTIONS" })
  private static void initTableStatsCache() {
    Mspb.CountTableResponse getTableStatsResponse = masterClient.getAllTableStats(retry);
    if (getTableStatsResponse == null) {
      return;
    }

    // go through the table stat response and extract the stats info
    List<Mspb.Counter> counterList = getTableStatsResponse.getListList();
    for (Mspb.Counter counter : counterList) {
      Table table = MetaData.Holder.get().getTable(counter.getDbId(), counter.getTableId());

      // TODO if table is null, should we update the metadata?
      if (table == null) {
        continue;
      }

      // initialize active table cache
      final long rowCount = counter.getCount();
      final Schema schema = new Schema(session, table.getReadableColumns());
      tableStatsCache.put(table, new TableStats(table, rowCount));
    }
  }

  public static double calculateDataSampleRatio(Table table) {
    // the table row count should be presented at this point
    TableStats tableStats = tableStatsCache.getIfPresent(table);
    if (tableStats == null) {
      LOG.warn("Row count for table {} could not be found", table.getName());
      return DEFAULT_SAMPLE_RATIO;
    }

    final long rowCount = tableStats.getRetrievedRowCount();

    if (rowCount <= 0) {
      LOG.warn("Row count for table {} is less than 0", table.getName());
      return DEFAULT_SAMPLE_RATIO;
    }

    return Math.min(MAX_SAMPLE_SIZE / (double) rowCount, 1.0);
  }

  private static void refreshRetrievedTableRowCounts(Set<Table> tables) {

    // only obtain the row counts of the given tables
    Mspb.CountTableResponse getTableStatsResponse = masterClient.getTableStats(tables, retry);
    if (getTableStatsResponse == null) {
      return;
    }

    // go through the table stat response and extract the stats info
    List<Mspb.Counter> counterList = getTableStatsResponse.getListList();
    for (Mspb.Counter counter : counterList) {
      Table table = MetaData.Holder.get().getTable(counter.getDbId(), counter.getTableId());
      long rowCount = counter.getCount();

      if (table == null) {
        LOG.warn("table {} in DB {} is not presented in metadata", counter.getTableId(), counter.getDbId());
        continue;
      }

      TableStats tableStats = tableStatsCache.getIfPresent(table);
      if (tableStats == null) {
        tableStatsCache.put(table, new TableStats(table, rowCount));
        continue;
      }

      tableStats.setRetrievedRowCount(rowCount);
    }
  }

  /**
   * Update the table stats cache for a given set of tables
   *
   * @param tables the given set of tables for update
   */
  public static void updateTableStatsCache(Set<Table> tables) {
    CompletionService<Flux<ExecResult>> completionService =
            new ExecutorCompletionService<>(Executors.newFixedThreadPool(2));

    // we need firstly refresh the table row count (in order to calculate data sample ratio) before update the table stats
    refreshRetrievedTableRowCounts(tables);

    // for all the tables recently used, we update the corresponding table stats
    Map<Future<Flux<ExecResult>>, Table> futureTableMap = tables.stream().collect(
            Collectors.toMap(table -> completionService.submit(new CollectTableSamplesTask(table, session.getTxn())), table -> table));

    int failedStatsCount = 0;
    int successfulStatsCount = 0;
    while (successfulStatsCount + failedStatsCount != futureTableMap.size()) {
      // any error on a table should not impact the stats fetching for other tables
      try {
        Future<Flux<ExecResult>> resultFuture = completionService.take();
        Table table = futureTableMap.get(resultFuture);
        resultFuture.get()
                .onErrorContinue((e, o) -> LOG.error("Failed to fetch the stats for one table {} : {}", table.getName(), e.getMessage()))
                .subscribe(r -> updateTableStatsCache(session, table, (QueryExecResult) r));

        successfulStatsCount++;
      } catch (Exception e) {
        failedStatsCount++;
        LOG.error("Error in fetching table stats: {}, failedStatsCount = {}, successfulStatsCount = {}", e.getMessage(), failedStatsCount, successfulStatsCount);
      }
    }

    LOG.info("Table stats cache has been updated. Current cache size = {}, failedStatsCount = {}", tableStatsCache.size(), failedStatsCount);
  }

  /**
   * Update the table stats cache for the table whose sample data are obtained from DS directly (no stats-push-down)
   *
   * @param session    session of the background thread for updating table stats
   * @param table      table whose stats needs to be updated
   * @param execResult sample data from DS
   */
  public static void updateTableStatsCache(Session session, Table table, QueryExecResult execResult) {
    // at this point, we may not have the row count retrieved from master yet, but it does not matter since we already got the result
    TableStats tableStats = tableStatsCache.getIfPresent(table);
    if (tableStats == null) {
      tableStats = new TableStats(table, 0);
    }

    tableStats.update(session, execResult);

    // put the table stats back to the cache
    tableStatsCache.put(table, tableStats);
  }

  /**
   * Update the index stats of the given table and its corresponding cache
   *
   * @param table          table whose stats needs to be updated
   * @param index          index of the table whose stats needs to be updated
   * @param histogram      histogram of the index
   * @param countMinSketch cms of the index
   */
  public static void updateTableStatsCache(Table table, Index index, Histogram histogram, CountMinSketch countMinSketch) {
    TableStats tableStats = tableStatsCache.getIfPresent(table);

    if (tableStats == null) {
      tableStats = new TableStats(table, 0);
    }

    tableStats.updateIndexStats(index, histogram, countMinSketch);
    tableStatsCache.put(table, tableStats);
  }

  /**
   * Update the column stats of the given table and its corresponding cache
   *
   * @param table          table whose stats needs to be updated
   * @param column         column of the table whose stats needs to be updated
   * @param histogram      histogram of the column
   * @param countMinSketch cms of the column
   */
  public static void updateTableStatsCache(Table table, Column column, Histogram histogram, CountMinSketch countMinSketch) {
    TableStats tableStats = tableStatsCache.getIfPresent(table);

    if (tableStats == null) {
      tableStats = new TableStats(table, 0);
    }

    tableStats.updateColumnStats(column, histogram, countMinSketch);
    tableStatsCache.put(table, tableStats);
  }

  public static void addToCache(Table table, TableStats tableStats) {
    tableStatsCache.put(table, tableStats);
  }

  public static void shutdown() {
    statsCollectorService.shutdown();
  }

  public static void resetAllTableStats() {
    tableStatsCache.cleanUp();
  }

  // We need to ensure all the schemas are created by the same session in StatsManager
  public static Schema schemaFromStatsManager(Table table) {
    return new Schema(session, table.getReadableColumns());
  }

  // get the stats for the given table
  public static TableStats getTableStats(Table table) {

    // first try to get the stats from the cache
    TableStats tableStats = tableStatsCache.getIfPresent(table);

    if (tableStats != null || !enableBackgroundStatsCollector) {
      return tableStats;
    }

    // TODO then try to get the table stats from the remote table stored on DS

    // finally if we still cannot find the corresponding table stats, do the calculation on-the-fly
    updateTableStatsCache(Collections.singleton(table));

    tableStats = tableStatsCache.getIfPresent(table);
    if (tableStats == null) {
      LOG.error("Something wrong with updating the table stats for table {}", table.getName());
      return new TableStats(table, 0);
    }

    return tableStats;
  }

  /**
   * Task to collect single table statistics
   */
  @SuppressFBWarnings
  private static final class CollectTableSamplesTask implements Callable<Flux<ExecResult>> {
    private Table table;
    private Transaction transaction;

    CollectTableSamplesTask(Table table, Transaction transaction) {
      this.table = table;
      this.transaction = transaction;
    }

    @Override
    public Flux<ExecResult> call() throws BaseException {
      List<Integer> outputOffsets = Arrays.stream(table.getReadableColumns()).map(Column::getOffset)
              .collect(Collectors.toList());

      Processorpb.DataSample dataSample = Processorpb.DataSample.newBuilder()
              .addAllColumns(getAllColumns(table.getReadableColumns()))
              .setRatio(calculateDataSampleRatio(table))
              .build();

      Processorpb.Processor.Builder processorBuilder = Processorpb.Processor.newBuilder()
              .setType(Processorpb.ProcessorType.DATA_SAMPLE_TYPE)
              .setDataSample(dataSample);

      ColumnExpr[] columnExprs = Arrays.stream(table.getReadableColumns())
              .map(column -> new ColumnExpr(session.allocColumnID(), column)).toArray(ColumnExpr[]::new);

      return session.getStoreEngine().select(session, table, Lists.newArrayList(processorBuilder), columnExprs, outputOffsets);
    }

    private List<Exprpb.ColumnInfo> getAllColumns(Column[] columns) {
      List<Exprpb.ColumnInfo> columnInfoList = new ArrayList<>(columns.length);
      for (Column column : columns) {
        columnInfoList.add(Exprpb.ColumnInfo.newBuilder()
                .setId(column.getId().intValue())
                .setTyp(column.getType().getType())
                .setUnsigned(column.getType().getUnsigned()).build());
      }
      return columnInfoList;
    }
  }
}
