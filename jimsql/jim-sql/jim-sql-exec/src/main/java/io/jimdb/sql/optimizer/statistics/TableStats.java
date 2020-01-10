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
import static io.jimdb.sql.optimizer.statistics.StatsUtils.DEFAULT_TOPN;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import javax.annotation.Nonnull;

import io.jimdb.core.Session;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.Schema;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.model.meta.Column;
import io.jimdb.core.model.meta.Index;
import io.jimdb.core.model.meta.Table;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.values.Value;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

/**
 * TableStatistics represents the statistics for a table
 */
@SuppressFBWarnings
public class TableStats {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableStats.class);

  private Map<Integer, ColumnStats> nonIndexedColumnStatsMap;

  private Map<Integer, IndexStats> indexStatsMap;

  private long retrievedRowCount; // retrieved table row count from master
  private long estimatedRowCount; // estimated table row count based on stats

  // TODO currently unused
  private long modifiedCount; // modified row count in the table

  // To be used to update table stats
  private Schema schema; // all-column schema
  private Table table;

  // Note that the session can only be the session in TableStatsManager
  public TableStats(Session session, Table table, Schema schema, long retrievedRowCount) {
    this.table = table;
    this.nonIndexedColumnStatsMap = Maps.newHashMapWithExpectedSize(table.getReadableColumns().length);
    this.indexStatsMap = Maps.newHashMapWithExpectedSize(table.getReadableIndices().length);
    this.estimatedRowCount = 0;
    this.modifiedCount = 0;
    this.retrievedRowCount = retrievedRowCount;
    this.schema = new Schema(session, table.getReadableColumns());
  }

  public Map<Integer, ColumnStats> getNonIndexedColumnStatsMap() {
    return nonIndexedColumnStatsMap;
  }

  public Map<Integer, IndexStats> getIndexStatsMap() {
    return indexStatsMap;
  }

  public void updateIndexStats(Index index, Histogram histogram, CountMinSketch countMinSketch) {
    if (!indexStatsMap.containsKey(index.getId())) {
      indexStatsMap.put(index.getId(), new IndexStats(countMinSketch, histogram, index));
    }

    IndexStats indexStats = indexStatsMap.get(index.getId());
    indexStats.reset(countMinSketch, histogram);
  }

  public void updateColumnStats(Column column, Histogram histogram, CountMinSketch countMinSketch) {
    if (!nonIndexedColumnStatsMap.containsKey(column.getId())) {
      nonIndexedColumnStatsMap.put(column.getId(), new ColumnStats(countMinSketch, histogram, column));
    }

    ColumnStats columnStats = nonIndexedColumnStatsMap.get(column.getId());
    columnStats.reset(countMinSketch, histogram);
  }

  public long getModifiedCount() {
    return modifiedCount;
  }

  public Schema getSchema() {
    return this.schema;
  }

  public long getEstimatedRowCount() {
    return estimatedRowCount;
  }

  public void setEstimatedRowCount(long estimatedRowCount) {
    this.estimatedRowCount = estimatedRowCount;
  }

  public long getRetrievedRowCount() {
    return retrievedRowCount;
  }

  public void setRetrievedRowCount(long retrievedRowCount) {
    this.retrievedRowCount = retrievedRowCount;
  }

  /**
   * Update the table stats based on the value received from DS
   *
   * @param execResult data received from DS
   */
  public void update(Session session, ExecResult execResult) {
    ColumnExpr[] columnExprs = execResult.getColumns();
    Column[] columns = table.getReadableColumns();

    if (columns.length != columnExprs.length) {
      // something wrong with the result
      LOGGER.error("number of columns in the table ({}) is not equal to the number of columns received from DS ({})",
              columns.length, columnExprs.length);
      return;
    }

    final Index[] indices = table.getReadableIndices();
    if (indices == null) {
      // something wrong here
      LOGGER.error("indices of the table should not be null");
      return;
    }

    // indexes include both the primary and non-primary indexes
//    Set<Integer> indexedColOffsets = Arrays.stream(indices)
//            .flatMap(i -> Arrays.stream(i.getColumns()).map(Column::getOffset))
//            .collect(Collectors.toSet());

    final int[] nonIndexColOffsets = Arrays.stream(columns)
            .map(Column::getOffset)
            //.filter(i -> !indexedColOffsets.contains(i))
            .filter(i -> i != 0) // we need to filter the primary which will be process in index stats
            .mapToInt(i -> i)
            .toArray();

    final int length = indices.length + nonIndexColOffsets.length;
    // compute the row count by returned result size and the sample ratio
    this.estimatedRowCount = (long) (execResult.size() / TableStatsManager.calculateDataSampleRatio(table));

    // building the sample collector before generating column stats
    Tuple2<IndexSampleCollector[], ColumnSampleCollector[]> sampleCollectors =
            buildSampleCollectors(execResult, columns, indices, nonIndexColOffsets, length);

    IndexSampleCollector[] indexSampleCollectors = sampleCollectors.getT1();
    for (int i = 0; i < indices.length; i++) {
      IndexSampleCollector sampleCollector = indexSampleCollectors[i];
      //long rowCount = Math.max(this.rowCount, sampleCollector.sampleCount());
      sampleCollector.calculateTotalSize(estimatedRowCount);

      Index indexInfo = indices[i];
      IndexStats indexStats = buildIndexStats(session, indexInfo, sampleCollector, this.estimatedRowCount);
      this.indexStatsMap.put(indexStats.getIndexInfo().getId(), indexStats);
    }

    ColumnSampleCollector[] columnSampleCollectors = sampleCollectors.getT2();
    for (int i = 0; i < nonIndexColOffsets.length; i++) {
      ColumnSampleCollector sampleCollector = columnSampleCollectors[i];
      Column columnInfo = columns[nonIndexColOffsets[i]];
      // TODO we must ensure that the samples are sorted by the row id.
      ColumnStats columnStats = buildNonIndexedColumnStats(session, columnInfo, sampleCollector, this.estimatedRowCount);
      this.nonIndexedColumnStatsMap.put(columnStats.getColumnInfo().getId(), columnStats);
    }
  }

  private Tuple2<IndexSampleCollector[], ColumnSampleCollector[]> buildSampleCollectors(ExecResult execResult,
                                                                                        Column[] columns,
                                                                                        @Nonnull Index[] indices,
                                                                                        int[] nonIndexColOffsets,
                                                                                        int collectorSize) {

    IndexSampleCollector[] indexSampleCollectors = Arrays.stream(indices)
            .map(item -> new IndexSampleCollector(DEFAULT_ROW_COUNT, item.getColumns().length))
            .toArray(IndexSampleCollector[]::new);

    ColumnSampleCollector[] columnSampleCollectors = IntStream.range(0, collectorSize - indices.length)
            .mapToObj(i -> new ColumnSampleCollector(DEFAULT_ROW_COUNT))
            .toArray(ColumnSampleCollector[]::new);

    for (int i = 0; i < execResult.size(); i++) {
      ValueAccessor row = execResult.getRow(i);

      for (int j = 0; j < indices.length; j++) {
        Index index = indices[j];
        Value[] indexValues = Arrays.stream(index.getColumns())
                .map(c -> row.get(c.getOffset()))
                .toArray(Value[]::new);

        indexSampleCollectors[j].collect(indexValues);
      }

      for (int j = 0; j < nonIndexColOffsets.length; j++) {
        int offset = nonIndexColOffsets[j];
        Column column = columns[offset];
        Value value = row.get(column.getOffset());
        columnSampleCollectors[j].collect(value);
      }
    }

    return Tuples.of(indexSampleCollectors, columnSampleCollectors);
  }

  private ColumnStats buildNonIndexedColumnStats(Session session, Column columnInfo,
                                                 ColumnSampleCollector sampleCollector, long rowCount) {
    // convert sample to byte arrays first
    final int sampleCount = sampleCollector.sampleCount();
    List<byte[]> data = Lists.newArrayListWithExpectedSize(sampleCount);

    // TODO we actually scan the samples twice here: one to get the data[][] and the other to build the histogram.
    //  These two scans can be merged into a single scan
    for (int i = 0; i < sampleCount; i++) {
      SampleItem sampleItem = sampleCollector.getSample(i);
      sampleItem.setOrdinal(i);
      if (sampleItem.getValue().isNull()) {
        continue;
      }
      data.add(sampleItem.getValue().toByteArray());
    }

    CountMinSketch countMinSketch = new CountMinSketch.CountMinSketchBuilder(data, rowCount).build();

    final long ndv = countMinSketch.getEstimatedNDV();
    Histogram histogram = new Histogram.DefaultIndexOrColumnHistogramBuilder(sampleCollector, columnInfo.getId(), ndv)
            .withSession(session)
            .withRowCount(rowCount)
            .build();

    return new ColumnStats(countMinSketch, histogram, columnInfo);
  }

  // Build index stats. Note that an index can consist of multiple columns.
  private IndexStats buildIndexStats(Session session, Index indexInfo, IndexSampleCollector sampleCollector, long
          rowCount) {
    final int sampleCount = sampleCollector.sampleCount();
    final int columnCount = indexInfo.getColumns().length;

    CountMinSketch countMinSketch =
            new CountMinSketch.CountMinSketchBuilder(sampleCollector.getPrefixBytes(0), rowCount).build();

    for (int i = 1; i < columnCount; i++) {
      CountMinSketch newCountMinSketch =
              new CountMinSketch.CountMinSketchBuilder(sampleCollector.getPrefixBytes(i), rowCount).build();

      countMinSketch.merge(newCountMinSketch, DEFAULT_TOPN);
    }

    final long ndv = countMinSketch.getEstimatedNDV();
    Histogram histogram = new Histogram.DefaultIndexOrColumnHistogramBuilder(sampleCollector, indexInfo.getId(), ndv)
            .withSession(session).withRowCount(rowCount)
            .build();

    return new IndexStats(countMinSketch, histogram, indexInfo);
  }

  @VisibleForTesting
  public ColumnStats getColumnStats(Long columnId) {
    return nonIndexedColumnStatsMap.get(columnId);
  }
}
