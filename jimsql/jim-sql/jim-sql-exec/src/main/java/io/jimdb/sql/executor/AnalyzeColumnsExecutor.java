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

package io.jimdb.sql.executor;

import static io.jimdb.sql.optimizer.statistics.StatsUtils.DEFAULT_CMS_DEPTH;
import static io.jimdb.sql.optimizer.statistics.StatsUtils.DEFAULT_CMS_WIDTH;
import static io.jimdb.sql.optimizer.statistics.StatsUtils.DEFAULT_TOPN;
import static io.jimdb.sql.optimizer.statistics.StatsUtils.MAX_NUM_BUCKETS;
import static io.jimdb.sql.optimizer.statistics.StatsUtils.MAX_SAMPLE_SIZE;
import static io.jimdb.sql.optimizer.statistics.StatsUtils.MAX_SKETCH_SIZE;
import static io.jimdb.sql.optimizer.statistics.StatsUtils.STATS_TABLE;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import io.jimdb.core.Session;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.RowValueAccessor;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.expression.ValueRange;
import io.jimdb.core.model.meta.Column;
import io.jimdb.core.model.meta.Table;
import io.jimdb.core.model.result.impl.QueryExecResult;
import io.jimdb.pb.Exprpb;
import io.jimdb.pb.Statspb;
import io.jimdb.core.plugin.store.Engine;
import io.jimdb.sql.optimizer.physical.RangeBuilder;
import io.jimdb.sql.optimizer.statistics.ColumnSampleCollector;
import io.jimdb.sql.optimizer.statistics.ColumnStats;
import io.jimdb.sql.optimizer.statistics.CountMinSketch;
import io.jimdb.sql.optimizer.statistics.Histogram;
import io.jimdb.sql.optimizer.statistics.TableStatsManager;
import io.jimdb.core.values.Value;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

/**
 * Analyze executor for analyzing column stats
 */
@SuppressFBWarnings("EI_EXPOSE_REP2")
public class AnalyzeColumnsExecutor extends AnalyzeExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(AnalyzeColumnsExecutor.class);
  private Session session;
  private Table table;
  private Column[] columns;
  private Engine storeEngine;

  public AnalyzeColumnsExecutor(final Session session, final Table table, final Column[] columns, final Engine storeEngine) {
    this.session = session;
    this.table = table;
    this.columns = columns;
    this.storeEngine = storeEngine;
  }

  @Override
  // Execute the columns push-down
  public Flux<QueryExecResult> execute(Session session) {

    //ColumnExpr[] columnExprs = Arrays.stream(STATS_TABLE.getColumns())
    //                                   .map(column -> new ColumnExpr(session.allocColumnID(), column)).toArray(ColumnExpr[]::new);

    //return Flux.just(new QueryExecResult(columnExprs, null));

    // TODO is the primary key signed or unsigned ? see TiDB AnalyzeColumnsExec
    List<ValueRange> ranges = Collections.singletonList(RangeBuilder.fullRange()); //RangeBuilder.buildFullIntegerRange(true);

    final List<Integer> columnIds = Arrays.stream(columns).map(Column::getId).collect(Collectors.toList());

    // fetch the analyze results from DS
    Flux<List<Tuple2<Statspb.Histogram, List<Statspb.SampleCollector>>>> rawResult = fetchAnalyzeResult(ranges);
    LOGGER.debug("successfully fetched analyze results from DS for columns {} ... ", columnIds);

    // build the stats from the raw results, also update the stats table
    Flux<List<Tuple2<Histogram, CountMinSketch>>> tuple2 = buildStats(rawResult);

    LOGGER.debug("finished building stats for columns {} ... ", columnIds);

    // convert to ExecResult
    Flux<QueryExecResult> result = tuple2.map(this::convertToExecResult);

    LOGGER.debug("finished converting results for columns {} ... ", columnIds);

    return result;
  }

  private QueryExecResult convertToExecResult(List<Tuple2<Histogram, CountMinSketch>> tupleList) {

    ColumnExpr[] columnExprs = Arrays.stream(STATS_TABLE.getReadableColumns())
                                       .map(column -> new ColumnExpr(session.allocColumnID(), column)).toArray(ColumnExpr[]::new);

    ValueAccessor[] rows = new ValueAccessor[columns.length];

    // we omit the first column which is the primary
    if (tupleList.size() != columns.length - 1) {
      LOGGER.error("received tuple size {} not equal to the columns size ({}) - 1", tupleList.size(), columns.length);
      return new QueryExecResult(columnExprs, null);
    }

    for (int i = 0; i < tupleList.size(); i++) {
      final Histogram histogram = tupleList.get(i).getT1();
      final CountMinSketch countMinSketch = tupleList.get(i).getT2();

      Value[] values = ColumnStats.convertToValues(columns[i].getId(), false, (long) histogram.getTotalRowCount(), histogram, countMinSketch);

      RowValueAccessor rowValueAccessor = new RowValueAccessor(values);
      rows[i] = rowValueAccessor;
    }

    return new QueryExecResult(columnExprs, rows);
  }

  // Build the stats based on the given analyzeResult obtained from DS
  private Flux<List<Tuple2<Histogram, CountMinSketch>>> buildStats(Flux<List<Tuple2<Statspb.Histogram, List<Statspb.SampleCollector>>>> analyzeResult) {
    return analyzeResult.map(this::buildStatsFromResult);
  }

  private List<Tuple2<Histogram, CountMinSketch>> buildStatsFromResult(List<Tuple2<Statspb.Histogram, List<Statspb.SampleCollector>>> tupleList) {
    Objects.requireNonNull(tupleList, "Result from DS should not be null");

    if (tupleList.isEmpty()) {
      LOGGER.warn("Index analysis result from DS is empty, which is unexpected...");
      // TODO is there a better way to do this?
      return Collections.singletonList(Tuples.of(new Histogram(-1, 0, 0), new CountMinSketch(0, 0)));
    }

    // initialize the sample collectors
    final int sampleCollectorSize = tupleList.get(0).getT2().size();
    List<ColumnSampleCollector> sampleCollectors = Lists.newArrayListWithExpectedSize(sampleCollectorSize);

    // we omit the first column here because the fist column is always the primary,
    // which will be analyzed during index stats analysis anyways
    if (sampleCollectorSize != columns.length - 1) {
      LOGGER.error("Expect sample collectors with size of {}, but it actually has size of {} ... ", columns.length, sampleCollectorSize);
    }

    for (Statspb.SampleCollector pbSampleCollector : tupleList.get(0).getT2()) {
      ColumnSampleCollector sampleCollector = new ColumnSampleCollector(MAX_SAMPLE_SIZE);
      ColumnSampleCollector newSampleCollector = ColumnSampleCollector.fromPb(pbSampleCollector);
      sampleCollector.merge(newSampleCollector);
      sampleCollectors.add(sampleCollector);
    }

    // update the sample collectors
    for (int i = 1; i < tupleList.size(); i++) {
      List<Statspb.SampleCollector> pbSampleCollectors = tupleList.get(i).getT2();
      for (int j = 0; j < pbSampleCollectors.size(); j++) {
        ColumnSampleCollector newSampleCollector = ColumnSampleCollector.fromPb(pbSampleCollectors.get(j));
        sampleCollectors.get(j).merge(newSampleCollector);
      }
    }

    List<Tuple2<Histogram, CountMinSketch>> result = Lists.newArrayListWithExpectedSize(columns.length);
    // build histogram and cms for each column
    for (int i = 1; i < columns.length; i++) {
      ColumnSampleCollector sampleCollector = sampleCollectors.get(i - 1);
      sampleCollector.extractTopN(DEFAULT_TOPN);
      for (int j = 0; j < sampleCollectors.get(i - 1).sampleCount(); j++) {

        sampleCollector.getSample(j).setOrdinal(j);
        // TODO DecodeColumnValue ? AnalyzeColumnsExec.buildStats
      }

      CountMinSketch countMinSketch = sampleCollector.getCms();
      final long ndv = countMinSketch.getEstimatedNDV();
      Histogram histogram =  new Histogram.DefaultIndexOrColumnHistogramBuilder(sampleCollector, columns[i].getId(), ndv)
                                     .withSession(session)
                                     .withRowCount(sampleCollector.sampleCount())
                                     .build();

      // update the cached table stats
      TableStatsManager.updateTableStatsCache(table, columns[i], histogram, countMinSketch);
      result.add(Tuples.of(histogram, countMinSketch));
    }

    return result;
  }

  // fetch the pushed-down analysis result
  private Flux<List<Tuple2<Statspb.Histogram, List<Statspb.SampleCollector>>>> fetchAnalyzeResult(List<ValueRange> ranges) {

    final List<Exprpb.ColumnInfo> columnInfoList = Arrays.stream(columns).map(column ->
        Exprpb.ColumnInfo.newBuilder()
                .setId(Math.toIntExact(column.getId()))
                .setUnsigned(false)
                .setTyp(column.getType().getType())
                .build()
    ).collect(Collectors.toList());

    Statspb.ColumnsStatsRequest.Builder reqBuilder = Statspb.ColumnsStatsRequest.newBuilder()
                                                             .setBucketMax(MAX_NUM_BUCKETS)
                                                             .setCmsketchDepth(DEFAULT_CMS_DEPTH)
                                                             .setCmsketchWidth(DEFAULT_CMS_WIDTH)
                                                             .setSampleMax(MAX_SAMPLE_SIZE)
                                                             .setSketchMax(MAX_SKETCH_SIZE)
                                                             .addAllColumnsInfo(columnInfoList);



    return storeEngine.analyzeColumns(table, columns, this.session.getStmtContext().getTimeout(), ranges, reqBuilder);
  }

}
