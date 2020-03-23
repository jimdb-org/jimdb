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

package io.jimdb.sql.executor;

import static io.jimdb.sql.optimizer.statistics.StatsUtils.DEFAULT_CMS_DEPTH;
import static io.jimdb.sql.optimizer.statistics.StatsUtils.DEFAULT_CMS_WIDTH;
import static io.jimdb.sql.optimizer.statistics.StatsUtils.DEFAULT_NUM_BUCKETS;
import static io.jimdb.sql.optimizer.statistics.StatsUtils.MAX_NUM_BUCKETS;
import static io.jimdb.sql.optimizer.statistics.StatsUtils.STATS_TABLE;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import io.jimdb.core.Session;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.RowValueAccessor;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.expression.ValueRange;
import io.jimdb.core.model.meta.Column;
import io.jimdb.core.model.meta.Index;
import io.jimdb.core.model.meta.Table;
import io.jimdb.core.model.result.impl.QueryExecResult;
import io.jimdb.pb.Exprpb;
import io.jimdb.pb.Statspb;
import io.jimdb.core.plugin.store.Engine;
import io.jimdb.sql.optimizer.physical.RangeBuilder;
import io.jimdb.sql.optimizer.statistics.CountMinSketch;
import io.jimdb.sql.optimizer.statistics.Histogram;
import io.jimdb.sql.optimizer.statistics.IndexStats;
import io.jimdb.sql.optimizer.statistics.TableStatsManager;
import io.jimdb.core.values.Value;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

/**
 * Analyze executor for analyzing index stats
 */
public class AnalyzeIndexExecutor extends AnalyzeExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(AnalyzeIndexExecutor.class);

  private Session session;
  private Table table;
  private Index index;
  private Engine storeEngine;

  public AnalyzeIndexExecutor(Session session, Table table, Index index, Engine storeEngine) {
    this.session = session;
    this.table = table;
    this.index = index;
    this.storeEngine = storeEngine;
  }

  @Override
  // Execute the index push-down
  public Flux<QueryExecResult> execute(Session session) {

//    ColumnExpr[] columnExprs = Arrays.stream(STATS_TABLE.getColumns())
//                                       .map(column -> new ColumnExpr(session.allocColumnID(), column)).toArray(ColumnExpr[]::new);
//
//    return Flux.just(new QueryExecResult(columnExprs, null));

    List<ValueRange> ranges = RangeBuilder.fullRangeList();
    // TODO
    // For single-column index, we do not load null rows from TiKV, so the built histogram would not include
    // null values, and its `NullCount` would be set by result of another distsql call to get null rows.
    // For multi-column index, we cannot define null for the rows, so we still use full range, and the rows
    // containing null fields would exist in built histograms. Note that, the `NullCount` of histograms for
    // multi-column index is always 0 then.
    if (index.getColumns().length == 1) {
      // todo ranges should be full range and also not null
    }

    final List<Integer> columnIds = Arrays.stream(index.getColumns()).map(Column::getId).collect(Collectors.toList());

    // fetch the analyze results from DS
    Flux<List<Tuple2<Statspb.Histogram, Statspb.CMSketch>>> rawResult = fetchAnalyzeResult(ranges);

    LOGGER.debug("successfully fetched analyze results from DS for index {} that has columns {} ... ", index.getId(), columnIds);

    // build the stats from the raw results, also update the stats table
    Flux<Tuple2<Histogram, CountMinSketch>> tuple2 = buildStats(rawResult);

    LOGGER.debug("finished building stats for index {} that has columns {} ... ", index.getId(), columnIds);


    // convert to ExecResult
    Flux<QueryExecResult> result = tuple2.map(this::convertToExecResult);

    LOGGER.debug("finished converting results for index {} that has columns {} ... ", index.getId(), columnIds);
    return result;
  }


  private QueryExecResult convertToExecResult(Tuple2<Histogram, CountMinSketch> tuple) {

    final Histogram histogram = tuple.getT1();
    final CountMinSketch countMinSketch = tuple.getT2();

    Value[] values = IndexStats.convertToValues(index.getId(), true, (long) histogram.getTotalRowCount(), histogram, countMinSketch);

    RowValueAccessor rowValueAccessor = new RowValueAccessor(values);

    ColumnExpr[] columnExprs = Arrays.stream(STATS_TABLE.getReadableColumns())
                                       .map(column -> new ColumnExpr(session.allocColumnID(), column)).toArray(ColumnExpr[]::new);

    return new QueryExecResult(columnExprs, new ValueAccessor[]{rowValueAccessor});
  }

  // Build the stats based on the given analyzeResult obtained from DS
  private Flux<Tuple2<Histogram, CountMinSketch>> buildStats(Flux<List<Tuple2<Statspb.Histogram, Statspb.CMSketch>>> analyzeResult) {
    return analyzeResult.map(this::buildStatsFromResult);
  }

  private Tuple2<Histogram, CountMinSketch> buildStatsFromResult(List<Tuple2<Statspb.Histogram, Statspb.CMSketch>> tupleList) {
    Objects.requireNonNull(tupleList, "result from DS should not be null");

    if (tupleList.isEmpty()) {
      LOGGER.warn("index analysis result from DS is empty, which is unexpected...");
      // TODO is there a better way to do this?
      return Tuples.of(new Histogram(-1, 0, 0), new CountMinSketch(0, 0));
    }

    Tuple2<Statspb.Histogram, Statspb.CMSketch> tuple = tupleList.get(0);

    // TODO we need to make sure t1 and t2 are not null here
    Histogram histogram = Histogram.fromPb(tuple.getT1());
    CountMinSketch cms = CountMinSketch.fromPb(tuple.getT2());

    for (int i = 1; i < tupleList.size(); i++) {
      tuple = tupleList.get(i);
      histogram = histogram.merge(session, Histogram.fromPb(tuple.getT1()), DEFAULT_NUM_BUCKETS);
      cms.merge(CountMinSketch.fromPb(tuple.getT2()), 0);
    }

    // update the cached table stats
    TableStatsManager.updateTableStatsCache(table, index, histogram, cms);

    return Tuples.of(histogram, cms);
  }

  // fetch the pushed-down analysis result
  private Flux<List<Tuple2<Statspb.Histogram, Statspb.CMSketch>>> fetchAnalyzeResult(List<ValueRange> ranges) {

    final List<Exprpb.ColumnInfo> columnInfoList =
            Arrays.stream(index.getColumns()).map(column -> Exprpb.ColumnInfo.newBuilder()
                                                                    .setId(Math.toIntExact(column.getId()))
                                                                    .setUnsigned(false)
                                                                    .setTyp(column.getType().getType())
                                                                    .build())
                    .collect(Collectors.toList());


    Statspb.IndexStatsRequest.Builder reqBuilder = Statspb.IndexStatsRequest.newBuilder()
                                                           .setBucketMax(MAX_NUM_BUCKETS)
                                                           .setCmsketchDepth(DEFAULT_CMS_DEPTH)
                                                           .setCmsketchWidth(DEFAULT_CMS_WIDTH)
                                                           .addAllColumnsInfo(columnInfoList);

    return storeEngine.analyzeIndex(session, table, index, this.session.getStmtContext().getTimeout(), ranges, reqBuilder);
  }
}
