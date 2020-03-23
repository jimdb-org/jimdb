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
package io.jimdb.test.mock.store;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.jimdb.common.exception.BaseException;
import io.jimdb.core.Session;
import io.jimdb.core.config.JimConfig;
import io.jimdb.core.expression.Assignment;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.RowValueAccessor;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.expression.ValueRange;
import io.jimdb.core.model.meta.Column;
import io.jimdb.core.model.meta.Index;
import io.jimdb.core.model.meta.RangeInfo;
import io.jimdb.core.model.meta.Table;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.model.result.QueryResult;
import io.jimdb.core.model.result.impl.DMLExecResult;
import io.jimdb.core.model.result.impl.QueryExecResult;
import io.jimdb.core.plugin.store.Engine;
import io.jimdb.core.plugin.store.Transaction;
import io.jimdb.core.values.BinaryValue;
import io.jimdb.core.values.LongValue;
import io.jimdb.core.values.Value;
import io.jimdb.pb.Processorpb;
import io.jimdb.pb.Statspb;

import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;

/**
 * @version V1.0
 */
public class MockStoreEngine implements Engine {

  @Override
  public Transaction beginTxn(Session session) {
    return new MockTransaction();
  }

  @Override
  public Flux<Boolean> put(Table table, byte[] key, byte[] value, Instant timeout) throws BaseException {
    return Flux.just(Boolean.TRUE);
  }

  @Override
  public Flux<byte[]> get(Table table, byte[] key, Instant timeout) throws BaseException {
    return Flux.just("a".getBytes());
  }

  @Override
  public Flux<Boolean> delete(Table table, byte[] key, Instant timeout) throws BaseException {
    return Flux.just(Boolean.TRUE);
  }

  @Override
  public Set<RangeInfo> getRanges(Table table) throws BaseException {
    return Collections.EMPTY_SET;
  }

  @Override
  public Flux<ExecResult> insert(Session session, Table table, Column[] insertCols, Expression[][] rows, Assignment[] duplicate,
                                 boolean hasRefColumn) throws BaseException {
    if (rows == null || rows.length == 0) {
      return Flux.create(sink -> sink.next(DMLExecResult.EMPTY));
    }

    int rowSize = rows.length;
    return Flux.create(sink -> sink.next(new DMLExecResult(rowSize, 0)));
  }

  @Override
  public Flux<ExecResult> update(Session session, Table table, Assignment[] assignments, QueryResult rows) throws BaseException {
    if (rows == null || rows.size() == 0) {
      return Flux.just(DMLExecResult.EMPTY);
    }
    int resultRowLength = rows.size();
    ExecResult result = new DMLExecResult(resultRowLength);

    return Flux.create(sink -> sink.next(result));
  }

  @Override
  public Flux<ExecResult> delete(Session session, Table table, QueryResult rows) throws BaseException {
    if (rows == null || rows.size() == 0) {
      return Flux.just(DMLExecResult.EMPTY);
    }
    int resultRowLength = rows.size();
    ExecResult execResult = new DMLExecResult(resultRowLength);
    return Flux.create(sink -> sink.next(execResult));
  }

  @Override
  public Flux<ExecResult> get(Session session, List<Index> indexes, List<Value[]> values, ColumnExpr[] resultColumns) {
    int size = resultColumns.length;
    ValueAccessor[] rows = new ValueAccessor[1];
    Value[] colValues = { BinaryValue.getInstance("Tom".getBytes()),
        LongValue.getInstance(30L),
        BinaryValue.getInstance("13010010000".getBytes()),
        LongValue.getInstance(90L),
        LongValue.getInstance(5000L) };
    ValueAccessor row = new RowValueAccessor(colValues);
    rows[0] = row;
    return Flux.just(new QueryExecResult(resultColumns, rows));
  }

  @Override
  public byte[] addIndex(Session session, Index index, byte[] startKey, byte[] endKey, int limit) {
    return new byte[0];
  }

  @Override
  public Flux<ExecResult> select(Session session, Table table, List<Processorpb.Processor.Builder> processors,
                                 ColumnExpr[] resultColumns,
                                 List<Integer> outputOffsetList) throws BaseException {
    return getTableData(table, resultColumns, null);
  }

  @Override
  public Flux<ExecResult> select(Session session, Index index, List<Processorpb.Processor.Builder> processors,
                                 ColumnExpr[] resultColumns,
                                 List<Integer> outputOffsetList, List<ValueRange> ranges) throws BaseException {
    return getTableData(index.getTable(), resultColumns, null);
  }



  public Flux<ExecResult> getTableData(Table table, ColumnExpr[] resultColumns, Map<Object, Object> filter) {
    return Flux.just(getExecResult(table, resultColumns, filter));
  }

  public ExecResult getExecResult(Table table, ColumnExpr[] resultColumns, Map<Object, Object> filter) {
    ValueAccessor[] newRows = MockTableData.provideDatas(table, resultColumns, filter);
    return new QueryExecResult(resultColumns, newRows);
  }

  @Override
  public Flux<List<Tuple2<Statspb.Histogram, Statspb.CMSketch>>> analyzeIndex(Session session, Table table, Index index, Instant timeout, List<ValueRange> ranges, Statspb.IndexStatsRequest.Builder reqBuilder) throws BaseException {
    return null;
  }

  @Override
  public Flux<List<Tuple2<Statspb.Histogram, List<Statspb.SampleCollector>>>> analyzeColumns(Session session, Table table, Column[] columns, Instant timeout, List<ValueRange> ranges, Statspb.ColumnsStatsRequest.Builder reqBuilder) throws BaseException {
    return null;
  }

  @Override
  public void init(JimConfig c) {
  }

  @Override
  public void close() throws IOException {

  }
}
