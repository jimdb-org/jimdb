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

import java.util.List;
import java.util.Map;

import io.jimdb.common.exception.JimException;
import io.jimdb.core.expression.Assignment;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.RowValueAccessor;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.expression.ValueRange;
import io.jimdb.core.model.meta.Column;
import io.jimdb.core.model.meta.Index;
import io.jimdb.core.model.meta.Table;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.model.result.QueryResult;
import io.jimdb.core.model.result.impl.AckExecResult;
import io.jimdb.core.model.result.impl.DMLExecResult;
import io.jimdb.core.model.result.impl.QueryExecResult;
import io.jimdb.pb.Processorpb;
import io.jimdb.pb.Txn;
import io.jimdb.core.plugin.store.Transaction;
import io.jimdb.core.values.BinaryValue;
import io.jimdb.core.values.LongValue;
import io.jimdb.core.values.Value;

import reactor.core.publisher.Flux;

/**
 * @version V1.0
 */
public class MockTransaction implements Transaction {

  @Override
  public Flux<ExecResult> insert(Table table, Column[] insertCols, List<Expression[]> rows, Assignment[] duplicate,
                                 boolean hasRefColumn) throws JimException {
    if (rows == null || rows.isEmpty()) {
      return Flux.create(sink -> sink.next(DMLExecResult.EMPTY));
    }

    int rowSize = rows.size();
    return Flux.create(sink -> sink.next(new DMLExecResult(rowSize, 0)));
  }

  @Override
  public Flux<ExecResult> update(Table table, Assignment[] assignments, QueryResult rows) throws JimException {
    if (rows == null || rows.size() == 0) {
      return Flux.just(DMLExecResult.EMPTY);
    }
    int resultRowLength = rows.size();
    ExecResult result = new DMLExecResult(resultRowLength);

    return Flux.create(sink -> sink.next(result));
  }

  @Override
  public Flux<ExecResult> delete(Table table, QueryResult rows) throws JimException {
    if (rows == null || rows.size() == 0) {
      return Flux.just(DMLExecResult.EMPTY);
    }
    int resultRowLength = rows.size();
    ExecResult execResult = new DMLExecResult(resultRowLength);
    return Flux.create(sink -> sink.next(execResult));
  }

  @Override
  public Flux<ExecResult> get(List<Index> indexes, List<Value[]> values, ColumnExpr[] resultColumns) {
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

  public Flux<ExecResult> getTableData(Table table, ColumnExpr[] resultColumns, Map<Object, Object> filter) {
    return Flux.just(getExecResult(table, resultColumns, filter));
  }

  public ExecResult getExecResult(Table table, ColumnExpr[] resultColumns, Map<Object, Object> filter) {
    ValueAccessor[] newRows = MockTableData.provideDatas(table, resultColumns, filter);
    return new QueryExecResult(resultColumns, newRows);
  }

  @Override
  public Flux<ExecResult> select(Table table, List<Processorpb.Processor.Builder> processors,
                                 ColumnExpr[] resultColumns,
                                 List<Integer> outputOffsetList) throws JimException {
    return getTableData(table, resultColumns, null);
  }

  @Override
  public Flux<ExecResult> select(Index index, List<Processorpb.Processor.Builder> processors,
                                 ColumnExpr[] resultColumns,
                                 List<Integer> outputOffsetList, List<ValueRange> ranges) throws JimException {
    return getTableData(index.getTable(), resultColumns, null);
  }

  @Override
  public Flux<ValueAccessor[]> selectByStream(Table table, Txn.SelectFlowRequest.Builder selectFlowBuilder, ColumnExpr[] resultColumns) throws JimException {
    return null;
  }

  @Override
  public void addIndex(Index index, ColumnExpr[] columns, ValueAccessor[] rows) {

  }

  @Override
  public Flux<ExecResult> commit() throws JimException {
    return Flux.just(AckExecResult.getInstance());
  }

  @Override
  public Flux<ExecResult> rollback() throws JimException {
    return Flux.just(AckExecResult.getInstance());
  }

  @Override
  public boolean isPending() {
    return true;
  }
}
