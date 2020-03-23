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
package io.jimdb.core.plugin.store;

import java.time.Instant;
import java.util.List;
import java.util.Set;

import io.jimdb.common.exception.BaseException;
import io.jimdb.core.Session;
import io.jimdb.core.expression.Assignment;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.ValueRange;
import io.jimdb.core.model.meta.Column;
import io.jimdb.core.model.meta.Index;
import io.jimdb.core.model.meta.RangeInfo;
import io.jimdb.core.model.meta.Table;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.model.result.QueryResult;
import io.jimdb.core.plugin.Plugin;
import io.jimdb.core.values.Value;
import io.jimdb.pb.Processorpb;
import io.jimdb.pb.Statspb;

import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;

/**
 * @version V1.0
 */
public interface Engine extends Plugin {
  Transaction beginTxn(Session session);

  /**
   * @param table
   * @param key
   * @param value
   * @param timeout
   * @return
   * @throws BaseException
   */
  @Deprecated
  Flux<Boolean> put(Table table, byte[] key, byte[] value, Instant timeout) throws BaseException;

  /**
   * @param table
   * @param key
   * @param timeout
   * @return
   * @throws BaseException
   */
  @Deprecated
  Flux<byte[]> get(Table table, byte[] key, Instant timeout) throws BaseException;

  /**
   * @param table
   * @param key
   * @param timeout
   * @return
   * @throws BaseException
   */
  @Deprecated
  Flux<Boolean> delete(Table table, byte[] key, Instant timeout) throws BaseException;

  /**
   * @param table
   * @return
   * @throws BaseException
   */
  Set<RangeInfo> getRanges(Table table) throws BaseException;

  /**
   * Insert the specified row.
   * If duplicate is not null,then execute update when primary keys conflict.
   *
   * @param table        TODO
   * @param insertCols   TODO
   * @param rows         TODO
   * @param duplicate    TODO
   * @param hasRefColumn TODO
   * @return TODO
   * @throws BaseException TODO
   */
  Flux<ExecResult> insert(Session session, Table table, Column[] insertCols, Expression[][] rows, Assignment[] duplicate,
                          boolean hasRefColumn) throws BaseException;

  /**
   * Update the specified row. If rows not specified then update all table.
   *
   * @param table       TODO
   * @param assignments TODO
   * @param rows        TODO
   * @return TODO
   * @throws BaseException TODO
   */
  Flux<ExecResult> update(Session session, Table table, Assignment[] assignments, QueryResult rows) throws BaseException;

  /**
   * Delete table by primary key and delete the associated index.
   * If keys not specified then truncate the table.
   *
   * @param table TODO
   * @param rows  TODO
   * @return TODO
   * @throws BaseException TODO
   */
  Flux<ExecResult> delete(Session session, Table table, QueryResult rows) throws BaseException;

  /**
   * Bulk get row data based on primary key or unique index.
   *
   * @param indices       TODO
   * @param values        TODO
   * @param resultColumns TODO
   * @return TODO
   */
  Flux<ExecResult> get(Session session, List<Index> indices, List<Value[]> values, ColumnExpr[] resultColumns);

  /**
   * Back fill index data, and return the last key traversed.
   *
   * @param index    TODO
   * @param startKey TODO
   * @param endKey   TODO
   * @param limit    TODO
   * @return last key
   */
  byte[] addIndex(Session session, Index index, byte[] startKey, byte[] endKey, int limit);

  Flux<ExecResult> select(Session session, Table table, List<Processorpb.Processor.Builder> processors, ColumnExpr[] resultColumns,
                          List<Integer> outputOffsetList) throws BaseException;

  Flux<ExecResult> select(Session session, Index index, List<Processorpb.Processor.Builder> processors, ColumnExpr[] resultColumns,
                          List<Integer> outputOffsetList, List<ValueRange> ranges) throws BaseException;

  Flux<List<Tuple2<Statspb.Histogram, Statspb.CMSketch>>>
      analyzeIndex(Session session, Table table, Index index, Instant timeout, List<ValueRange> ranges, Statspb.IndexStatsRequest.Builder reqBuilder) throws BaseException;


  Flux<List<Tuple2<Statspb.Histogram, List<Statspb.SampleCollector>>>>
      analyzeColumns(Session session, Table table, Column[] columns, Instant timeout, List<ValueRange> ranges, Statspb.ColumnsStatsRequest.Builder reqBuilder) throws BaseException;


}
