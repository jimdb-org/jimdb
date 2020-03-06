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

import java.util.List;

import io.jimdb.common.exception.BaseException;
import io.jimdb.core.expression.Assignment;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.ValueRange;
import io.jimdb.core.model.meta.Column;
import io.jimdb.core.model.meta.Index;
import io.jimdb.core.model.meta.Table;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.model.result.QueryResult;
import io.jimdb.core.values.Value;
import io.jimdb.pb.Processorpb;

import reactor.core.publisher.Flux;

/**
 * Define the interface for operations within a transaction.
 * Note: The implementation must support reuse.
 */
public interface Transaction {
  /**
   * Insert the specified row.
   * If duplicate is not null,then execute update when primary keys conflict.
   *
   * @param table TODO
   * @param insertCols TODO
   * @param rows TODO
   * @param duplicate TODO
   * @param hasRefColumn TODO
   * @return TODO
   * @throws BaseException TODO
   */
  Flux<ExecResult> insert(Table table, Column[] insertCols, List<Expression[]> rows, Assignment[] duplicate,
                          boolean hasRefColumn) throws BaseException;

  /**
   * Update the specified row. If rows not specified then update all table.
   *
   * @param table TODO
   * @param assignments TODO
   * @param rows TODO
   * @return TODO
   * @throws BaseException TODO
   */
  Flux<ExecResult> update(Table table, Assignment[] assignments, QueryResult rows) throws BaseException;

  /**
   * Delete table by primary key and delete the associated index.
   * If keys not specified then truncate the table.
   *
   * @param table TODO
   * @param rows TODO
   * @return TODO
   * @throws BaseException TODO
   */
  Flux<ExecResult> delete(Table table, QueryResult rows) throws BaseException;

  /**
   * Bulk get row data based on primary key or unique index.
   *
   * @param indices TODO
   * @param values TODO
   * @param resultColumns TODO
   * @return TODO
   */
  Flux<ExecResult> get(List<Index> indices, List<Value[]> values, ColumnExpr[] resultColumns);

  /**
   * Back fill index data, and return the last key traversed.
   *
   * @param index TODO
   * @param startKey TODO
   * @param endKey TODO
   * @param limit TODO
   * @return last key
   */
  byte[] addIndex(Index index, byte[] startKey, byte[] endKey, int limit);

  Flux<ExecResult> select(Table table, List<Processorpb.Processor.Builder> processors, ColumnExpr[] resultColumns,
                          List<Integer> outputOffsetList) throws BaseException;

  Flux<ExecResult> select(Index index, List<Processorpb.Processor.Builder> processors, ColumnExpr[] resultColumns,
                          List<Integer> outputOffsetList, List<ValueRange> ranges) throws BaseException;

  Flux<ExecResult> commit() throws BaseException;

  Flux<ExecResult> rollback() throws BaseException;

  /**
   * Returns whether the transaction has not been committed or rollback.
   * The transactions should maintain state to support reuse.
   *
   * @return TODO
   */
  boolean isPending();
}
