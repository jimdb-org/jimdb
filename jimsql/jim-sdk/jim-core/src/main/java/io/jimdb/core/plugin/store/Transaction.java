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

import io.jimdb.core.model.meta.Column;
import io.jimdb.core.model.meta.Index;
import io.jimdb.core.model.meta.Table;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.model.result.QueryResult;
import io.jimdb.common.exception.JimException;
import io.jimdb.core.expression.Assignment;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.expression.ValueRange;
import io.jimdb.pb.Processorpb;
import io.jimdb.pb.Txn;
import io.jimdb.core.values.Value;

import reactor.core.publisher.Flux;

/**
 * Define the interface for operations within a transaction.
 * Note: The implementation must support reuse.
 *
 * @version V1.0
 */
public interface Transaction {
  /**
   * Insert the specified row.
   * If duplicate is not null,then execute update when primary keys conflict.
   *
   * @param table
   * @param insertCols
   * @param rows
   * @param duplicate
   * @param hasRefColumn
   * @return
   * @throws JimException
   */
  Flux<ExecResult> insert(Table table, Column[] insertCols, List<Expression[]> rows, Assignment[] duplicate,
                          boolean hasRefColumn) throws JimException;

  /**
   * Update the specified row. If rows not specified then update all table.
   *
   * @param table
   * @param assignments
   * @param rows
   * @return
   * @throws JimException
   */
  Flux<ExecResult> update(Table table, Assignment[] assignments, QueryResult rows) throws JimException;

  /**
   * Delete table by primary key and delete the associated index.
   * If keys not specified then truncate the table.
   *
   * @param table
   * @param rows
   * @return
   * @throws JimException
   */
  Flux<ExecResult> delete(Table table, QueryResult rows) throws JimException;

  /**
   * Bulk get row data based on primary key or unique index.
   *
   * @param indexs
   * @param values
   * @param resultColumns
   * @return
   */
  Flux<ExecResult> get(List<Index> indexs, List<Value[]> values, ColumnExpr[] resultColumns);

  Flux<ExecResult> select(Table table, List<Processorpb.Processor.Builder> processors, ColumnExpr[] resultColumns,
                          List<Integer> outputOffsetList) throws JimException;

  Flux<ExecResult> select(Index index, List<Processorpb.Processor.Builder> processors, ColumnExpr[] resultColumns,
                          List<Integer> outputOffsetList, List<ValueRange> ranges) throws JimException;

  Flux<ValueAccessor[]> selectByStream(Table table, Txn.SelectFlowRequest.Builder selectFlowBuilder, ColumnExpr[] resultColumns) throws JimException;

  void addIndex(Index index, ColumnExpr[] columns, ValueAccessor[] rows);

  Flux<ExecResult> commit() throws JimException;

  Flux<ExecResult> rollback() throws JimException;

  /**
   * Returns whether the transaction has not been committed or rollback.
   * The transactions should maintain state to support reuse.
   *
   * @return
   */
  boolean isPending();
}
