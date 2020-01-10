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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import io.jimdb.core.Session;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.RowValueAccessor;
import io.jimdb.core.expression.Schema;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.expression.aggregate.AggregateFunc;
import io.jimdb.core.expression.aggregate.Cell;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.model.result.impl.QueryExecResult;
import io.jimdb.core.values.Value;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.core.publisher.Flux;

/**
 * Executor for streaming aggregation
 */
@SuppressFBWarnings()
public class StreamAggExecutor extends AggExecutor {
  private Schema schema;
  private AggregateFunc[] aggFuncs;
  private Expression[] groupByExprs;

  public StreamAggExecutor(Schema schema, AggregateFunc[] aggFuncs, Expression[] groupByExprs) {
    this.schema = schema;
    this.aggFuncs = aggFuncs;
    this.groupByExprs = groupByExprs;
  }

  /**
   * execute
   *
   * @param session
   * @param childData
   * @return
   */
  @Override
  public Flux<ExecResult> execute(Session session, ExecResult childData) {
    if (childData.size() == 0) {
      ValueAccessor row = initEmptyRow(aggFuncs);
      return Flux.just(new QueryExecResult(schema.getColumns().toArray(new ColumnExpr[schema.getColumns().size()]),
              new ValueAccessor[] {row}));
    }

    List<ValueAccessor> inputs = new ArrayList<>(childData.size());
    List<ValueAccessor> outputs = new ArrayList<>(childData.size());
    Value[] lastGroupValues = new Value[groupByExprs.length];

    childData.forEach(row -> {
      if (isNewGroupValues(lastGroupValues, groupByExprs, row)) {
        drainInputs(session, aggFuncs, inputs, outputs);
        inputs.clear();
      }
      inputs.add(row);
    });

    if (!inputs.isEmpty()) {
      drainInputs(session, aggFuncs, inputs, outputs);
    }

    return Flux.just(new QueryExecResult(schema.getColumns().toArray(new ColumnExpr[schema.getColumns().size()]),
            outputs.toArray(new ValueAccessor[outputs.size()])));
  }

  /**
   * drainInputs: calculate inputs in one groupkey to output
   *
   * @param aggFuncs
   * @param inputs
   * @param outputs
   */
  private void drainInputs(Session session, AggregateFunc[] aggFuncs, List<ValueAccessor> inputs,
                           List<ValueAccessor> outputs) {
    // calculate partial result
    if (inputs.isEmpty()) {
      return;
    }
    // update inner result
    List<Cell> innerRow = initCells(aggFuncs);
    for (int i = 0; i < aggFuncs.length; i++) {
      aggFuncs[i].calculatePartialResult(session, inputs.toArray(new RowValueAccessor[inputs.size()]), innerRow.get(i));
    }

    RowValueAccessor output = new RowValueAccessor(new Value[aggFuncs.length]);
    // get final result
    for (int i = 0; i < aggFuncs.length; i++) {
      aggFuncs[i].append2Result(session, output, i, innerRow.get(i));
    }
    outputs.add(output);
  }

  /**
   * isNewGroupValues: extract group values(tmpGroupValues) from one row by groupByExprs
   * and compare with lastGroupValues. If they are different then return true,
   * otherwise return false.
   *
   * @param lastGroupValues
   * @param groupByExprs
   * @param row
   * @return
   */
  private boolean isNewGroupValues(Value[] lastGroupValues,
                                   Expression[] groupByExprs, ValueAccessor row) {
    if (groupByExprs.length == 0) {
      return false;
    }

    Value[] tmpGroupValues = new Value[groupByExprs.length];
    boolean isEqual = true;

    if (lastGroupValues[0] == null) {
      isEqual = false;
    }
    for (int i = 0; i < groupByExprs.length; i++) {
      Value value = groupByExprs[i].exec(row);
      if (isEqual) {
        isEqual = value.equals(lastGroupValues[i]);
      }
      tmpGroupValues[i] = value;
    }

    if (isEqual) {
      return false;
    }

    System.arraycopy(tmpGroupValues, 0, lastGroupValues, 0, tmpGroupValues.length);
    return true;
  }

  @Override
  public String toString() {
    return "aggFuncs :" + Arrays.stream(this.aggFuncs).collect(Collectors.toList());
  }
}
