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
import java.util.List;

import io.jimdb.core.Session;
import io.jimdb.core.expression.RowValueAccessor;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.expression.aggregate.AggregateFunc;
import io.jimdb.core.expression.aggregate.Cell;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.values.Value;

import reactor.core.publisher.Flux;

/**
 * Abstract class for aggregation executor
 */
public abstract class AggExecutor implements Executor {

  public abstract Flux<ExecResult> execute(Session session, ExecResult execResult);

  List<Cell> initCells(AggregateFunc[] aggFuncs) {
    List<Cell> cells = new ArrayList<>(aggFuncs.length);
    for (AggregateFunc aggregateFunc : aggFuncs) {
      cells.add(aggregateFunc.initCell());
    }
    return cells;
  }

  ValueAccessor initEmptyRow(AggregateFunc[] aggFuncs) {
    RowValueAccessor rva = new RowValueAccessor(new Value[aggFuncs.length]);
    for (int i = 0; i < aggFuncs.length; i++) {
      rva.set(i, aggFuncs[i].initCell().getValue());
    }
    return rva;
  }
}
