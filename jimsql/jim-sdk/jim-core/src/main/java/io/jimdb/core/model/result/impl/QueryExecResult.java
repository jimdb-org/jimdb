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
package io.jimdb.core.model.result.impl;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import io.jimdb.core.model.result.ResultType;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.model.result.ExecResult;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
public final class QueryExecResult extends ExecResult {
  public static final ValueAccessor[] EMPTY_ROW = new ValueAccessor[0];

  private ColumnExpr[] columns;

  private ValueAccessor[] rows;

  public QueryExecResult(final ColumnExpr[] columns, ValueAccessor[] rows) {
    this.columns = columns;
    this.rows = rows;
  }

  public static QueryExecResult merge(List<QueryExecResult> resultList) {
    ValueAccessor[] rows = resultList.stream()
                                   .filter(Objects::nonNull)
                                   .flatMap(result -> Arrays.stream(result.rows))
                                   .filter(Objects::nonNull)
                                   .toArray(ValueAccessor[]::new);
    return new QueryExecResult(resultList.get(0).columns, rows);
  }

  @Override
  public ResultType getType() {
    return ResultType.QUERY;
  }

  @Override
  public int size() {
    return rows == null ? 0 : rows.length;
  }

  @Override
  public ColumnExpr[] getColumns() {
    return this.columns;
  }

  @Override
  public ValueAccessor getRow(int i) {
    return rows[i];
  }

  @Override
  public void setRow(int i, ValueAccessor row) {
    rows[i] = row;
  }

  @Override
  public void truncate(int offset, int length) {
    if (offset >= rows.length || length == 0) {
      rows = new ValueAccessor[0];
      return;
    }
    if (offset + length > rows.length) {
      length = rows.length - offset;
    }
    ValueAccessor[] dest = new ValueAccessor[length];
    System.arraycopy(rows, offset, dest, 0, length);
    rows = dest;
  }

  @Override
  public void forEach(Consumer<ValueAccessor> action) {
    for (ValueAccessor row : rows) {
      action.accept(row);
    }
  }

  @Override
  public void accept(Consumer<ValueAccessor[]> action) {
    action.accept(rows);
  }
}
