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
package io.jimdb.sql.operator;

import io.jimdb.core.Session;
import io.jimdb.common.exception.JimException;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.RowValueAccessor;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.model.result.impl.QueryExecResult;
import io.jimdb.sql.optimizer.OperatorVisitor;
import io.jimdb.sql.optimizer.ParameterizedOperatorVisitor;
import io.jimdb.core.values.NullValue;
import io.jimdb.core.values.Value;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.core.publisher.Flux;

/**
 * @version V1.0
 */
public final class DualTable extends RelOperator {
  private int rowCount;
  private boolean placeHolder;

  public DualTable(final int rowCount) {
    this.rowCount = rowCount;
  }

  private DualTable(DualTable dualTable) {
    this.rowCount = dualTable.rowCount;
    this.placeHolder = dualTable.placeHolder;
    this.copyBaseParameters(dualTable);
  }

  @Override
  public DualTable copy() {
    DualTable dualTable = new DualTable(this);
    dualTable.children = this.copyChildren();

    return dualTable;
  }

  @Override
  @SuppressFBWarnings("NP_NULL_ON_SOME_PATH")
  public Flux<ExecResult> execute(final Session session) throws JimException {
    final ValueAccessor[] rows = new ValueAccessor[rowCount];
    final int colNum = schema == null || schema.isEmpty() ? 0 : schema.size();
    Value[] columns;
    for (int i = 0; i < rowCount; i++) {
      if (colNum == 0) {
        rows[i] = ValueAccessor.EMPTY;
        continue;
      }

      columns = new Value[colNum];
      for (int j = 0; j < colNum; j++) {
        columns[j] = NullValue.getInstance();
      }
      rows[i] = new RowValueAccessor(columns);
    }
    return Flux.just(new QueryExecResult(schema.getColumns().toArray(new ColumnExpr[schema.getColumns().size()]), rows));
  }

  public <T, R> R acceptVisitor(Session session, ParameterizedOperatorVisitor<T, R> visitor, T t) {
    return visitor.visitOperator(session, this, t);
  }

  @Override
  public <R> R acceptVisitor(OperatorVisitor<R> visitor) {
    return visitor.visitOperator(this);
  }

  public int getRowCount() {
    return this.rowCount;
  }

  public void setRowCount(int rowCount) {
    this.rowCount = rowCount;
  }

  public boolean isPlaceHolder() {
    return placeHolder;
  }

  public void setPlaceHolder(boolean placeHolder) {
    this.placeHolder = placeHolder;
  }
}
