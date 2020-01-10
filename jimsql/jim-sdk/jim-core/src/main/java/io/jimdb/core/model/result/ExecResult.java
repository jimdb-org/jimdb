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
package io.jimdb.core.model.result;

import java.util.function.Consumer;

import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.ValueAccessor;

/**
 * @version V1.0
 */
public abstract class ExecResult implements DMLResult, QueryResult {
  @Override
  public long refCnt() {
    return 0;
  }

  @Override
  public void retain() {
  }

  @Override
  public boolean release() {
    return true;
  }

  @Override
  public boolean isEof() {
    return true;
  }

  @Override
  public long getAffectedRows() {
    throw new UnsupportedOperationException("unsupported method getAffectedRows");
  }

  @Override
  public long getLastInsertId() {
    throw new UnsupportedOperationException("unsupported method getLastInsertId");
  }

  @Override
  public int size() {
    throw new UnsupportedOperationException("unsupported method size");
  }

  @Override
  public ColumnExpr[] getColumns() {
    throw new UnsupportedOperationException("unsupported method getColumns");
  }

  @Override
  public ValueAccessor getRow(int i) {
    throw new UnsupportedOperationException("unsupported method getRow");
  }

  @Override
  public void setRow(int i, ValueAccessor row) {
    throw new UnsupportedOperationException("unsupported method sertRow");
  }

  @Override
  public void truncate(int offset, int length) {
    throw new UnsupportedOperationException("unsupported method truncate");
  }

  @Override
  public void forEach(Consumer<ValueAccessor> action) {
    throw new UnsupportedOperationException("unsupported method forEach row");
  }

  @Override
  public void accept(Consumer<ValueAccessor[]> action) {
    throw new UnsupportedOperationException("unsupported method accept");
  }
}
