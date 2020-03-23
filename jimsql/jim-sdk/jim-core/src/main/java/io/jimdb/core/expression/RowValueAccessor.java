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
package io.jimdb.core.expression;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import io.jimdb.core.model.meta.Column;
import io.jimdb.core.values.NullValue;
import io.jimdb.core.values.Value;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings({ "EI_EXPOSE_REP", "EI_EXPOSE_REP2", "PL_PARALLEL_LISTS" })
public final class RowValueAccessor implements ValueAccessor {
  private Value[] values;

  public RowValueAccessor(final Value[] value) {
    this.values = value;
  }

  public Value[] getValues() {
    return this.values;
  }

  public void setValues(Value[] values) {
    this.values = values;
  }

  @Override
  public int size() {
    return values == null ? 0 : values.length;
  }

  @Override
  public Value get(int index) {
    if (index > values.length || values[index] == null) {
      return NullValue.getInstance();
    }
    return values[index];
  }

  @Override
  public void set(int index, Value value) {
    this.values[index] = value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RowValueAccessor that = (RowValueAccessor) o;
    return Arrays.equals(values, that.values);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(values);
  }

  @Override
  public Value[] extractValues(Column[] columns, ColumnExpr[] columnExprs) {
    Map<Integer, Integer> mapping = new HashMap<>(columnExprs.length);
    for (int i = 0; i < columnExprs.length; i++) {
      ColumnExpr expr = columnExprs[i];
      mapping.put(expr.getId(), i);
    }

    int colLength = columns.length;
    Value[] values = new Value[colLength];
    for (int i = 0; i < colLength; i++) {
      Column column = columns[i];
      Integer location = mapping.get(column.getId());
      if (location == null) {
        continue;
      }
      Value value = get(location);
      values[column.getOffset()] = value;
    }
    return values;

  }
}
