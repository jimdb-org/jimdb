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

import java.util.Arrays;
import java.util.List;

import io.jimdb.core.Session;
import io.jimdb.common.exception.JimException;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.Schema;
import io.jimdb.core.model.meta.Index;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.values.Value;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.core.publisher.Flux;

/**
 * @version V1.0
 */
public final class KeyGet extends RelOperator {
  private final List<Index> indices;
  private final List<Value[]> values;

  public KeyGet(List<Index> indices, List<Value[]> values, Schema schema) {
    this.indices = indices;
    this.values = values;
    this.schema = schema;
  }

  private KeyGet(KeyGet keyGet) {
    this.indices = keyGet.indices;
    this.values = keyGet.values;
    this.copyBaseParameters(keyGet);
  }

  @Override
  public KeyGet copy() {
    KeyGet keyGet = new KeyGet(this);
    keyGet.children = this.copyChildren();

    return keyGet;
  }

  @Override
  public OperatorType getOperatorType() {
    return OperatorType.SIMPLE;
  }

  @Override
  public Flux<ExecResult> execute(Session session) throws JimException {
    return session.getTxn().get(indices, values, schema.getColumns().toArray(new ColumnExpr[schema.getColumns().size()]));
  }

  public String getName() {
    return "KeyGet";
  }

  @Override
  @SuppressFBWarnings("PL_PARALLEL_LISTS")
  public String toString() {
    if (this.indices == null || this.values == null || this.indices.size() != this.values.size()) {
      return "KeyGet";
    }
    final StringBuilder stringBuilder = new StringBuilder("KeyGet(");
    if (this.indices.size() > 0) {
      for (int i = 0; i < this.indices.size(); i++) {
        Index index = this.indices.get(i);
        Value[] values = this.values.get(i);
        stringBuilder.append('(').append(index.getName()).append('(');
        Arrays.stream(index.getColumns()).forEach(column -> stringBuilder.append(column.getName()).append(','));
        stringBuilder.deleteCharAt(stringBuilder.length() - 1).append("),values(");
        Arrays.stream(values).forEach(value ->
                stringBuilder.append(value.getString()).append(':').append(value.getType()).append(','));
        stringBuilder.deleteCharAt(stringBuilder.length() - 1).append(")),");
      }
      stringBuilder.deleteCharAt(stringBuilder.length() - 1).append(')');
    }

    List<ColumnExpr> projCols = this.getSchema().getColumns();
    if (projCols != null && projCols.size() > 0) {
      stringBuilder.append(" -> Projection(");
      projCols.forEach(col -> stringBuilder.append(col).append(','));
      stringBuilder.deleteCharAt(stringBuilder.length() - 1).append(')');
    }

    return stringBuilder.toString();
  }

  public List<Index> getIndices() {
    return indices;
  }

  public List<Value[]> getValues() {
    return values;
  }
}
