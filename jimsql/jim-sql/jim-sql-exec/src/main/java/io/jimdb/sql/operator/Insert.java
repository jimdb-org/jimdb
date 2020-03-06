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
package io.jimdb.sql.operator;

import java.util.List;

import io.jimdb.core.Session;
import io.jimdb.common.exception.BaseException;
import io.jimdb.core.expression.Assignment;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.Schema;
import io.jimdb.core.model.meta.Column;
import io.jimdb.core.model.meta.Table;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.sql.optimizer.OperatorVisitor;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.core.publisher.Flux;

/**
 * @version V1.0
 */
@SuppressFBWarnings("EI_EXPOSE_REP2")
public final class Insert extends RelOperator {
  private final Table table;
  private boolean hasRefColumn;
  private Schema schema4Dup;
  private Column[] columns;
  private List<Expression[]> values;
  private Assignment[] duplicate;
  private RelOperator select;

  public Insert(Table table, Schema schema) {
    this.table = table;
    this.schema = schema;
  }

  @Override
  public OperatorType getOperatorType() {
    return OperatorType.INSERT;
  }

  public boolean isHasRefColumn() {
    return hasRefColumn;
  }

  public void setHasRefColumn(boolean hasRefColumn) {
    this.hasRefColumn = hasRefColumn;
  }

  public Schema getSchema4Dup() {
    return schema4Dup;
  }

  public void setSchema4Dup(Schema schema4Dup) {
    this.schema4Dup = schema4Dup;
  }

  public void setColumns(Column[] columns) {
    this.columns = columns;
  }

  public void setValues(List<Expression[]> values) {
    this.values = values;
  }

  public void setDuplicate(Assignment[] duplicate) {
    this.duplicate = duplicate;
  }

  public RelOperator getSelect() {
    return select;
  }

  public void setSelect(RelOperator select) {
    this.select = select;
  }

  @Override
  public void resolveOffset() {
    super.resolveOffset();
    if (duplicate != null) {
      for (Assignment assign : duplicate) {
        assign.setColumn((ColumnExpr) assign.getColumn().resolveOffset(schema, true));
        assign.setExpression(assign.getExpression().resolveOffset(schema4Dup, true));
      }
    }
    if (values != null) {
      for (Expression[] cols : values) {
        int size = cols.length;
        for (int i = 0; i < size; i++) {
          cols[i] = cols[i].resolveOffset(schema, true);
        }
      }
    }
  }

  @Override
  public <R> R acceptVisitor(OperatorVisitor<R> visitor) {
    return visitor.visitOperator(this);
  }

  @Override
  public Flux<ExecResult> execute(Session session) throws BaseException {
    return session.getTxn().insert(table, columns, values, duplicate, hasRefColumn);
  }

  @Override
  public String getName() {
    return "Insert";
  }
}
