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

import io.jimdb.core.Session;
import io.jimdb.common.exception.BaseException;
import io.jimdb.core.expression.Assignment;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.Schema;
import io.jimdb.core.model.meta.Table;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.model.result.QueryResult;
import io.jimdb.sql.optimizer.OperatorVisitor;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.core.publisher.Flux;

/**
 * @version V1.0
 */
@SuppressFBWarnings("EI_EXPOSE_REP2")
public final class Update extends Operator {
  private final Table table;
  private final Assignment[] assignments;
  private RelOperator select;

  public Update(Table table, Assignment[] assignments) {
    this.table = table;
    this.assignments = assignments;
  }

  public void setSelect(RelOperator select) {
    this.select = select;
  }

  public RelOperator getSelect() {
    return this.select;
  }

  @Override
  public OperatorType getOperatorType() {
    return OperatorType.UPDATE;
  }

  @Override
  public void resolveOffset() {
    super.resolveOffset();
    Schema selSchema = select.getSchema();
    for (Assignment assign : assignments) {
      assign.setColumn((ColumnExpr) assign.getColumn().resolveOffset(selSchema, true));
      assign.setExpression(assign.getExpression().resolveOffset(selSchema, true));
    }
  }

  @Override
  public Flux<ExecResult> execute(Session session) throws BaseException {
    if (this.select == null) {
      return session.getStoreEngine().update(session, table, assignments, null);
    }

    return this.select.execute(session)
            .flatMap(r -> session.getStoreEngine().update(session, table, assignments, (QueryResult) r));
  }

  @Override
  public <R> R acceptVisitor(OperatorVisitor<R> visitor) {
    return visitor.visitOperator(this);
  }

  @Override
  public String getName() {
    return "Update";
  }
}
