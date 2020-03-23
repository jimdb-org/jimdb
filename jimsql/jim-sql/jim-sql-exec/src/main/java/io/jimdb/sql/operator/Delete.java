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
import io.jimdb.core.model.meta.Table;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.model.result.QueryResult;
import io.jimdb.sql.optimizer.OperatorVisitor;

import reactor.core.publisher.Flux;

/**
 * @version V1.0
 */
public final class Delete extends Operator {
  private final Table table;
  private RelOperator select;

  public Delete(final Table table) {
    this.table = table;
  }

  public void setSelect(RelOperator select) {
    this.select = select;
  }

  public RelOperator getSelect() {
    return this.select;
  }

  @Override
  public OperatorType getOperatorType() {
    return OperatorType.DELETE;
  }

  @Override
  public Flux<ExecResult> execute(Session session) throws BaseException {
    if (this.select == null) {
      return session.getStoreEngine().delete(session, table, null);
    }

    return this.select.execute(session)
            .flatMap(r -> session.getStoreEngine().delete(session, table, (QueryResult) r));
  }

  @Override
  public <R> R acceptVisitor(OperatorVisitor<R> visitor) {
    return visitor.visitOperator(this);
  }

  @Override
  public String getName() {
    return "Delete";
  }
}
