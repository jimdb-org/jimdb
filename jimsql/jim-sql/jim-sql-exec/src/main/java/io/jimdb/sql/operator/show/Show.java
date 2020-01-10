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
package io.jimdb.sql.operator.show;

import io.jimdb.core.Session;
import io.jimdb.common.exception.JimException;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.sql.operator.Operator;
import io.jimdb.sql.operator.RelOperator;

import reactor.core.publisher.Flux;

/**
 * @version V1.0
 */
public final class Show extends Operator {
  private RelOperator showOperator;
  private RelOperator projectOperator;

  public Show(RelOperator showOperator, RelOperator projectOperator) {
    this.showOperator = showOperator;
    this.projectOperator = projectOperator;
  }

  public Show(RelOperator showOperator) {
    this.showOperator = showOperator;
    this.projectOperator = null;
  }

  public RelOperator getShowOperator() {
    return showOperator;
  }

  public RelOperator getProjectOperator() {
    return projectOperator;
  }

  public void setShowOperator(RelOperator showOperator) {
    this.showOperator = showOperator;
  }

  @Override
  public OperatorType getOperatorType() {
    return projectOperator == null ? OperatorType.SIMPLE : OperatorType.SHOW;
  }

  @Override
  public Flux<ExecResult> execute(Session session) throws JimException {
    return showOperator.execute(session);
  }
}
