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
import io.jimdb.common.exception.JimException;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.plugin.SQLEngine;

import reactor.core.publisher.Flux;

/**
 * Rollback
 *
 * @since 2019/12/24
 */
public final class Rollback extends Operator {
  private static final Rollback INSTANCE = new Rollback();

  public static Rollback getInstance() {
    return INSTANCE;
  }

  private Rollback() {
  }

  @Override
  public OperatorType getOperatorType() {
    return OperatorType.SIMPLE;
  }

  @Override
  public Flux<ExecResult> execute(Session session) throws JimException {
    session.getVarContext().setStatus(SQLEngine.ServerStatus.INTRANS, false);
    return session.getTxn().rollback();
  }
}
