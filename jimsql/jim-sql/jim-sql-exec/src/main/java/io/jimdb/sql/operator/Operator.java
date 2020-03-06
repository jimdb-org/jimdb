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

import java.util.Collections;

import io.jimdb.core.Session;
import io.jimdb.common.exception.BaseException;
import io.jimdb.core.expression.Schema;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.sql.optimizer.OperatorVisitor;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.core.publisher.Flux;

/**
 * Generic step of planned operation.
 *
 * @version V1.0
 */
public abstract class Operator implements AutoCloseable {
  protected Schema schema;

  public abstract OperatorType getOperatorType();

  public String getName() {
    return "";
  }

  /**
   * Execute this expression asynchronously.
   *
   * @param session
   * @return Flux
   * @throws BaseException
   */
  public abstract Flux<ExecResult> execute(Session session) throws BaseException;

  public Schema getSchema() {
    if (schema == null) {
      schema = new Schema(Collections.EMPTY_LIST);
    }
    return schema;
  }

  public void setSchema(Schema schema) {
    this.schema = schema;
  }

  @SuppressFBWarnings("ACEM_ABSTRACT_CLASS_EMPTY_METHODS")
  public void resolveOffset() {
  }

  public <R> R acceptVisitor(OperatorVisitor<R> visitor) {
    return visitor.visitOperator(this);
  }

  @Override
  public void close() {
  }

  /**
   *
   */
  public enum OperatorType {
    INSERT(true),
    UPDATE(true),
    DELETE(true),

    SIMPLE(false),
    LOGIC(false),
    DDL(false),
    PREPARE(false),
    SHOW(false),
    ANALYZE(false),
    EXPLAIN(false);

    private final boolean writable;

    OperatorType(boolean writable) {
      this.writable = writable;
    }

    public boolean isWritable() {
      return writable;
    }
  }
}
