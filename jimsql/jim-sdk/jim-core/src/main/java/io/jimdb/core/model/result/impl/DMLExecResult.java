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
package io.jimdb.core.model.result.impl;

import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.model.result.ResultType;

/**
 * @version V1.0
 */
public final class DMLExecResult extends ExecResult {
  public static final long EMPTY_INSERT_ID = 0;
  public static final DMLExecResult EMPTY = new DMLExecResult(0);

  private final long affectedRows;
  private final long insertId;

  public DMLExecResult(long affectedRows) {
    this(affectedRows, EMPTY_INSERT_ID);
  }

  public DMLExecResult(long affectedRows, long insertId) {
    this.affectedRows = affectedRows;
    this.insertId = insertId;
  }

  @Override
  public long getAffectedRows() {
    return this.affectedRows;
  }

  @Override
  public long getLastInsertId() {
    return this.insertId;
  }

  @Override
  public ResultType getType() {
    return ResultType.DML;
  }
}
