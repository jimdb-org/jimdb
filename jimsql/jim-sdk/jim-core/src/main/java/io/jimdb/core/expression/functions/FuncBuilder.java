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
package io.jimdb.core.expression.functions;

import io.jimdb.core.Session;
import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.core.expression.Expression;

/**
 * @version V1.0
 */
public abstract class FuncBuilder {
  protected final int argMinSize;
  protected final int argMaxSize;
  protected final String name;

  public FuncBuilder(String name, int argMinSize, int argMaxSize) {
    this.name = name;
    this.argMinSize = argMinSize;
    this.argMaxSize = argMaxSize;
  }

  public final Func build(Session session, Expression[] args) {
    if (args == null || args.length < argMinSize || (argMaxSize > 0 && args.length > argMaxSize)) {
      throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT, name);
    }

    return doBuild(session, args);
  }

  protected abstract Func doBuild(Session session, Expression[] args);
}
