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
import io.jimdb.common.exception.JimException;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.values.StringValue;

/**
 * @version V1.0
 */
public final class DatabaseFunc extends Func {

  protected DatabaseFunc() {
    this.name = "DATABASE";
  }

  @Override
  public Func clone() {
    return new DatabaseFunc();
  }

  @Override
  public StringValue execString(ValueAccessor accessor) throws JimException {
    return StringValue.getInstance(this.session.getVarContext().getDefaultCatalog());
  }

  /**
   * @version V1.0
   */
  public static final class DatabaseBuilder extends NOParamFuncBuilder {
    public DatabaseBuilder(String name) {
      super(name);
    }

    @Override
    protected Func doBuild(Session session, Expression[] args) {
      return new DatabaseFunc();
    }
  }

}
