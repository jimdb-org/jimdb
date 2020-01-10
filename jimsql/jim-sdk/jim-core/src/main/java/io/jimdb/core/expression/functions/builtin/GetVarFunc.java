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
package io.jimdb.core.expression.functions.builtin;

import io.jimdb.core.expression.functions.Func;
import io.jimdb.core.Session;
import io.jimdb.common.exception.JimException;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.expression.functions.UnaryFuncBuilder;
import io.jimdb.core.types.ValueType;
import io.jimdb.core.values.StringValue;

/**
 * @version V1.0
 */
public final class GetVarFunc extends Func {
  private GetVarFunc() {
  }

  protected GetVarFunc(Session session, Expression[] args) {
    super(session, args, ValueType.STRING, ValueType.STRING);
    this.name = "GetVariable";
  }

  @Override
  public GetVarFunc clone() {
    GetVarFunc result = new GetVarFunc();
    clone(result);
    return result;
  }

  @Override
  public StringValue execString(ValueAccessor accessor) throws JimException {
    StringValue stringValue = args[0].execString(session, accessor);
    String userVariable = session.getVarContext().getUserVariable(stringValue.getString());
    return StringValue.getInstance(userVariable);
  }

  /**
   * @version V1.0
   */
  public static final class GetVarFuncBuilder extends UnaryFuncBuilder {
    public GetVarFuncBuilder(String name) {
      super(name);
    }

    @Override
    protected Func doBuild(Session session, Expression[] args) {
      return new GetVarFunc(session, args);
    }
  }
}
