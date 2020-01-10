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
package io.jimdb.core.expression.functions.builtin.logic;

import io.jimdb.core.expression.functions.Func;
import io.jimdb.core.expression.functions.UnaryFuncBuilder;
import io.jimdb.core.Session;
import io.jimdb.common.exception.JimException;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.pb.Exprpb;
import io.jimdb.pb.Metapb.SQLType;
import io.jimdb.core.types.ValueType;
import io.jimdb.core.values.LongValue;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings("UP_UNUSED_PARAMETER")
public final class NotFunc extends Func {
  private NotFunc() {
  }

  protected NotFunc(Session session, Expression[] args) {
    super(session, args, ValueType.LONG, ValueType.LONG);
    this.code = Exprpb.ExprType.UnaryNot;
    this.name = this.code.name();
    SQLType.Builder builder = resultType.toBuilder();
    builder.setPrecision(1);
    this.resultType = builder.build();
  }

  @Override
  public NotFunc clone() {
    NotFunc result = new NotFunc();
    clone(result);
    return result;
  }

  @Override
  public LongValue execLong(ValueAccessor accessor) throws JimException {
    LongValue v = args[0].execLong(session, accessor);
    if (v == null) {
      return null;
    }
    return v.getValue() == 0 ? LongValue.getInstance(1) : LongValue.getInstance(0);
  }

  /**
   * NotFunc Builder.
   */
  public static final class NotFuncBuilder extends UnaryFuncBuilder {
    public NotFuncBuilder(String name) {
      super(name);
    }

    @Override
    protected Func doBuild(Session session, Expression[] args) {
      return new NotFunc(session, args);
    }
  }
}
