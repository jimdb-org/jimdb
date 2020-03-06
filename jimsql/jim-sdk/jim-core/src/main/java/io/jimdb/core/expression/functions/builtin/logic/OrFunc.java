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

import io.jimdb.core.expression.functions.BinaryFuncBuilder;
import io.jimdb.core.expression.functions.Func;
import io.jimdb.core.Session;
import io.jimdb.common.exception.BaseException;
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
@SuppressFBWarnings("CLI_CONSTANT_LIST_INDEX")
public final class OrFunc extends Func {
  private OrFunc() {
  }

  protected OrFunc(Session session, Expression[] args) {
    super(session, args, ValueType.LONG, ValueType.LONG, ValueType.LONG);
    this.code = Exprpb.ExprType.LogicOr;
    this.name = this.code.name();
    SQLType.Builder builder = resultType.toBuilder();
    builder.setPrecision(1);
    this.resultType = builder.build();
  }

  @Override
  public OrFunc clone() {
    OrFunc result = new OrFunc();
    clone(result);
    return result;
  }

  @Override
  public LongValue execLong(ValueAccessor accessor) throws BaseException {
    LongValue v1 = args[0].execLong(session, accessor);
    if (v1 != null && v1.getValue() != 0) {
      return LongValue.getInstance(1);
    }

    int result = 1;
    LongValue v2 = args[1].execLong(session, accessor);
    if (v2 == null || v2.getValue() == 0) {
      result = 0;
    }
    return LongValue.getInstance(result);
  }

  /**
   *
   */
  public static final class OrFuncBuilder extends BinaryFuncBuilder {
    public OrFuncBuilder(String name) {
      super(name);
    }

    @Override
    protected Func doBuild(Session session, Expression[] args) {
      return new OrFunc(session, args);
    }
  }
}
