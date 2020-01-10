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

package io.jimdb.core.expression.functions.builtin.math;

import static io.jimdb.pb.Exprpb.ExprType.Ceil;

import io.jimdb.core.expression.functions.Func;
import io.jimdb.core.expression.functions.UnaryFuncBuilder;
import io.jimdb.core.Session;
import io.jimdb.common.exception.JimException;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.types.ValueType;
import io.jimdb.core.values.DoubleValue;
import io.jimdb.core.values.LongValue;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings("CN_IDIOM_NO_SUPER_CALL")
public class CeilFunc extends Func {
  private CeilFunc() {
  }

  protected CeilFunc(Session session, Expression[] args) {
    super(session, args, ValueType.LONG, ValueType.DECIMAL);
    this.code = Ceil;
    this.name = code.name();
  }
  @Override
  public Func clone() {
    CeilFunc ceilFunc = new CeilFunc();
    clone(ceilFunc);
    return ceilFunc;
  }

  @Override
  public LongValue execLong(ValueAccessor accessor) throws JimException {
    DoubleValue doubleValue = args[0].execDouble(session, accessor);
    Double ceil = Math.ceil(doubleValue.getValue());
    return LongValue.getInstance(ceil.intValue());
  }

  /**
   * @version V1.0
   */
  public static final class CeilFuncBuilder extends UnaryFuncBuilder {
    public CeilFuncBuilder(String name) {
      super(name);
    }

    @Override
    protected Func doBuild(Session session, Expression[] args) {
      return new CeilFunc(session, args);
    }
  }
}
