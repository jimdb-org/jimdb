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

import io.jimdb.core.expression.functions.Func;
import io.jimdb.core.expression.functions.UnaryFuncBuilder;
import io.jimdb.core.Session;
import io.jimdb.common.exception.JimException;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.pb.Exprpb;
import io.jimdb.core.types.ValueType;
import io.jimdb.core.values.DoubleValue;
import io.jimdb.core.values.LongValue;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings("CN_IDIOM_NO_SUPER_CALL")
public class FloorFunc extends Func {
  private FloorFunc() {

  }

  protected FloorFunc(Session session, Expression[] args) {
    super(session, args, ValueType.LONG, ValueType.DECIMAL);
    this.code = Exprpb.ExprType.Floor;
    this.name = code.name();
  }

  @Override
  public Func clone() {
    FloorFunc floorFunc = new FloorFunc();
    clone(floorFunc);
    return floorFunc;
  }

  @Override
  public LongValue execLong(ValueAccessor accessor) throws JimException {
    DoubleValue doubleValue = args[0].execDouble(session, accessor);
    Double floor = Math.floor(doubleValue.getValue());
    return LongValue.getInstance(floor.intValue());
  }

  /**
   * floorFuncBuilder
   */
  public static final class FloorFuncBuilder extends UnaryFuncBuilder {
    public FloorFuncBuilder(String name) {
      super(name);
    }

    @Override
    protected Func doBuild(Session session, Expression[] args) {
      return new FloorFunc(session, args);
    }
  }
}
