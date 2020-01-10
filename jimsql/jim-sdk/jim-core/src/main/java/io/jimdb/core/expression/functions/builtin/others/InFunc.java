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
package io.jimdb.core.expression.functions.builtin.others;

import io.jimdb.core.expression.functions.Func;
import io.jimdb.core.expression.functions.FuncBuilder;
import io.jimdb.core.Session;
import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.exception.JimException;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.pb.Exprpb.ExprType;
import io.jimdb.core.types.Types;
import io.jimdb.core.types.ValueType;
import io.jimdb.core.values.LongValue;
import io.jimdb.core.values.Value;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings("CLI_CONSTANT_LIST_INDEX")
public final class InFunc extends Func {

  private InFunc() {
  }

  protected InFunc(Session session, Expression[] args, ValueType retTp, ValueType... expectArgs) {
    super(session, args, retTp, expectArgs);

    ValueType valueType = Types.sqlToValueType(args[0].getResultType());
    switch (valueType) {
      case LONG:
        // TODO in long
        this.code = ExprType.IN;
        break;
      case UNSIGNEDLONG:
        this.code = ExprType.IN;
        break;
      case DOUBLE:
        this.code = ExprType.IN;
        break;
      case DECIMAL:
        this.code = ExprType.IN;
        break;
      case STRING:
        this.code = ExprType.IN;
        break;
      case BINARY:
        this.code = ExprType.IN;
        break;
      default:
        throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_NOT_SUPPORTED_YET, valueType.name());
    }
    this.name = this.code.name();
//    this.resultType.setPrecision(1);
  }

  @Override
  public Func clone() {
    InFunc result = new InFunc();
    clone(result);
    return result;
  }

  @Override
  public LongValue execLong(ValueAccessor accessor) throws JimException {
    Value arg0 = args[0].exec(accessor);
    if (arg0.isNull()) {
      return LongValue.getInstance(0);
    }

    for (int i = 1; i < args.length; i++) {
      Value value = args[i].exec(accessor);
      if (value.isNull()) {
        continue;
      }
      if (value.compareTo(session, arg0) == 0) {
        return LongValue.getInstance(1);
      }
    }

    return LongValue.getInstance(0);
  }


  /**
   * @version V1.0
   */
  public static final class InFuncBuilder extends FuncBuilder {
    public InFuncBuilder(String name) {
      super(name, 2, Integer.MAX_VALUE);
    }

    @Override
    protected Func doBuild(Session session, Expression[] args) {
      ValueType[] expectArgs = new ValueType[args.length];
      for (int i = 1; i < args.length; i++) {
        expectArgs[i] = Types.sqlToValueType(args[0].getResultType());
      }
      return new InFunc(session, args, ValueType.LONG, expectArgs);
    }
  }

}
