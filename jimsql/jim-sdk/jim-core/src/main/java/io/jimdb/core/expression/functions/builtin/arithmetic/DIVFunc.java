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
package io.jimdb.core.expression.functions.builtin.arithmetic;

import io.jimdb.core.Session;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.expression.functions.BinaryFuncBuilder;
import io.jimdb.core.expression.functions.Func;
import io.jimdb.core.types.ValueType;
import io.jimdb.core.values.DecimalValue;
import io.jimdb.core.values.DoubleValue;
import io.jimdb.core.values.LongValue;
import io.jimdb.core.values.UnsignedLongValue;
import io.jimdb.core.values.ValueConvertor;
import io.jimdb.common.exception.JimException;
import io.jimdb.pb.Exprpb;
import io.jimdb.pb.Metapb.SQLType;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings({ "CLI_CONSTANT_LIST_INDEX", "CN_IDIOM_NO_SUPER_CALL", "NAB_NEEDLESS_BOOLEAN_CONSTANT_CONVERSION" })
public class DIVFunc extends Func {
  public DIVFunc() {
  }

  public DIVFunc(Session session, Exprpb.ExprType code, Expression[] args, ValueType retTp, ValueType... expectArgs) {
    super(session, args, retTp, expectArgs);
    this.code = code;
    this.name = code.name();
  }

  @Override
  public Func clone() {
    DIVFunc divFunc = new DIVFunc();
    clone(divFunc);
    return divFunc;
  }

  public LongValue execLong(ValueAccessor accessor) throws JimException {
    LongValue v1 = args[0].execLong(session, accessor);
    if (v1 == null) {
      return null;
    }
    LongValue v2 = args[1].execLong(session, accessor);
    if (v2 == null) {
      return null;
    }

    return ValueConvertor.convertToLong(session, v1.divide(session, v2), null);
  }

  @Override
  public UnsignedLongValue execUnsignedLong(ValueAccessor accessor) throws JimException {
    UnsignedLongValue v1 = args[0].execUnsignedLong(session, accessor);
    if (v1 == null) {
      return null;
    }
    UnsignedLongValue v2 = args[1].execUnsignedLong(session, accessor);
    if (v2 == null) {
      return null;
    }

    return (UnsignedLongValue) v1.divide(session, v2);
  }

  @Override
  public DoubleValue execDouble(ValueAccessor accessor) throws JimException {
    DoubleValue v1 = args[0].execDouble(session, accessor);
    if (v1 == null) {
      return null;
    }
    DoubleValue v2 = args[1].execDouble(session, accessor);
    if (v2 == null) {
      return null;
    }
    return (DoubleValue) v1.divide(session, v2);
  }

  @Override
  public DecimalValue execDecimal(ValueAccessor accessor) throws JimException {
    DecimalValue v1 = args[0].execDecimal(session, accessor);
    if (v1 == null) {
      return null;
    }
    DecimalValue v2 = args[1].execDecimal(session, accessor);
    if (v2 == null) {
      return null;
    }

    return (DecimalValue) v1.divide(session, v2);
  }

  /**
   *
   */
  public static class DIVFuncBuilder extends BinaryFuncBuilder {

    public DIVFuncBuilder(String name) {
      super(name);
    }

    @Override
    protected Func doBuild(Session session, Expression[] args) {
      SQLType arg1Type = args[0].getResultType();
      SQLType arg2Type = args[1].getResultType();
      ValueType arg1ValueType = ArithmeticUtil.getArithmeticType(arg1Type);
      ValueType arg2ValueType = ArithmeticUtil.getArithmeticType(arg2Type);

      if (arg1ValueType == ValueType.LONG && arg2ValueType == ValueType.LONG) {
        DIVFunc result = new DIVFunc(session, Exprpb.ExprType.IntDivInt, args, ValueType.LONG, ValueType.DOUBLE,
                ValueType.DOUBLE);
        if (arg1Type.getUnsigned() || arg2Type.getUnsigned()) {
          SQLType.Builder resultType = result.getResultType().toBuilder();
          resultType.setUnsigned(true);
          result.setResultType(resultType.build());
        }
        return result;
      }

      DIVFunc result = new DIVFunc(session, Exprpb.ExprType.IntDivDecimal, args, ValueType.LONG, ValueType.DECIMAL,
              ValueType.DECIMAL);
      if (arg1Type.getUnsigned() || arg2Type.getUnsigned()) {
        SQLType.Builder resultType = result.getResultType().toBuilder();
        resultType.setUnsigned(true);
        result.setResultType(resultType.build());
      }
      return result;
    }
  }
}
