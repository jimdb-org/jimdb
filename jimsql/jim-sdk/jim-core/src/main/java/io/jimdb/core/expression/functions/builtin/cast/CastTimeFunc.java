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
package io.jimdb.core.expression.functions.builtin.cast;

import io.jimdb.core.expression.functions.Func;
import io.jimdb.core.Session;
import io.jimdb.common.exception.JimException;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.values.TimeValue;
import io.jimdb.core.values.Value;
import io.jimdb.core.values.ValueConvertor;
import io.jimdb.pb.Exprpb;
import io.jimdb.pb.Metapb.SQLType;
import io.jimdb.core.values.LongValue;
import io.jimdb.core.values.StringValue;

/**
 * @version V1.0
 */
final class CastTimeFunc extends Func {
  private CastTimeFunc() {
  }

  protected CastTimeFunc(Exprpb.ExprType code, Expression[] args, SQLType resultType, Session session) {
    super();
    this.code = code;
    this.name = code.name();
    this.args = args;
    this.resultType = resultType;
    this.session = session;
  }

  @Override
  public LongValue execLong(ValueAccessor accessor) throws JimException {
    return args[0].execLong(session, accessor);
  }

  @Override
  public StringValue execString(ValueAccessor accessor) throws JimException {
    return args[0].execString(session, accessor);
  }

  @Override
  public TimeValue execTime(ValueAccessor accessor) throws JimException {
    final Expression arg = args[0];
    final Value value = arg.execTime(session, accessor);
    if (value == null) {
      return null;
    }
    return ValueConvertor.convertToTime(session, value, resultType);
  }

  @Override
  public CastTimeFunc clone() {
    CastTimeFunc result = new CastTimeFunc();
    clone(result);
    return result;
  }
}
