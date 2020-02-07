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
package io.jimdb.core.expression.functions.builtin;

import io.jimdb.common.exception.JimException;
import io.jimdb.core.Session;
import io.jimdb.core.context.PreparedContext;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.expression.functions.Func;
import io.jimdb.core.expression.functions.UnaryFuncBuilder;
import io.jimdb.core.types.ValueType;
import io.jimdb.core.values.DateValue;
import io.jimdb.core.values.DecimalValue;
import io.jimdb.core.values.DoubleValue;
import io.jimdb.core.values.JsonValue;
import io.jimdb.core.values.LongValue;
import io.jimdb.core.values.StringValue;
import io.jimdb.core.values.TimeValue;
import io.jimdb.core.values.Value;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings("CN_IDIOM_NO_SUPER_CALL")
public class GetParamFunc extends Func {

  public GetParamFunc() {
    this.name = "GetParam";
  }

  public GetParamFunc(Session session, Expression[] args) {
    super(session, args, ValueType.STRING, ValueType.LONG);
  }

  /**
   * @version V1.0
   */
  public static final class GetParamFuncBuilder extends UnaryFuncBuilder {
    public GetParamFuncBuilder(String name) {
      super(name);
    }

    @Override
    protected Func doBuild(Session session, Expression[] args) {
      return new GetParamFunc(session, args);
    }
  }

  @Override
  public GetParamFunc clone() {
    GetParamFunc newFunc = new GetParamFunc();
    clone(newFunc);
    return newFunc;
  }

  @Override
  public StringValue execString(ValueAccessor accessor) throws JimException {
    Value realValue = getValue();
    return StringValue.getInstance(realValue.getString());
  }

  @Override
  public LongValue execLong(ValueAccessor accessor) throws JimException {
    Value realValue = getValue();
    return (LongValue) realValue;
  }

  @Override
  public DecimalValue execDecimal(ValueAccessor accessor) throws JimException {
    Value realValue = getValue();
    return (DecimalValue) realValue;
  }

  @Override
  public DateValue execDate(ValueAccessor accessor) throws JimException {
    Value realValue = getValue();
    return (DateValue) realValue;
  }

  @Override
  public DoubleValue execDouble(ValueAccessor accessor) throws JimException {
    Value realValue = getValue();
    return (DoubleValue) realValue;
  }

  @Override
  public JsonValue execJson(ValueAccessor accessor) throws JimException {
    Value realValue = getValue();
    return JsonValue.getInstance(realValue.getString());
  }

  @Override
  public TimeValue execTime(ValueAccessor accessor) throws JimException {
    Value realValue = getValue();
    return (TimeValue) realValue;
  }

  private Value getValue() {
    PreparedContext prepareContext = session.getPreparedContext();
    LongValue longValue = args[0].execLong(session, null);
    Long value = longValue.getValue();
    return prepareContext.getParamValues()[value.intValue()];
  }
}
