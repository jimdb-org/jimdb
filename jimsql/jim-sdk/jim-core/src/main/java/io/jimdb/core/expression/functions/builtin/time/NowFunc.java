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
package io.jimdb.core.expression.functions.builtin.time;

import io.jimdb.common.exception.BaseException;
import io.jimdb.core.Session;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.expression.functions.Func;
import io.jimdb.core.expression.functions.FuncBuilder;
import io.jimdb.core.types.ValueType;
import io.jimdb.core.values.DateValue;
import io.jimdb.pb.Basepb;
import io.jimdb.pb.Exprpb;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings("CN_IDIOM_NO_SUPER_CALL")
public class NowFunc extends Func {

  protected static final int MAX_FSP = 6;
  protected static final int MIN_FSP = 0;
  protected static final int DEFAULT_FSP = 0;
  public static final String TIMESTAMP = "timestamp";

  private NowFunc() {

  }

  protected NowFunc(Session session) {
    super(session, new Expression[]{}, ValueType.DATE);
    this.code = Exprpb.ExprType.Now;
    this.name = this.code.name();
  }

  protected NowFunc(Session session, Expression arg) {
    super(session, new Expression[]{ arg }, ValueType.DATE, new ValueType[]{ ValueType.LONG });
    this.code = Exprpb.ExprType.Now;
    this.name = this.code.name();
  }

  @Override
  public DateValue execDate(ValueAccessor accessor) throws BaseException {
    int fsp = DEFAULT_FSP;
    if (args.length > 0) {
      fsp = Math.toIntExact(args[0].execLong(session, accessor).getValue());
    }

    if (fsp > MAX_FSP) {
      //todo exception
    } else if (fsp < MIN_FSP) {
      //todo exception
    }

    return getNowWithFsp(session, fsp);
  }

  private DateValue getNowWithFsp(Session session, int fsp) {
    DateValue now = DateValue.getNow(Basepb.DataType.DateTime, fsp, session.getStmtContext().getLocalTimeZone());
    String timestampStr = session.getVarContext().getSessionVariable(TIMESTAMP);
    if (timestampStr != null && !timestampStr.isEmpty()) {
      Long timestamp = Long.valueOf(timestampStr);
      if (timestamp < 0) {
        return now;
      }
      return DateValue.getInstance(timestamp, Basepb.DataType.DateTime, fsp);
    }
    DateValue cachedNow = session.getStmtContext().getNowCached(session.getStmtContext().getLocalTimeZone());
    return DateValue.convertToMysqlNow(cachedNow, fsp);
  }

  @Override
  public Func clone() {
    NowFunc result = new NowFunc();
    clone(result);
    return result;
  }

  /**
   * @version V1.0
   */
  public static final class NowFuncBuilder extends FuncBuilder {
    public NowFuncBuilder(String name) {
      super(name, 0, 1);
    }

    @Override
    protected Func doBuild(Session session, Expression[] args) {
      if (args.length == 1) {
        return new NowFunc(session, args[0]);
      }
      return new NowFunc(session);
    }
  }
}
