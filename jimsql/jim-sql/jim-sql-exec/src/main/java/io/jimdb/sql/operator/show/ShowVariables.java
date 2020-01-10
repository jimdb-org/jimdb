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
package io.jimdb.sql.operator.show;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.jimdb.core.Session;
import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.JimException;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.RowValueAccessor;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.model.result.impl.QueryExecResult;
import io.jimdb.sql.operator.RelOperator;
import io.jimdb.core.values.StringValue;
import io.jimdb.core.values.Value;
import io.jimdb.core.variable.SysVariable;

import reactor.core.publisher.Flux;

/**
 * @version V1.0
 */
public final class ShowVariables extends RelOperator {
  private boolean global;

  public void setGlobal(boolean global) {
    this.global = global;
  }

  @Override
  public Flux<ExecResult> execute(Session session) throws JimException {
    List<ValueAccessor> valueAccessors = new ArrayList<>();
    Map<String, SysVariable> variableMap = session.getVarContext().getAllVars();
    variableMap.forEach((k, v) -> {
      String value;
      if (global) {
        value = session.getVarContext().getNoneVariable(k);
      } else {
        try {
          value = session.getVarContext().getSessionVariable(k);
        } catch (DBException ex) {
          value = session.getVarContext().getNoneVariable(k);
        }
      }
      valueAccessors.add(new RowValueAccessor(new Value[]{ StringValue.getInstance(k), StringValue.getInstance(value) }));
    });

    return Flux.just(new QueryExecResult(this.schema.getColumns().toArray(new ColumnExpr[0]), valueAccessors.toArray
            (new ValueAccessor[0])));
  }
}
