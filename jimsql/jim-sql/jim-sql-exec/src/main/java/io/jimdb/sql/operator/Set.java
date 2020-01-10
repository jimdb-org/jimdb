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
package io.jimdb.sql.operator;

import io.jimdb.core.Session;
import io.jimdb.core.context.VariableContext;
import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.exception.JimException;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.model.result.impl.AckExecResult;
import io.jimdb.sql.analyzer.Variables.Variable;
import io.jimdb.sql.analyzer.Variables.VariableAssign;
import io.jimdb.core.values.StringValue;
import io.jimdb.core.variable.SysVariable;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.core.publisher.Flux;

/**
 * @version V1.0
 */
public final class Set extends Operator {
  private final VariableAssign[] assigns;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public Set(VariableAssign... assigns) {
    this.assigns = assigns;
  }

  @Override
  public OperatorType getOperatorType() {
    return OperatorType.SIMPLE;
  }

  @Override
  public Flux<ExecResult> execute(Session session) throws JimException {
    Variable var;
    VariableContext varContext = session.getVarContext();
    for (VariableAssign assign : assigns) {
      var = assign.getVariable();
      if (!var.isSystem()) {
        if (assign.isDefaultValue()) {
          varContext.setUserVariable(var.getName(), "");
        } else {
          StringValue value = assign.getValue().execString(session, ValueAccessor.EMPTY);
          if (value == null) {
            varContext.setUserVariable(var.getName(), null);
          } else {
            varContext.setUserVariable(var.getName(), value.getString());
          }
        }
        continue;
      }

      this.setSysVariable(session, assign);
    }
    return Flux.just(AckExecResult.getInstance());
  }

  private void setSysVariable(Session session, VariableAssign assign) throws JimException {
    final Variable var = assign.getVariable();
    final String name = var.getName();
    final VariableContext varContext = session.getVarContext();
    final SysVariable sysVar = varContext.getVariableDefine(name);
    if (sysVar == null) {
      throw DBException.get(ErrorModule.EXECUTOR, ErrorCode.ER_UNKNOWN_SYSTEM_VARIABLE, name);
    }
    if (sysVar.isNone()) {
      throw DBException.get(ErrorModule.EXECUTOR, ErrorCode.ER_VARIABLE_IS_READONLY, name);
    }

    if (var.isGlobal()) {
      if (!sysVar.isGlobal()) {
        throw DBException.get(ErrorModule.EXECUTOR, ErrorCode.ER_LOCAL_VARIABLE, name);
      }
      String value = this.getVarValue(session, assign, sysVar);
      varContext.setGlobalVariable(name, value);
      return;
    }

    // Set SESSION system variable.
    if (!sysVar.isSession()) {
      throw DBException.get(ErrorModule.EXECUTOR, ErrorCode.ER_GLOBAL_VARIABLE, name);
    }
    String value = this.getVarValue(session, assign, sysVar);
    varContext.setSessionVariable(name, value);
  }

  private String getVarValue(Session session, VariableAssign assign, SysVariable sysVar) {
    if (assign.isDefaultValue()) {
      if (sysVar != null) {
        return sysVar.getValue();
      }

      String value = session.getVarContext().getGlobalVariable(assign.getVariable().getName());
      return value == null ? "" : value;
    }

    StringValue value = assign.getValue().execString(session, ValueAccessor.EMPTY);
    return value == null ? "" : value.getString();
  }
}
