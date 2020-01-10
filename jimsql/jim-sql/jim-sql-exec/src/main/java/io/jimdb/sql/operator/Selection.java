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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import io.jimdb.core.Session;
import io.jimdb.common.exception.JimException;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.model.result.impl.QueryExecResult;
import io.jimdb.sql.optimizer.statistics.OperatorStatsInfo;
import io.jimdb.sql.optimizer.OperatorVisitor;
import io.jimdb.sql.optimizer.ParameterizedOperatorVisitor;
import io.jimdb.core.values.LongValue;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.core.publisher.Flux;

/**
 * @version V1.0
 */
@SuppressFBWarnings({ "EI_EXPOSE_REP", "FCBL_FIELD_COULD_BE_LOCAL" })
public final class Selection extends RelOperator {

  private List<Expression> conditions;

  public Selection(List<Expression> conditions, RelOperator... children) {
    this.conditions = conditions;
    this.children = children;
  }

  public Selection(List<Expression> conditions, OperatorStatsInfo statInfo) {
    this.conditions = conditions;
    this.statInfo = statInfo;
  }

  private Selection(Selection selection) {
    this.conditions = selection.conditions;
    this.copyBaseParameters(selection);
  }

  public List<Expression> getConditions() {
    return conditions;
  }

  public void setConditions(List<Expression> conditions) {
    this.conditions = conditions;
  }

  @Override
  public Selection copy() {
    Selection selection = new Selection(this);
    selection.children = this.copyChildren();

    return selection;
  }

  @Override
  public Flux<ExecResult> execute(Session session) throws JimException {
    if (hasChildren()) {
      return children[0].execute(session).map(execResult -> {
        List<ValueAccessor> list = new ArrayList<>();
        //All filtering conditions are and relationships;
        //"execLong" method to determine whether the filter conditions are met
        //1 is satisfied, 0 is not satisfied
        execResult.forEach(row -> {
          boolean flag = true;
          for (Expression condition : conditions) {
            LongValue value = condition.execLong(session, row);
            if (value == null || value.getValue() == 0) {
              flag = false;
              break;
            }
          }
          //Join the row if the condition is satisfied
          if (flag) {
            list.add(row);
          }
        });
        return new QueryExecResult(execResult.getColumns(), list.toArray(new ValueAccessor[list.size()]));
      });
    }
    return Flux.just(new QueryExecResult(getSchema().getColumns().toArray(new ColumnExpr[getSchema().getColumns().size()]), new ValueAccessor[0]));
  }

  @Override
  public ColumnExpr[][] getCandidatesProperties() {
    return this.getChildren()[0].getCandidatesProperties();
  }

  @Override
  public void resolveOffset() {
    if (hasChildren()) {
      Arrays.stream(children).forEach(Operator::resolveOffset);
    }

    if (this.conditions != null) {
      for (int i = 0; i < this.conditions.size(); i++) {
        this.conditions.set(i, this.conditions.get(i).resolveOffset(children[0].getSchema(), true));
      }
    }
  }

  public <T, R> R acceptVisitor(Session session, ParameterizedOperatorVisitor<T, R> visitor, T t) {
    return visitor.visitOperator(session, this, t);
  }

  @Override
  public <R> R acceptVisitor(OperatorVisitor<R> visitor) {
    return visitor.visitOperator(this);
  }

  @Override
  public String getName() {
    return "Selection";
  }

  @Override
  @SuppressFBWarnings({ "ITC_INHERITANCE_TYPE_CHECKING", "UCPM_USE_CHARACTER_PARAMETERIZED_METHOD" })
  public String toString() {
    StringBuilder sb = new StringBuilder("Selection={");
    sb.append(export(conditions.toArray(new Expression[0])));
    sb.append('}');
    return sb.toString();
  }
}
