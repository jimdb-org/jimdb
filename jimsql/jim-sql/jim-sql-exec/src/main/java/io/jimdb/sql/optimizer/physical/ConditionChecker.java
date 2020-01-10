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
package io.jimdb.sql.optimizer.physical;

import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.FuncExpr;
import io.jimdb.core.expression.functions.FuncType;

/**
 * @version V1.0
 */
public class ConditionChecker {

  private long uniqueId;

  public ConditionChecker(long id) {
    this.uniqueId = id;
  }

  public boolean check(Expression expression) {
    switch (expression.getExprType()) {
      case FUNC:
        return checkFunc((FuncExpr) expression);
      case COLUMN:
        return checkColumn(expression);
      case CONST:
        return true;
      default:
        return false;
    }
  }

  private boolean checkFunc(FuncExpr funcExpr) {
    FuncType funcType = funcExpr.getFuncType();
    if (funcType == FuncType.BooleanAnd
            || funcType == FuncType.BooleanOr) {
      if (funcExpr.getArgs().length == 2) {
        boolean flag = true;
        for (int i = 0; i < funcExpr.getArgs().length; i++) {
          flag = flag & check(funcExpr.getArgs()[i]);
        }
        return flag;
      }
    } else if (funcType == FuncType.Equality
            || funcType == FuncType.NotEqual
            || funcType == FuncType.GreaterThanOrEqual
            || funcType == FuncType.GreaterThan
            || funcType == FuncType.LessThanOrEqual
            || funcType == FuncType.LessThan) {
      if (funcExpr.getArgs().length == 2) {
        boolean flag = true;
        for (int i = 0; i < funcExpr.getArgs().length; i++) {
          flag = flag & check(funcExpr.getArgs()[i]);
        }
        return flag;
      }
    }
    return false;
  }

  private boolean checkColumn(Expression expression) {
    if (!(expression instanceof ColumnExpr)) {
      return false;
    }
    // TODO: we should use getUid() here
    Integer id = ((ColumnExpr) expression).getId();
    return id != null && id.longValue() == this.uniqueId;
  }
}
