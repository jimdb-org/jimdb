/*
 * Copyright 2019 The Chubao Authors.
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
package io.jimdb.sql.smith.stmt;

import java.util.List;

import io.jimdb.model.Table;
import io.jimdb.sql.smith.Random;
import io.jimdb.sql.smith.Weight;
import io.jimdb.sql.smith.cxt.Scope;
import io.jimdb.sql.smith.scalar.Scalar;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLLimit;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectOrderByItem;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings("FCCD_FIND_CLASS_CIRCULAR_DEPENDENCY")
public abstract class StatementWeight extends Weight {
  protected  Scalar scalar;

  public abstract SQLObject makeStatement(Scope scope);

  public SQLOrderBy makeOrderBy(Scope scope, Table table, List<SQLSelectItem> selectItems) {
    if (selectItems == null || selectItems.isEmpty()) {
      return null;
    }

    SQLOrderBy sqlOrderBy = new SQLOrderBy();
    if (Random.coin()) {
      SQLSelectItem sqlSelectItem = selectItems.get(Random.getRandomInt(selectItems.size()));
      SQLSelectOrderByItem selectOrderByItem = new SQLSelectOrderByItem();
      selectOrderByItem.setExpr(sqlSelectItem.getExpr());
      selectOrderByItem.setType(scope.getSmither().randDirection());
      sqlOrderBy.addItem(selectOrderByItem);
    }
    return sqlOrderBy;
  }

  public SQLExpr makeWhere(Scope scope, Table table) {
    if (Random.coin()) {
      return scalar.makeBoolExpr(scope, table.getColumns());
    }
    return null;
  }

  public SQLLimit makeLimit(Scope scope) {
    if (!scope.getSmither().isDisableLimits()) {
      if (Random.rand6() > 2) {
        SQLLimit sqlLimit = new SQLLimit();
        sqlLimit.setRowCount(Random.rand100());
      }
    }
    return null;
  }

}
