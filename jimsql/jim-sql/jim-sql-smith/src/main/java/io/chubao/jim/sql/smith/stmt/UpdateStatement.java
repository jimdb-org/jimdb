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

import io.jimdb.model.Column;
import io.jimdb.model.Table;
import io.jimdb.sql.smith.Random;
import io.jimdb.sql.smith.Smither;
import io.jimdb.sql.smith.cxt.Scope;
import io.jimdb.sql.smith.scalar.Scalar;
import io.jimdb.sql.smith.table.SimpleTableSourceExpr;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLUpdateSetItem;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;

/**
 * @version V1.0
 */
public final class UpdateStatement extends StatementWeight {

  public UpdateStatement(Scalar scalar, int weight) {
    this.weight = weight;
    this.scalar = scalar;
  }

  @Override
  public SQLObject makeStatement(Scope scope) {

    Smither smither = scope.getSmither();
    Table table = smither.getRandomTable();
    SimpleTableSourceExpr sourceExpr = new SimpleTableSourceExpr(0);
    SQLExprTableSource tableSource = (SQLExprTableSource) sourceExpr.makeTableExpr(table);

    MySqlUpdateStatement updateStatement = new MySqlUpdateStatement();
    updateStatement.setTableSource(tableSource);

    updateStatement.setWhere(makeWhere(scope, table));

    updateStatement.setLimit(makeLimit(scope));

    Column[] columns = table.getColumns();

    for (Column column : columns) {
      if (Random.coin()) {
        SQLUpdateSetItem setItem = new SQLUpdateSetItem();
        SQLIdentifierExpr sqlIdentifierExpr = new SQLIdentifierExpr();
        sqlIdentifierExpr.setName(column.getName());
        SQLExpr sqlExpr = this.scalar.makeScalar(scope, columns, column.getType().getType());
        setItem.setColumn(sqlIdentifierExpr);
        setItem.setValue(sqlExpr);
        updateStatement.addItem(setItem);
      }
    }

    return updateStatement;
  }
}
