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

import java.util.ArrayList;
import java.util.List;

import io.jimdb.model.Column;
import io.jimdb.model.Table;
import io.jimdb.pb.Basepb;
import io.jimdb.sql.smith.Random;
import io.jimdb.sql.smith.Smither;
import io.jimdb.sql.smith.cxt.Scope;
import io.jimdb.sql.smith.scalar.Scalar;
import io.jimdb.sql.smith.table.SimpleTableSourceExpr;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;

/**
 * @version V1.0
 */
public final class InsertStatement extends StatementWeight {


  public InsertStatement(Scalar scalar, int weight) {
    this.weight = weight;
    this.scalar = scalar;
  }

  @Override
  public SQLObject makeStatement(Scope scope) {

    Smither smither = scope.getSmither();

    MySqlInsertStatement insertStatement = new MySqlInsertStatement();

    Table table = smither.getRandomTable();
    SimpleTableSourceExpr sourceExpr = new SimpleTableSourceExpr(0);
    SQLExprTableSource tableSource = (SQLExprTableSource) sourceExpr.makeTableExpr(table);

    insertStatement.setTableSource(tableSource);
    boolean unNamed = Random.coin();


    Column[] columns = table.getColumns();

    List<Basepb.DataType> types = new ArrayList<>();

    for (int i = 0; i < columns.length; i++) {
      if (unNamed || columns[i].getType().isNotNull() || Random.coin()) {
        SQLIdentifierExpr sqlIdentifierExpr = new SQLIdentifierExpr();
        sqlIdentifierExpr.setName(columns[i].getName());
        types.add(columns[i].getType().getType());
        insertStatement.addColumn(sqlIdentifierExpr);
      }
    }

    if (!types.isEmpty()) {
      for (Basepb.DataType type : types) {
        SQLInsertStatement.ValuesClause valuesClause = new SQLInsertStatement.ValuesClause();
        SQLExpr value = this.scalar.makeScalar(scope, columns, type);
        valuesClause.addValue(value);
        insertStatement.setValues(valuesClause);
      }
    }

    return insertStatement;
  }
}













