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

import io.jimdb.model.Table;
import io.jimdb.pb.Basepb;
import io.jimdb.sql.smith.Random;
import io.jimdb.sql.smith.Smither;
import io.jimdb.sql.smith.cxt.Context;
import io.jimdb.sql.smith.cxt.Scope;
import io.jimdb.sql.smith.scalar.Scalar;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLSetQuantifier;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings({ "DLS_DEAD_LOCAL_STORE", "NP_UNWRITTEN_FIELD", "UWF_UNWRITTEN_FIELD",
        "WOC_WRITE_ONLY_COLLECTION_LOCAL", "NP_NULL_PARAM_DEREF_ALL_TARGETS_DANGEROUS",
        "RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE", "PSC_PRESIZE_COLLECTIONS"})
public final class SelectStatement extends StatementWeight {


  public SelectStatement(Scalar scalar, int weight) {
    this.weight = weight;
    this.scalar = scalar;
  }

  @Override
  public SQLObject makeStatement(Scope scope) {
    Smither smither = scope.getSmither();

    SQLSelectStatement sqlSelectStatement = new SQLSelectStatement();
    SQLSelect sqlSelect = new SQLSelect();
    MySqlSelectQueryBlock queryBlock = new MySqlSelectQueryBlock();
    Table table = smither.getRandomTable();
    SQLTableSource tableSource = (SQLTableSource) smither.getTableExprWeights()[smither.getTableExprWeight().getNext()]
            .makeTableExpr(table);
    queryBlock.setFrom(tableSource);
    if (queryBlock.getFrom() != null) {
      queryBlock.setWhere(makeWhere(scope, table));
    }

    if (Random.rand6() <= 2) {
      //TODO group by
      //TODO Having
    }

    List<SQLSelectItem> selectItems = makeSelectList(scope, null, new Context(), table);
    for (SQLSelectItem selectItem : selectItems) {
      queryBlock.addSelectItem(selectItem);
    }

    if (Random.rand100() == 1) {
      queryBlock.setDistionOption(SQLSetQuantifier.DISTINCT);
    }

    queryBlock.setOrderBy(makeOrderBy(scope, table, selectItems));

    queryBlock.setLimit(makeLimit(scope));
    sqlSelect.setQuery(queryBlock);
    sqlSelectStatement.setSelect(sqlSelect);

    return sqlSelectStatement;
  }



  public List<SQLSelectItem> makeSelectList(Scope scope, Basepb.DataType[] dataTypes, Context context, Table table) {
    List<SQLSelectItem> selectItems = new ArrayList<>();
    if (dataTypes == null) {
      dataTypes = Scalar.makeDesiredTypes();
    }

    for (Basepb.DataType dataType : dataTypes) {
      SQLSelectItem item = new SQLSelectItem();
      SQLExpr sqlExpr = scalar.makeScalarContext(scope, new Context(), dataType, table.getColumns());
      item.setExpr(sqlExpr);
      item.setAlias(scope.getSmither().getName("col"));
      selectItems.add(item);
    }
    return selectItems;
  }


}




