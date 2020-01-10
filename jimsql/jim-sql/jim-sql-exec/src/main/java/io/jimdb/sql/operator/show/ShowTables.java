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
package io.jimdb.sql.operator.show;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import io.jimdb.core.Session;
import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.exception.JimException;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.RowValueAccessor;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.model.meta.Table;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.model.result.impl.QueryExecResult;
import io.jimdb.sql.operator.RelOperator;
import io.jimdb.common.utils.lang.StringUtil;
import io.jimdb.core.values.StringValue;
import io.jimdb.core.values.Value;

import reactor.core.publisher.Flux;

/**
 * @version V1.0
 */
public final class ShowTables extends RelOperator {
  private final String database;

  public ShowTables(String database) {
    this.database = database;
  }

  @Override
  public Flux<ExecResult> execute(Session session) throws JimException {
    if (StringUtil.isBlank(database)) {
      throw DBException.get(ErrorModule.EXECUTOR, ErrorCode.ER_NO_DB_ERROR);
    }

    Table[] tables = session.getTxnContext().getMetaData().getCatalog(database).getTables();
    Arrays.sort(tables, (t1, t2) -> {
      if (t1.getId().equals(t2.getId())) {
        return 0;
      }
      return t1.getId() > t2.getId() ? 1 : -1;
    });

    List<ValueAccessor> rows = new ArrayList<>(tables.length);
    for (Table table : tables) {
      Value[] values = new Value[]{ StringValue.getInstance(table.getName()) };
      RowValueAccessor row = new RowValueAccessor(values);
      rows.add(row);
    }
    return Flux.just(new QueryExecResult(schema.getColumns().toArray(new ColumnExpr[0]), rows.toArray(new ValueAccessor[0])));
  }
}
