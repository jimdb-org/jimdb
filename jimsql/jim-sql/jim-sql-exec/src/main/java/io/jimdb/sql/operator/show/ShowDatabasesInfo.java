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
import java.util.Collection;
import java.util.List;

import io.jimdb.common.exception.BaseException;
import io.jimdb.core.Session;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.RowValueAccessor;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.model.meta.Catalog;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.model.result.impl.QueryExecResult;
import io.jimdb.core.values.LongValue;
import io.jimdb.core.values.StringValue;
import io.jimdb.core.values.Value;
import io.jimdb.sql.operator.RelOperator;

import reactor.core.publisher.Flux;

/**
 * @version V1.0
 */
public class ShowDatabasesInfo extends RelOperator {
  @Override
  public Flux<ExecResult> execute(Session session) throws BaseException {
    Collection<Catalog> dbs = (Collection<Catalog>) session.getTxnContext().getMetaData().getAllCatalogs();
    dbs.stream().sorted();
    List<RowValueAccessor> rows = new ArrayList<>(dbs.size());
    for (Catalog catalog : dbs) {
      Value[] values = new Value[]{
              LongValue.getInstance(catalog.getId()),
              StringValue.getInstance(catalog.getName()) };
      RowValueAccessor row = new RowValueAccessor(values);
      rows.add(row);
    }
    return Flux.just(new QueryExecResult(schema.getColumns().toArray(new ColumnExpr[0]),
            rows.toArray(new ValueAccessor[0])));
  }
}

