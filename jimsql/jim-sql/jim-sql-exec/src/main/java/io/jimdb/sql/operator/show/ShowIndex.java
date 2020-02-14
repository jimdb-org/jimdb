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
import java.util.List;

import io.jimdb.core.Session;
import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.exception.JimException;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.RowValueAccessor;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.model.meta.Column;
import io.jimdb.core.model.meta.Index;
import io.jimdb.core.model.meta.Table;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.model.result.impl.QueryExecResult;
import io.jimdb.sql.operator.RelOperator;
import io.jimdb.common.utils.lang.StringUtil;
import io.jimdb.core.values.LongValue;
import io.jimdb.core.values.NullValue;
import io.jimdb.core.values.StringValue;
import io.jimdb.core.values.Value;

import reactor.core.publisher.Flux;

/**
 * @version V1.0
 */
public final class ShowIndex extends RelOperator {
  private static final String DEFAULT_COLLATION = "A";
  private static final String DEFAULT_INDEX_TYPE = "BTREE";
  private static final Integer CARDINALITY = 0;
  private static final Value PACKED = NullValue.getInstance();
  private static final String COMMENT = "";

  private final String database;
  private final String table;

  public ShowIndex(String database, String table) {
    this.database = database;
    this.table = table;
  }

  @Override
  public Flux<ExecResult> execute(Session session) throws JimException {
    if (StringUtil.isBlank(database) || StringUtil.isBlank(table)) {
      throw DBException.get(ErrorModule.EXECUTOR, ErrorCode.ER_NO_SUCH_TABLE, database, table);
    }

    String colIfNull = "YES";
    String indexComment;
    Table tableInfo = session.getTxnContext().getMetaData().getTable(database, table);
    List<ValueAccessor> rows = new ArrayList<>();
    if (tableInfo != null) {
      Index[] indices = tableInfo.getReadableIndices();
      for (Index index : indices) {
        int nonUnique = 1;
        indexComment = index.getIndexComment();
        if (index.isPrimary()) {
          nonUnique = 0;
          colIfNull = "";
          indexComment = "";
        }
        if (index.isUnique()) {
          nonUnique = 0;
        }
        Column[] indexColumns = index.getColumns();
        for (int i = 0; i < indexColumns.length; i++) {
          Value[] values = new Value[]{
                  StringValue.getInstance(tableInfo.getName()),
                  LongValue.getInstance(nonUnique),
                  StringValue.getInstance(index.getName()),
                  LongValue.getInstance(i + 1),
                  StringValue.getInstance(indexColumns[i].getName()),
                  StringValue.getInstance(DEFAULT_COLLATION),
                  LongValue.getInstance(CARDINALITY),
                  NullValue.getInstance(),
                  PACKED,
                  StringValue.getInstance(colIfNull),
                  StringValue.getInstance(DEFAULT_INDEX_TYPE),
                  StringValue.getInstance(COMMENT),
                  StringValue.getInstance(indexComment)
          };
          RowValueAccessor row = new RowValueAccessor(values);
          rows.add(row);
        }
      }
    }

    return Flux.just(new QueryExecResult(schema.getColumns().toArray(new ColumnExpr[0]), rows.toArray(new ValueAccessor[0])));
  }
}
