/*
 * Copyright 2019 The JimDB Authors.
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

import io.jimdb.core.Session;
import io.jimdb.common.exception.JimException;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.RowValueAccessor;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.model.result.impl.QueryExecResult;
import io.jimdb.sql.operator.RelOperator;
import io.jimdb.core.values.LongValue;
import io.jimdb.core.values.StringValue;
import io.jimdb.core.values.Value;

import reactor.core.publisher.Flux;

/**
 * @version V1.0
 */
public final class ShowCollation extends RelOperator {

  @Override
  public Flux<ExecResult> execute(Session session) throws JimException {
    Value charset = StringValue.getInstance("utf8");
    Value len = LongValue.getInstance(1L);
    Value value = StringValue.getInstance("Yes");
    final ValueAccessor[] values = new ValueAccessor[]{
            new RowValueAccessor(new Value[]{
                    charset,
                    charset,
                    len,
                    value,
                    value,
                    len
            }) };
    return Flux.just(new QueryExecResult(this.schema.getColumns().toArray(new ColumnExpr[0]), values));
  }
}
