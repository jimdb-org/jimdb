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
package io.jimdb.sql.smith.agg;

import io.jimdb.expression.aggregate.AggregateType;
import io.jimdb.model.Column;
import io.jimdb.pb.Basepb;
import io.jimdb.sql.smith.cxt.Scope;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings({ "IL_INFINITE_RECURSIVE_LOOP" })
public class CountFunction extends AggregateFunction {
  @Override
  public SQLExpr makeSQLExpr(Scope scope, Basepb.DataType dataType, Column[] columns) {
    SQLAggregateExpr aggExpr = new SQLAggregateExpr(AggregateType.COUNT.getName());
    aggExpr.addArgument(makeSQLExpr(scope, dataType, columns));
    return aggExpr;
  }
}
