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

package io.jimdb.sql.optimizer;

import io.jimdb.core.Session;
import io.jimdb.sql.operator.Aggregation;
import io.jimdb.sql.operator.DualTable;
import io.jimdb.sql.operator.IndexSource;
import io.jimdb.sql.operator.Limit;
import io.jimdb.sql.operator.Order;
import io.jimdb.sql.operator.Projection;
import io.jimdb.sql.operator.RelOperator;
import io.jimdb.sql.operator.Selection;
import io.jimdb.sql.operator.TableSource;
import io.jimdb.sql.operator.TopN;

/**
 * Base operator visitor with parameter that will be used during visiting
 *
 * @param <P> the given parameter used during visiting
 * @param <R> return type
 */
public abstract class ParameterizedOperatorVisitor<P, R> {
  public R visitOperator(Session session, Aggregation operator, P parameter) {
    return visitOperatorByDefault(session, operator, parameter);
  }

  public R visitOperator(Session session, Projection operator, P parameter) {
    return visitOperatorByDefault(session, operator, parameter);
  }

  public R visitOperator(Session session, Limit operator, P parameter) {
    return visitOperatorByDefault(session, operator, parameter);
  }

  public R visitOperator(Session session, Order operator, P parameter) {
    return visitOperatorByDefault(session, operator, parameter);
  }

  public R visitOperator(Session session, Selection operator, P parameter) {
    return visitOperatorByDefault(session, operator, parameter);
  }

  public R visitOperator(Session session, TableSource operator, P parameter) {
    return visitOperatorByDefault(session, operator, parameter);
  }

  public R visitOperator(Session session, DualTable operator, P parameter) {
    return visitOperatorByDefault(session, operator, parameter);
  }

  // Physical operator only
  public R visitOperator(Session session, IndexSource operator, P parameter) {
    return visitOperatorByDefault(session, operator, parameter);
  }

  // Physical operator only
  public R visitOperator(Session session, TopN operator, P parameter) {
    return visitOperatorByDefault(session, operator, parameter);
  }

  public R visitOperator(Session session, RelOperator operator, P parameter) {
    return visitOperatorByDefault(session, operator, parameter);
  }

  public abstract R visitOperatorByDefault(Session session, RelOperator operator, P parameter);
}
