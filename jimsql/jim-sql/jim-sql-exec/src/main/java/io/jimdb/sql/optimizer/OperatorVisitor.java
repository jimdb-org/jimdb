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

package io.jimdb.sql.optimizer;

import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.sql.operator.Aggregation;
import io.jimdb.sql.operator.Delete;
import io.jimdb.sql.operator.DualTable;
import io.jimdb.sql.operator.IndexLookup;
import io.jimdb.sql.operator.IndexSource;
import io.jimdb.sql.operator.Insert;
import io.jimdb.sql.operator.Limit;
import io.jimdb.sql.operator.Operator;
import io.jimdb.sql.operator.Order;
import io.jimdb.sql.operator.Projection;
import io.jimdb.sql.operator.RelOperator;
import io.jimdb.sql.operator.Selection;
import io.jimdb.sql.operator.TableSource;
import io.jimdb.sql.operator.TopN;
import io.jimdb.sql.operator.Update;

/**
 * Base operator visitor
 *
 * @param <R> return type
 */
public abstract class OperatorVisitor<R> {
  public R visitOperator(Aggregation operator) {
    return visitOperatorByDefault(operator);
  }

  public R visitOperator(Projection operator) {
    return visitOperatorByDefault(operator);
  }

  public R visitOperator(Limit operator) {
    return visitOperatorByDefault(operator);
  }

  public R visitOperator(Order operator) {
    return visitOperatorByDefault(operator);
  }

  public R visitOperator(Selection operator) {
    return visitOperatorByDefault(operator);
  }

  public R visitOperator(TableSource operator) {
    return visitOperatorByDefault(operator);
  }

  public R visitOperator(DualTable operator) {
    return visitOperatorByDefault(operator);
  }

  // Physical operator only
  public R visitOperator(TopN operator) {
    return visitOperatorByDefault(operator);
  }

  // Physical operator only
  public R visitOperator(IndexSource operator) {
    return visitOperatorByDefault(operator);
  }

  public R visitOperator(RelOperator operator) {
    return visitOperatorByDefault(operator);
  }

  public R visitOperator(IndexLookup operator) {
    throw DBException.get(ErrorModule.EXECUTOR, ErrorCode.ER_UNKNOWN_ERROR);
  }

  public R visitOperator(Update operator) {
    throw DBException.get(ErrorModule.EXECUTOR, ErrorCode.ER_UNKNOWN_ERROR);
  }

  public R visitOperator(Insert operator) {
    throw DBException.get(ErrorModule.EXECUTOR, ErrorCode.ER_UNKNOWN_ERROR);
  }

  public R visitOperator(Delete operator) {
    throw DBException.get(ErrorModule.EXECUTOR, ErrorCode.ER_UNKNOWN_ERROR);
  }

  public R visitOperator(Operator operator) {
    throw DBException.get(ErrorModule.EXECUTOR, ErrorCode.ER_UNKNOWN_ERROR);
  }

  public abstract R visitOperatorByDefault(RelOperator operator);

}
