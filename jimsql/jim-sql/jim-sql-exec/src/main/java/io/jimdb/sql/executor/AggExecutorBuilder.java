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
package io.jimdb.sql.executor;

import io.jimdb.core.Session;
import io.jimdb.core.expression.aggregate.AggregateBuilder;
import io.jimdb.core.expression.aggregate.AggregateExpr;
import io.jimdb.core.expression.aggregate.AggregateFunc;
import io.jimdb.core.expression.aggregate.AggregateType;
import io.jimdb.sql.operator.Aggregation;
import io.jimdb.sql.operator.Operator;
import io.jimdb.sql.operator.RelOperator;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings()
public class AggExecutorBuilder {
  public static AggExecutor build(Session session, RelOperator operator) {
    Operator.OperatorType type = operator.getOperatorType();
    switch (type) {
      case LOGIC:
        if (operator instanceof Aggregation) {
          Aggregation aggregation = (Aggregation) operator;
          switch (aggregation.getAggOpType()) {
            case HashAgg:
              return buildHashAgg(session, aggregation);
            case StreamAgg:
              return buildStreamAgg(session, aggregation);
          }
        }
        break;
      default:
        //todo
    }
    return null;
  }

  public AggExecutorBuilder() {
  }

  private static HashAggExecutor buildHashAgg(Session session, Aggregation aggregation) {
    AggregateFunc[] partialAggFuncs = new AggregateFunc[aggregation.getAggregateExprs().length];
    AggregateFunc[] finalAggFuncs = new AggregateFunc[aggregation.getAggregateExprs().length];
    AggregateExpr[] aggregateExprs = aggregation.getAggregateExprs();
    int index = 0;
    boolean isSerialExecute = false;
    int[] ordinal;
    for (int i = 0; i < aggregateExprs.length; i++) {
      if (aggregateExprs[i].isHasDistinct()) {
        isSerialExecute = true;
      }
      ordinal = new int[2];
      ordinal[0] = index++;
      if (AggregateType.AVG == aggregateExprs[i].getAggType()) {
        ordinal[1] = ++index;
      }
      AggregateExpr[] exprs = AggregateBuilder.build(session, aggregateExprs[i], ordinal);
      partialAggFuncs[i] = AggregateBuilder.build(exprs[0], i);
      finalAggFuncs[i] = AggregateBuilder.build(exprs[1], i);
    }

    return new HashAggExecutor(aggregation.getSchema(), partialAggFuncs, finalAggFuncs,
            aggregation.getGroupByExprs(), isSerialExecute);
  }

  private static StreamAggExecutor buildStreamAgg(Session session, Aggregation aggregation) {
    AggregateFunc[] aggFuncs = new AggregateFunc[aggregation.getAggregateExprs().length];
    AggregateExpr[] aggregateExprs = aggregation.getAggregateExprs();
    for (int i = 0; i < aggregateExprs.length; i++) {
      aggFuncs[i] = AggregateBuilder.build(aggregateExprs[i], i);
    }
    return new StreamAggExecutor(aggregation.getSchema(), aggFuncs, aggregation.getGroupByExprs());
  }
}
