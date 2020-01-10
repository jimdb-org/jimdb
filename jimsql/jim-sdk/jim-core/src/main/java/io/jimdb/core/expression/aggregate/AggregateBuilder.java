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
package io.jimdb.core.expression.aggregate;

import io.jimdb.core.Session;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.types.Types;
import io.jimdb.pb.Basepb;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings({ "UP_UNUSED_PARAMETER", "CLI_CONSTANT_LIST_INDEX", "CVAA_CONTRAVARIANT_ELEMENT_ASSIGNMENT" })
public class AggregateBuilder {

  /**
   * build: construct partialExpr and finalExpr by aggregateExpr
   *
   * @param session
   * @param aggregateExpr
   * @param ordinal
   * @return
   */
  public static AggregateExpr[] build(Session session, AggregateExpr aggregateExpr, int[] ordinal) {
    AggregateExpr partialExpr = new AggregateExpr(session, aggregateExpr.getAggType(), aggregateExpr.getArgs(), aggregateExpr.isHasDistinct());
    partialExpr.setType(aggregateExpr.getType());
    if (aggregateExpr.getAggFunctionMode() == AggregateExpr.AggFunctionMode.COMPLETE_MODE) {
      partialExpr.setAggFunctionMode(AggregateExpr.AggFunctionMode.PARTIAL1_MODE);
    } else if (aggregateExpr.getAggFunctionMode() == AggregateExpr.AggFunctionMode.FINAL_MODE) {
      partialExpr.setAggFunctionMode(AggregateExpr.AggFunctionMode.PARTIAL2_MODE);
    }

    AggregateExpr finalExpr = new AggregateExpr(session, aggregateExpr.getAggType(), aggregateExpr.getArgs(), aggregateExpr.isHasDistinct());
    finalExpr.setAggFunctionMode(AggregateExpr.AggFunctionMode.FINAL_MODE);
    finalExpr.setType(aggregateExpr.getType());

    Expression[] args;
    if (AggregateType.AVG == aggregateExpr.getAggType()) {
      // columnExpr1: sum
      ColumnExpr columnExpr1 = new ColumnExpr(session.allocColumnID());
      columnExpr1.setOriCol("avg_final_col_" + ordinal[0]);
      columnExpr1.setOffset(ordinal[0]);
      columnExpr1.setResultType(aggregateExpr.getType());

      // columnExpr2: count
      ColumnExpr columnExpr2 = new ColumnExpr(session.allocColumnID());
      columnExpr2.setOriCol("avg_final_col_" + ordinal[1]);
      columnExpr2.setOffset(ordinal[1]);
      columnExpr2.setResultType(Types.buildSQLType(Basepb.DataType.BigInt));
      args = new ColumnExpr[]{ columnExpr1, columnExpr2 };
    } else {
      ColumnExpr columnExpr = new ColumnExpr(session.allocColumnID());
      columnExpr.setOriCol(aggregateExpr.getName() + "_final_col_" + ordinal[0]);
      columnExpr.setOffset(ordinal[0]);
      columnExpr.setResultType(aggregateExpr.getType());
      args = new ColumnExpr[]{ columnExpr };
    }
    finalExpr.setArgs(args);

    return new AggregateExpr[]{ partialExpr, finalExpr };
  }

  public static AggregateFunc build(AggregateExpr aggregateExpr, int ordinal) {
    switch (aggregateExpr.getAggType()) {
      case COUNT:
        return buildCount(aggregateExpr, ordinal);
      case DISTINCT:
        return buildDistinctValue(aggregateExpr, ordinal);
      case SUM:
        return buildSum(aggregateExpr, ordinal);
      case AVG:
        return buildAvg(aggregateExpr, ordinal);
      case MAX:
        return buildMax(aggregateExpr, ordinal);
      case MIN:
        return buildMin(aggregateExpr, ordinal);
      case BIT_OR:
        return buildBitOr(aggregateExpr, ordinal);
      case BIT_XOR:
        return buildBitXor(aggregateExpr, ordinal);
      case BIT_AND:
        return buildBitAnd(aggregateExpr, ordinal);
    }
    return null;
  }

  private static AggregateFunc buildCount(AggregateExpr aggregateExpr, int ordinal) {
    if (aggregateExpr.isHasDistinct()) {
      switch (aggregateExpr.getAggFunctionMode()) {
        case COMPLETE_MODE:
        case PARTIAL1_MODE:
        case PARTIAL2_MODE:
          return new AggregateCount(aggregateExpr, ordinal, true);
        case FINAL_MODE:
          return new AggregateCount(aggregateExpr, ordinal, false);
        case DEDUP_MODE:
        default:
          return null;
      }
    } else {
      switch (aggregateExpr.getAggFunctionMode()) {
        case COMPLETE_MODE:
        case PARTIAL1_MODE:
          return new AggregateCount(aggregateExpr, ordinal, true);
        case PARTIAL2_MODE:
        case FINAL_MODE:
          return new AggregateCount(aggregateExpr, ordinal, false);
        case DEDUP_MODE:
        default:
          return null;
      }
    }
  }

  private static AggregateFunc buildDistinctValue(AggregateExpr aggregateExpr, int ordinal) {
    return new AggregateDistinctValue(aggregateExpr, ordinal, true); // Now we only have the push-down case
  }

  private static AggregateFunc buildSum(AggregateExpr aggregateExpr, int ordinal) {
    if (aggregateExpr.getAggFunctionMode() == AggregateExpr.AggFunctionMode.DEDUP_MODE) {
      return null;
    }
    if (aggregateExpr.isHasDistinct()) {
      switch (aggregateExpr.getAggFunctionMode()) {
        case COMPLETE_MODE:
        case PARTIAL1_MODE:
        case PARTIAL2_MODE:
          return new AggregateSum(aggregateExpr, ordinal, true);
        case FINAL_MODE:
          return new AggregateSum(aggregateExpr, ordinal, false);
        case DEDUP_MODE:
        default:
          return null;
      }
    } else {
      return new AggregateSum(aggregateExpr, ordinal, false);
    }
  }

  private static AggregateFunc buildAvg(AggregateExpr aggregateExpr, int ordinal) {
    if (aggregateExpr.isHasDistinct()) {
      switch (aggregateExpr.getAggFunctionMode()) {
        case COMPLETE_MODE:
        case PARTIAL1_MODE:
        case PARTIAL2_MODE:
          return new AggregateAvg(aggregateExpr, ordinal, true);
        case FINAL_MODE:
          return new AggregateAvg(aggregateExpr, ordinal, false);
        case DEDUP_MODE:
        default:
          return null;
      }
    } else {
      switch (aggregateExpr.getAggFunctionMode()) {
        case COMPLETE_MODE:
        case PARTIAL1_MODE:
          return new AggregateAvg(aggregateExpr, ordinal, true);
        case PARTIAL2_MODE:
        case FINAL_MODE:
          return new AggregateAvg(aggregateExpr, ordinal, false);
        case DEDUP_MODE:
        default:
          return null;
      }
    }
  }

  private static AggregateFunc buildMax(AggregateExpr aggregateExpr, int ordinal) {
    return buildMaxMin(aggregateExpr, ordinal, true);
  }

  private static AggregateFunc buildMin(AggregateExpr aggregateExpr, int ordinal) {
    return buildMaxMin(aggregateExpr, ordinal, false);
  }

  private static AggregateFunc buildMaxMin(AggregateExpr aggregateExpr, int ordinal, boolean isMax) {
    return new AggregateMaxMin(aggregateExpr, ordinal, isMax);
  }

  private static AggregateFunc buildBitOr(AggregateExpr aggregateExpr, int ordinal) {
    return null;
  }

  private static AggregateFunc buildBitXor(AggregateExpr aggregateExpr, int ordinal) {
    return null;
  }

  private static AggregateFunc buildBitAnd(AggregateExpr aggregateExpr, int ordinal) {
    return null;
  }
}



