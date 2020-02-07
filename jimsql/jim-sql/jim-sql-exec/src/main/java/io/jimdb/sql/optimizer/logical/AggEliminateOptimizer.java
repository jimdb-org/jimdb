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
package io.jimdb.sql.optimizer.logical;

import java.util.ArrayList;
import java.util.List;

import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.core.Session;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.FuncExpr;
import io.jimdb.core.expression.KeyColumn;
import io.jimdb.core.expression.Schema;
import io.jimdb.core.expression.ValueExpr;
import io.jimdb.core.expression.aggregate.AggregateExpr;
import io.jimdb.core.expression.aggregate.AggregateType;
import io.jimdb.core.expression.functions.FuncType;
import io.jimdb.core.expression.functions.builtin.cast.CastFuncBuilder;
import io.jimdb.core.types.Types;
import io.jimdb.core.values.LongValue;
import io.jimdb.pb.Basepb.DataType;
import io.jimdb.pb.Metapb.SQLType;
import io.jimdb.sql.operator.Aggregation;
import io.jimdb.sql.operator.Projection;
import io.jimdb.sql.operator.RelOperator;
import io.jimdb.sql.optimizer.physical.NFDetacher;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * AggEliminateOptimizer
 */
@SuppressFBWarnings({ "CFS_CONFUSING_FUNCTION_SEMANTICS" })
public class AggEliminateOptimizer implements IRuleOptimizer {

  @Override
  public RelOperator optimize(RelOperator relOperator, Session session) {
    return AggEliminator.eliminateAgg(session, relOperator);
  }

  /**
   * aggregation eliminator
   */
  public static class AggEliminator {

    public static RelOperator eliminateAgg(Session session, RelOperator relOperator) {
      if (relOperator.hasChildren()) {
        RelOperator[] children = relOperator.getChildren();
        RelOperator[] newChildren = new RelOperator[children.length];
        for (int i = 0; i < children.length; i++) {
          RelOperator newChild = eliminateAgg(session, children[i]);
          newChildren[i] = newChild;
        }

        relOperator.setChildren(newChildren);
      }

      if (!(relOperator instanceof Aggregation)) {
        return relOperator;
      }

      Projection projection = tryToEliminateAgg(session, (Aggregation) relOperator);
      if (projection != null) {
        return projection;
      }
      return relOperator;
    }

    public static Projection tryToEliminateAgg(Session session, Aggregation aggregation) {
      for (AggregateExpr aggregateExpr : aggregation.getAggregateExprs()) {
        if (aggregateExpr.getName().equalsIgnoreCase(AggregateType.GROUP_CONTACT.getName())) {
          return null;
        }
      }
      //clone arr : prevent the ColumnsExprs's offset modified.
      ColumnExpr[] groupByColumnsExprsArr = aggregation.getGroupByColumnExprs();
      List<ColumnExpr> groupByColumnsExprsList = new ArrayList<>(groupByColumnsExprsArr.length);
      for (int i = 0; i < groupByColumnsExprsArr.length; i++) {
        groupByColumnsExprsList.add(groupByColumnsExprsArr[i].clone());
      }
      Schema groupBySchema = new Schema(groupByColumnsExprsList);
      boolean hasUniqueKey = false;
      for (KeyColumn keyColumn : aggregation.getChildren()[0].getSchema().getKeyColumns()) {
        List<ColumnExpr> columnExprs = keyColumn.getColumnExprs();
        if (columnExprs == null || columnExprs.isEmpty()) {
          continue;
        }
        List<Integer> posList = groupBySchema.indexPosInSchema(columnExprs);
        if (posList != null) {
          hasUniqueKey = true;
          break;
        }
      }

      if (hasUniqueKey) {
        return convertAggToProjection(session, aggregation);
      }

      return null;
    }

    private static Projection convertAggToProjection(Session session, Aggregation aggregation) {

      AggregateExpr[] aggregateExprs = aggregation.getAggregateExprs();
      Expression[] expressions = new Expression[aggregateExprs.length];
      for (int i = 0; i < aggregateExprs.length; i++) {
        AggregateExpr aggregateExpr = aggregateExprs[i];
        expressions[i] = rewriteExpr(session, aggregateExpr);
      }

      return new Projection(expressions, aggregation.getSchema().clone(), aggregation.getChildren()[0]);
    }

    private static Expression rewriteExpr(Session session, AggregateExpr aggregateExpr) {
      AggregateType aggType = AggregateType.valueOf(aggregateExpr.getName().toUpperCase());
      switch (aggType) {
        case COUNT:
          if (aggregateExpr.getAggFunctionMode() == AggregateExpr.AggFunctionMode.FINAL_MODE) {
            return wrapCastFunc(session, aggregateExpr.getArgs()[0], aggregateExpr.getType());
          }
          return rewriteCountFunc(session, aggregateExpr.getArgs(), aggregateExpr.getType());
        case SUM:
        case AVG:
        case DISTINCT:
        case MAX:
        case MIN:
        case GROUP_CONTACT:
          return wrapCastFunc(session, aggregateExpr.getArgs()[0], aggregateExpr.getType());
        case BIT_AND:
        case BIT_OR:
        case BIT_XOR:
          return rewriteBitFunc(session, aggType, aggregateExpr.getArgs()[0], aggregateExpr.getType());
        default:
          throw DBException.get(ErrorModule.PLANNER, ErrorCode.ER_NOT_SUPPORTED_YET, aggType.getName());
      }
    }

    private static Expression rewriteCountFunc(Session session, Expression[] expressions, SQLType targetSqlType) {
      List<Expression> isNullExprs = new ArrayList<>(expressions.length);
      for (int i = 0; i < expressions.length; i++) {
        Expression expression = expressions[i];
        if (expression.getResultType().getNotNull()) {
          isNullExprs.add(ValueExpr.NULL);
        } else {
          isNullExprs.add(FuncExpr.build(session, FuncType.ISNULL, Types.buildSQLType(DataType.TinyInt), true,
                  expression));
        }
      }

      Expression innerExpr = NFDetacher.rebuildDNFCondition(session, isNullExprs);
      // TODO: change to 'IF'
//      return FuncExpr.build(session, FuncType.IF, targetSqlType, true, innerExpr, ValueExpr.ZERO, ValueExpr.ONE);
      return FuncExpr.build(session, FuncType.ISNULL, targetSqlType, true, innerExpr);
    }

    private static Expression rewriteBitFunc(Session session, AggregateType aggregateType, Expression arg,
                                             SQLType targetSqlType) {
      Expression innerExpr;
      if (arg.getResultType().getType() == DataType.Int) {
        innerExpr = arg;
      } else {
        SQLType.Builder builder = SQLType.newBuilder();
        builder.setType(DataType.BigInt)
                .setScale(arg.getResultType().getScale())
                .setPrecision(0);
        innerExpr = CastFuncBuilder.build(session, builder.build(), arg);
      }

      Expression outerExpr = wrapCastFunc(session, innerExpr, targetSqlType);
      Expression resultExpr;
      if (aggregateType == AggregateType.BIT_AND) {
        // TODO change to `IFNULL`
        resultExpr = FuncExpr.build(session, FuncType.ISNULL, targetSqlType, true, ValueExpr.ZERO.clone());
      } else {
        resultExpr = FuncExpr.build(session, FuncType.ISNULL, outerExpr.getResultType(), true,
                outerExpr, new ValueExpr(LongValue.getInstance(Long.MAX_VALUE), targetSqlType));
      }

      return resultExpr;
    }

    private static Expression wrapCastFunc(Session session, Expression arg, SQLType targetSqlType) {
      if (arg.getResultType() == targetSqlType) {
        return arg;
      }

      return CastFuncBuilder.build(session, targetSqlType, arg);
    }
  }
}
