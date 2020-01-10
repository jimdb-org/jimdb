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

package io.jimdb.sql.optimizer.physical;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.jimdb.core.Session;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.Schema;
import io.jimdb.core.expression.aggregate.AggregateExpr;
import io.jimdb.core.expression.aggregate.AggregateType;
import io.jimdb.pb.Basepb;
import io.jimdb.pb.Metapb;
import io.jimdb.sql.operator.Aggregation;
import io.jimdb.sql.operator.Limit;
import io.jimdb.sql.operator.Order;
import io.jimdb.sql.operator.Projection;
import io.jimdb.sql.operator.RelOperator;
import io.jimdb.sql.operator.Selection;
import io.jimdb.sql.operator.TopN;
import io.jimdb.sql.optimizer.OptimizationUtil;
import io.jimdb.sql.optimizer.ParameterizedOperatorVisitor;
import io.jimdb.sql.optimizer.statistics.StatsUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Attach operator to different tasks
 */
@SuppressFBWarnings()
@VisibleForTesting
public class OperatorToTaskAttacher extends ParameterizedOperatorVisitor<Task[], Task> {

  @Override
  public Task visitOperatorByDefault(Session session, RelOperator operator, Task[] tasks) {
    Task task = tasks[0].finish();
    task.attachOperator(operator);
    return task;
  }

  @Override
  public Task visitOperator(Session session, Aggregation operator, Task[] tasks) {
    Task task = tasks[0];
    if (task.getTaskType() == Task.TaskType.DS) {
      DSTask dsTask = (DSTask) task;
      RelOperator[] aggs = newPartialAggregate(session, operator);
      RelOperator partialAgg = aggs[0];
      RelOperator finalAgg = aggs[1];
      if (partialAgg != null) {
        RelOperator tablePlan = dsTask.getPushedDownTablePlan();
        if (tablePlan != null) {
          dsTask.finishIndexPlan();
          dsTask.attachOperatorToPushedDownTablePlan(partialAgg);
        } else {
          dsTask.attachOperatorToPushedDownIndexPlan(partialAgg);
        }
      }
      task = dsTask.finish();
      task.attachOperator(finalAgg);
    } else {
      task.attachOperator(operator);
    }

    // Add cost to task
    double costWithCpu;
    if (operator.getAggOpType() == Aggregation.AggOpType.HashAgg) {
      costWithCpu = task.getStatRowCount() * StatsUtils.CPU_FACTOR * StatsUtils.AGGREGATION_HASH_FACTOR
              + operator.getStatInfo().getEstimatedRowCount() * StatsUtils.AGGREGATION_HASH_CONTEXT_FACTOR;
    } else {
      costWithCpu = task.getStatRowCount() * StatsUtils.CPU_FACTOR;
    }
    task.cost.plus(costWithCpu);
    if (operator.hasDistinct()) {
      double costWithAggDistinct = costWithCpu * StatsUtils.AGGREGATION_DISTINCT_FACTOR;
      task.cost.plus(costWithAggDistinct);
    }
    return task;
  }

  private RelOperator[] newPartialAggregate(Session session, Aggregation aggregation) {
    AggregateExpr[] aggregateExprs = aggregation.getAggregateExprs();

    boolean hasDistinct = aggregation.hasDistinct();

    if (hasDistinct && aggregateExprs.length > 1) {
      return new RelOperator[]{ null, aggregation };
    }

    Schema finalSchema = aggregation.getSchema();
    List<ColumnExpr> partialSchemaColumns = new ArrayList<>();

    int idx = 0;
    List<AggregateExpr> partialAggExprs = new ArrayList<>();
    AggregateExpr[] finalAggExprs = new AggregateExpr[aggregateExprs.length];
    AggregateExpr aggExpr = null;
    AggregateExpr partialAggExpr;
    AggregateExpr finalAggFunc;
    Expression[] args;
    ColumnExpr columnExpr;
    for (int i = 0; i < aggregateExprs.length; i++) {
      aggExpr = aggregateExprs[i];
      args = new Expression[aggExpr.getAggType() == AggregateType.AVG ? 2 : 1];

      if (isInitAggExpr(aggExpr.getAggType())) {
        columnExpr = new ColumnExpr(session.allocColumnID());
        columnExpr.setAliasCol("col_" + idx++);
        columnExpr.setResultType(aggExpr.getArgs()[0].getResultType());
        partialSchemaColumns.add(columnExpr);
        args[0] = columnExpr;
        if (aggExpr.isHasDistinct()) {
          partialAggExpr = new AggregateExpr(session, AggregateType.DISTINCT, aggExpr.getArgs(), true);
        } else {
          partialAggExpr = new AggregateExpr(session, aggExpr.getAggType() == AggregateType.AVG ? AggregateType.SUM : aggExpr.getAggType(),
                  aggExpr.getArgs(), false);
        }
        partialAggExprs.add(partialAggExpr);
      }

      if (isInitAggCountExpr(aggExpr.getAggType())) {
        columnExpr = new ColumnExpr(session.allocColumnID());
        columnExpr.setAliasCol("col_" + idx++);
        partialSchemaColumns.add(columnExpr);
        args[args.length - 1] = columnExpr;
        if (aggExpr.isHasDistinct()) {
          partialAggExpr = new AggregateExpr(session, AggregateType.DISTINCT, aggExpr.getArgs(), true);
          columnExpr.setResultType(aggExpr.getArgs()[0].getResultType());
        } else {
          partialAggExpr = new AggregateExpr(session, AggregateType.COUNT, aggExpr.getArgs(), false);
          columnExpr.setResultType(Metapb.SQLType.newBuilder().setType(Basepb.DataType.BigInt).build());
        }
        partialAggExprs.add(partialAggExpr);
      }

      finalAggFunc = new AggregateExpr(session, aggExpr.getAggType(), args, aggExpr.isHasDistinct());
      finalAggFunc.setAggFunctionMode(AggregateExpr.AggFunctionMode.FINAL_MODE);
      finalAggFunc.setType(aggExpr.getType());
      finalAggExprs[i] = finalAggFunc;
    }

    int groupByItemsLength = aggregation.getGroupByExprs().length;
    Expression[] groupByItems = new Expression[groupByItemsLength];
    ColumnExpr gbyExpr;
    int i = 0;
    for (; i < groupByItemsLength; i++) {
      gbyExpr = new ColumnExpr(session.allocColumnID());
      gbyExpr.setAliasCol("col_" + (idx + i));
      gbyExpr.setResultType(aggregation.getGroupByExprs()[i].getResultType());
      partialSchemaColumns.add(gbyExpr);
      groupByItems[i] = gbyExpr;
    }

    if (hasDistinct) {
      aggregation.setGroupByExprs(groupByItems);
      Expression[] partialGroupByExprs = new Expression[groupByItemsLength + 1];
      System.arraycopy(aggregation.getGroupByExprs(), 0, partialGroupByExprs, 0, groupByItemsLength);
      partialGroupByExprs[groupByItemsLength] = aggExpr.getArgs()[0];
      aggregation.setGroupByExprs(partialGroupByExprs);
    }

    aggregation.setSchema(new Schema(partialSchemaColumns));

//    removeUnnecessaryDistinctValue(aggregation, finalAggExprs, groupByItems);

    Aggregation finalAgg = new Aggregation(finalAggExprs, groupByItems, finalSchema,
            aggregation.getAggOpType(), aggregation.getStatInfo());
    Aggregation partialAgg = new Aggregation(aggregation);
    partialAgg.setAggregateExprs(partialAggExprs.toArray(new AggregateExpr[partialAggExprs.size()]));
    return new RelOperator[]{ partialAgg, finalAgg };
  }

  private boolean isInitAggCountExpr(AggregateType type) {
    return type == AggregateType.COUNT || type == AggregateType.AVG;
  }

  private boolean isInitAggExpr(AggregateType type) {
    switch (type) {
      case SUM:
      case AVG:
      case MAX:
      case MIN:
      case GROUP_CONTACT:
      case BIT_OR:
      case BIT_AND:
      case BIT_XOR:
      case DISTINCT:
        return true;
      default:
        return false;
    }
  }

  // todo removeUnnecessaryDistinctValue
  private void removeUnnecessaryDistinctValue(Aggregation aggregation, AggregateExpr[] finalAggExprs,
                                              Expression[] groupByItems) {
  }

  @Override
  public Task visitOperator(Session session, Projection operator, Task[] tasks) {
    Task task = tasks[0];
    if (task.getTaskType() == Task.TaskType.DS) {
      task = task.finish();
    }
    task.attachOperator(operator);
    return task;
  }

  @Override
  public Task visitOperator(Session session, Limit operator, Task[] tasks) {
    Task task = tasks[0];
    if (task.getTaskType() == Task.TaskType.DS) {

      // TODO explain why we need this recalculation
      // consider when count = 0 , e.g.: limit 2,0
      long count = operator.getCount();
      if (count > 0) {
        count = operator.getOffset() + operator.getCount();
      }

      Limit pushDownLimit = new Limit(0, count);
      pushDownLimit.setStatInfo(operator.getStatInfo());
      task.attachOperator(pushDownLimit);
      task = task.finish();
    }
    task.attachOperator(operator);
    return task;
  }

  @Override
  public Task visitOperator(Session session, Order operator, Task[] tasks) {
    Task task = tasks[0].finish();
    task.attachOperator(operator);
    double cost = OptimizationUtil.getSortCost(task.getStatRowCount());
    task.addCost(cost);
    return task;
  }

  @Override
  public Task visitOperator(Session session, Selection operator, Task[] tasks) {
    Task task = tasks[0].finish();
    task.attachOperator(operator);
    double costWithCpu = task.getStatRowCount() * StatsUtils.CPU_FACTOR;
    task.addCost(costWithCpu);
    return task;
  }

  private double getTopNCost(TopN topN, double count) {
    return count * StatsUtils.CPU_FACTOR + topN.getCount() * StatsUtils.MEMORY_FACTOR;
  }

  @Override
  public Task visitOperator(Session session, TopN operator, Task[] tasks) {
    Task task = tasks[0];
    if (task.getTaskType() == Task.TaskType.DS) {
      DSTask dsTask = (DSTask) task;

      // TODO explain why we need this recalculation
      // consider when count = 0 , e.g.: limit 2,0
      long count = operator.getCount();
      if (count > 0) {
        count = operator.getOffset() + operator.getCount();
      }

      TopN topN = new TopN(operator.getOffset(), count, operator.getOrderExpressions(), operator.getStatInfo());

      Order.OrderExpression[] orderExpressions = operator.getOrderExpressions();
      // TODO get rid of this check
      List<ColumnExpr> columnExprs = Collections.emptyList();
      if (orderExpressions != null) {
        columnExprs = Lists.newArrayListWithExpectedSize(orderExpressions.length);
        for (Order.OrderExpression orderExpression : orderExpressions) {
          columnExprs.add((ColumnExpr) orderExpression.getExpression());
        }
      }

      if (!dsTask.isIndexFinished()
//              && OptimizationUtil.columnsCoveredByIndex(columnExprs, dsTask.getPushedDownIndexPlan().getSchema().getColumns())
      ) {
        dsTask.attachOperatorToPushedDownIndexPlan(topN);
      } else {
        dsTask.finishIndexPlan();
        dsTask.attachOperatorToPushedDownTablePlan(topN);
      }
      double cost1 = getTopNCost(operator, dsTask.getStatRowCount());
      dsTask.addCost(cost1);
    }
    task = task.finish();
    task.attachOperator(operator);
    double cost2 = getTopNCost(operator, task.getStatRowCount());
    task.addCost(cost2);
    return task;
  }
}
