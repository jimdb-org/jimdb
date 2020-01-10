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

package io.jimdb.sql.optimizer.physical;

import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.aggregate.AggregateExpr;
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
import io.jimdb.sql.optimizer.OperatorVisitor;

/**
 * Visitor to generate new physical operators (physical plans) from logical operator (logical plan)
 */
class PhysicalOperatorGenerator extends OperatorVisitor<RelOperator[]> {

  @Override
  public RelOperator[] visitOperatorByDefault(RelOperator operator) {
    return new RelOperator[]{ operator };
  }

  @Override
  public RelOperator[] visitOperator(Order order) {
    Order newOrder = new Order(order.getOrderExpressions());

    newOrder.copyBaseParameters(order);
    return new RelOperator[]{ newOrder };
  }

  @Override
  public RelOperator[] visitOperator(IndexSource operator) {
    return new RelOperator[0];
  }

  @Override
  public RelOperator[] visitOperator(DualTable dual) {
    DualTable newDualTable = new DualTable(dual.getRowCount());
    newDualTable.setPlaceHolder(dual.isPlaceHolder());
    newDualTable.copyBaseParameters(dual);
    newDualTable.setStatInfo(dual.getStatInfo());
    return new RelOperator[]{ newDualTable };
  }

  @Override
  public RelOperator[] visitOperator(TopN topn) {
    TopN newTopN = new TopN(topn.getOffset(), topn.getCount());
    newTopN.setOrderExpression(topn.getOrderExpressions());
    newTopN.setStatInfo(topn.getStatInfo());
    newTopN.setPhysicalProperty(new PhysicalProperty(Task.TaskType.DS));
    return new RelOperator[]{ newTopN };
  }

  @Override
  public RelOperator[] visitOperator(Selection slt) {
    Selection newSelection = new Selection(slt.getConditions());
    newSelection.copyBaseParameters(slt);
    return new RelOperator[]{ newSelection };
  }

  @Override
  public RelOperator[] visitOperator(Projection projection) {
    Projection newProjection = new Projection(projection.getExpressions(), projection.getSchema());
    newProjection.copyBaseParameters(projection);
    return new RelOperator[]{ newProjection };
  }

  @Override
  public RelOperator[] visitOperator(Limit limit) {
    Limit newLimit = new Limit(limit.getOffset(), limit.getCount());
    newLimit.copyBaseParameters(limit);
    newLimit.setPhysicalProperty(new PhysicalProperty(Task.TaskType.DS));
    return new RelOperator[]{ newLimit };
  }

  @Override
  public RelOperator[] visitOperator(TableSource tableSource) {
    TableSource newTableSource = new TableSource(tableSource);
    newTableSource.setPushDownPredicates(tableSource.getPushDownPredicates());
    return new RelOperator[]{ newTableSource };
  }

  @Override
  public RelOperator[] visitOperator(Aggregation aggregation) {
    RelOperator[] streamAggs = getStreamAggs(aggregation);
    RelOperator[] hashAggs = getHashAggs(aggregation);
    return arrayCopy(streamAggs, hashAggs);
  }

  private RelOperator[] getStreamAggs(Aggregation aggregation) {
    // init operatorProp
    PhysicalProperty operatorProp = new PhysicalProperty(aggregation.getGroupByColumnExprs());

    for (AggregateExpr aggregateExpr : aggregation.getAggregateExprs()) {
      if (aggregateExpr.getAggFunctionMode() == AggregateExpr.AggFunctionMode.FINAL_MODE) {
        return null;
      }
    }
    if (aggregation.getGroupByColumnExprs().length != aggregation.getGroupByExprs().length) {
      return null;
    }

    boolean isBuildStreamAgg = false;

    PhysicalProperty childProp = new PhysicalProperty(Task.TaskType.DS);
    if (aggregation.getGroupByColumnExprs().length == 0) {
      isBuildStreamAgg = true;
    } else {
//      if (aggregation.getCandidatesProperties() == null) {
//        LOG.error("getCandidatesProperties null. plan : {}", OperatorUtil.printRelOperatorTreeAsc(aggregation,
//        false));
//      }
      for (ColumnExpr[] exprs : aggregation.getCandidatesProperties()) {
        //exprs columns length is not equal group by items , so we can not promise sort
        if (exprs.length != aggregation.getGroupByColumnExprs().length) {
          continue;
        }
        childProp = new PhysicalProperty(Task.TaskType.DS, exprs);
        if (!operatorProp.prefixWith(childProp)) {
          continue;
        }
        isBuildStreamAgg = true;
        break;
      }
    }

    if (!isBuildStreamAgg) {
      return null;
    }

    Aggregation streamAgg = new Aggregation(aggregation.getAggregateExprs(), aggregation.getGroupByExprs(),
            aggregation.getSchema());
    streamAgg.setAggOpType(Aggregation.AggOpType.StreamAgg);
    streamAgg.setStatInfo(aggregation.getStatInfo());
    streamAgg.setPhysicalProperty(childProp);
    return new RelOperator[]{ streamAgg };
  }

  private RelOperator[] getHashAggs(Aggregation aggregation) {
    Aggregation hashAgg = new Aggregation(aggregation.getAggregateExprs(), aggregation.getGroupByExprs(),
            aggregation.getSchema());
    hashAgg.setStatInfo(aggregation.getStatInfo());
    hashAgg.setAggOpType(Aggregation.AggOpType.HashAgg);
    hashAgg.getPhysicalProperty().setTaskType(Task.TaskType.DS);
    return new RelOperator[]{ hashAgg };
  }

  private RelOperator[] arrayCopy(RelOperator[] t1, RelOperator[] t2) {
    if (t1 == null || t1.length == 0) {
      return t2;
    }
    if (t2 == null || t2.length == 0) {
      return t1;
    }
    RelOperator[] result = new RelOperator[t1.length + t2.length];
    System.arraycopy(t1, 0, result, 0, t1.length);
    System.arraycopy(t2, 0, result, t1.length, t2.length);
    return result;
  }
}
