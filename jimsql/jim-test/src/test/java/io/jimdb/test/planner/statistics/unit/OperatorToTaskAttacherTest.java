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

package io.jimdb.test.planner.statistics.unit;

import java.util.ArrayList;
import java.util.List;

import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.TableAccessPath;
import io.jimdb.core.expression.ValueRange;
import io.jimdb.sql.operator.Aggregation;
import io.jimdb.sql.operator.Limit;
import io.jimdb.sql.operator.Order;
import io.jimdb.sql.operator.Projection;
import io.jimdb.sql.operator.RelOperator;
import io.jimdb.sql.operator.Selection;
import io.jimdb.sql.operator.TableSource;
import io.jimdb.sql.operator.TopN;
import io.jimdb.sql.optimizer.physical.BestTaskFinder;
import io.jimdb.sql.optimizer.physical.OperatorToTaskAttacher;
import io.jimdb.sql.optimizer.physical.PhysicalProperty;
import io.jimdb.sql.optimizer.physical.Task;
import io.jimdb.sql.optimizer.statistics.OperatorStatsInfo;
import io.jimdb.test.planner.TestBase;
import io.jimdb.core.values.LongValue;
import io.jimdb.core.values.NullValue;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class OperatorToTaskAttacherTest extends TestBase {
  private static OperatorToTaskAttacher operatorToTaskAttacher;
  private static BestTaskFinder bestTaskFinder;
  private static TableSource tableSource;
  private static Aggregation aggregation;
  private static Task[] tasks = new Task[1];

  @BeforeClass
  public static void operatorToTaskAttacherTestSetup() {
    operatorToTaskAttacher = new OperatorToTaskAttacher();
    bestTaskFinder = new BestTaskFinder();
  }

  @Before
  public void testInit() {
    // create a plan tree
      String sql1 = "select name, age from user where age > 20 and salary > 7000";
      RelOperator logicalPlan = buildPlanAndLogicalOptimizeOnly(sql1);
      tableSource = (TableSource) logicalPlan.getChildren()[0];

      // mock data
      List<TableAccessPath> paths = tableSource.getTableAccessPaths();
      for (TableAccessPath path : paths) {
        if (!path.isTablePath()) {
          if (path.getIndex().getName().equals("idx_age")) {
            path.setCountOnAccess(7.0);
            LongValue value = LongValue.getInstance(20);
            ValueRange range = new ValueRange(value, NullValue.getInstance());
            List<ValueRange> valueRanges = new ArrayList<>();
            valueRanges.add(range);
            path.setRanges(valueRanges);
          } else if (path.getIndex().getName().equals("idx_salary")) {
            path.setCountOnAccess(2.0);
            LongValue value = LongValue.getInstance(7000);
            ValueRange range = new ValueRange(value, NullValue.getInstance());
            List<ValueRange> valueRanges = new ArrayList<>();
            valueRanges.add(range);
            path.setRanges(valueRanges);
          }
        }
      }

      // find best task
      tasks[0] = bestTaskFinder.visitOperator(session, tableSource, PhysicalProperty.DEFAULT_PROP);
  }

  @Test
  public void testVisitOperatorByDefault() {
    // prepare test data
    Projection projection = new Projection(new Expression[1], tableSource.getSchema(), tableSource);

    // run method
    Task task = operatorToTaskAttacher.visitOperatorByDefault(session, projection, tasks);

    // check result
    Assert.assertEquals("check task cost after attachment", 11.0, task.getCost(), 0.1);
  }

  @Test
  public void testVisitAggregationOperator() {
    // prepare test data
    String sql2 = "select sum(age) from user";
    RelOperator logicalPlan2 = buildPlanAndLogicalOptimizeOnly(sql2);
    aggregation = (Aggregation) logicalPlan2.getChildren()[0];
    aggregation.setStatInfo(new OperatorStatsInfo(5));


    // run method
    Task task = operatorToTaskAttacher.visitOperator(session, aggregation, tasks);

    // check result
    Assert.assertEquals("check task cost after attachment", 15.5, task.getCost(), 0.1);
  }

  @Test
  public void testVisitProjectionOperator() {
    // prepare test data
    Projection projection = new Projection(new Expression[1], tableSource.getSchema(), tableSource);

    // run method
    Task task = operatorToTaskAttacher.visitOperator(session, projection, tasks);

    // check result
    Assert.assertEquals("check task cost after attachment", 11.0, task.getCost(), 0.1);
  }

  @Test
  public void testVisitOrderOperator() {
    // prepare test data
    Order order = new Order(new Order.OrderExpression[1], tableSource);
    order.setStatInfo(new OperatorStatsInfo(10));

    // run method
    Task task = operatorToTaskAttacher.visitOperator(session, order, tasks);

    // check result
    Assert.assertEquals("check task cost after attachment", 70.0, task.getCost(), 0.1);
  }

  @Test
  public void testVisitSelectionOperator() {
    // prepare test data
    Selection selection = new Selection(new ArrayList<>(), tableSource);
    selection.setStatInfo(new OperatorStatsInfo(10));

    // run method
    Task task = operatorToTaskAttacher.visitOperator(session, selection, tasks);

    // check result
    Assert.assertEquals("check task cost after attachment", 20.0, task.getCost(), 0.1);
  }

  @Test
  public void testVisitTopNOperator() {
    // prepare test data
    TopN topN = new TopN(0, 5);
    topN.setStatInfo(new OperatorStatsInfo(5));
    topN.setChildren(tableSource);

    // run method
    Task task = operatorToTaskAttacher.visitOperator(session, topN, tasks);

    // check result
    Assert.assertEquals("check task cost after attachment", 40.5, task.getCost(), 0.1);
  }

  @Test
  public void testVisitLimitOperator() {
    // prepare test data
    Limit limit = new Limit(0, 3);
    limit.setStatInfo(new OperatorStatsInfo(3));
    limit.setChildren(tableSource);

    // run method
    Task task = operatorToTaskAttacher.visitOperator(session, limit, tasks);

    // check result
    Assert.assertEquals("check task cost after attachment", 11.0, task.getCost(), 0.1);
  }

}
