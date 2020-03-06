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

package io.jimdb.test.planner.statistics.unit;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.jimdb.core.expression.TableAccessPath;
import io.jimdb.core.expression.ValueRange;
import io.jimdb.sql.operator.DualTable;
import io.jimdb.sql.operator.IndexLookup;
import io.jimdb.sql.operator.IndexSource;
import io.jimdb.sql.operator.RelOperator;
import io.jimdb.sql.operator.TableSource;
import io.jimdb.sql.optimizer.physical.BestTaskFinder;
import io.jimdb.sql.optimizer.physical.PhysicalProperty;
import io.jimdb.sql.optimizer.physical.RangeBuilder;
import io.jimdb.sql.optimizer.physical.Task;
import io.jimdb.test.planner.TestBase;
import io.jimdb.core.values.LongValue;
import io.jimdb.core.values.NullValue;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class BestTaskFinderTest extends TestBase {

  private static BestTaskFinder bestTaskFinder;

  @BeforeClass
  public static void bestTaskFinderTestSetup() {
    bestTaskFinder = new BestTaskFinder();
  }

  @Test
  public void testVisitOperatorByDefault() {
    // create a plan tree
    String sql = "select name, age from user where age > 20 and salary > 7000";
    RelOperator physicalPlan = buildPlanAndLogicalOptimizeOnly(sql);
    TableSource tableSource = (TableSource) physicalPlan.getChildren()[0];

    // mock data
    List<TableAccessPath> paths = tableSource.getTableAccessPaths();
    for (TableAccessPath path : paths) {
      path.setCountOnAccess(10.0);
      path.setRanges(RangeBuilder.fullRangeList());

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

    // run method
    Task task = bestTaskFinder.visitOperator(session, physicalPlan, PhysicalProperty.DEFAULT_PROP);

    // check result
    IndexLookup indexLookup = (IndexLookup) task.getPlan().getChildren()[0];
    Assert.assertNotNull(indexLookup);

    IndexSource indexSource = indexLookup.getIndexSource();
    String bestIndex = indexSource.getKeyValueRange().getIndex().getName();
    Assert.assertEquals("check best index plan", "idx_salary", bestIndex);
    Assert.assertEquals("check best task cost", 11.0, task.getCost(), 0.1);

    TableSource tablePlan = (TableSource) indexLookup.getPushedDownTablePlan();
    Assert.assertNotNull(tablePlan);
  }

  @Test
  public void testVisitTableSourceOperator() {
    // create a plan tree
    String sql = "select name, age from user where age > 20 and salary > 7000";
    RelOperator logicalPlan = buildPlanAndLogicalOptimizeOnly(sql);
    TableSource tableSource = (TableSource) logicalPlan.getChildren()[0];

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

    // run method
    Task task = bestTaskFinder.visitOperator(session, tableSource, PhysicalProperty.DEFAULT_PROP);

    RelOperator plan = task.getPlan();
    Assert.assertTrue("root of the plan should be index look up", plan instanceof IndexLookup);

    // check result
    IndexSource indexSource = ((IndexLookup) plan).getIndexSource();
    String bestIndex = indexSource.getKeyValueRange().getIndex().getName();
    Assert.assertEquals("check best index plan", "idx_salary", bestIndex);
    Assert.assertEquals("check best task cost", 11.0, task.getCost(), 0.1);
  }

  @Test
  public void testVisitDualTableOperator() {
    // create a plan tree
    DualTable dualTable = new DualTable(1);

    // run method
    Task task = bestTaskFinder.visitOperator(session, dualTable, PhysicalProperty.DEFAULT_PROP);

    // check result
    Assert.assertEquals("check task plan for dual table", 1.0, task.getCost(), 0.1);
  }
}
