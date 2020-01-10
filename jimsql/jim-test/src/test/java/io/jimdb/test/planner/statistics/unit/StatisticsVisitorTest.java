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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;

import io.jimdb.sql.operator.Aggregation;
import io.jimdb.sql.operator.DualTable;
import io.jimdb.sql.operator.Limit;
import io.jimdb.sql.operator.Projection;
import io.jimdb.sql.operator.RelOperator;
import io.jimdb.sql.operator.Selection;
import io.jimdb.sql.operator.TableSource;
import io.jimdb.sql.operator.TopN;
import io.jimdb.sql.optimizer.physical.StatisticsVisitor;
import io.jimdb.sql.optimizer.statistics.OperatorStatsInfo;
import io.jimdb.sql.optimizer.statistics.TableSourceStatsInfo;
import io.jimdb.sql.optimizer.statistics.TableStats;
import io.jimdb.sql.optimizer.statistics.TableStatsManager;
import io.jimdb.test.planner.TestBase;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest(StatisticsVisitor.class)
@PowerMockIgnore({"javax.management.*", "org.apache.http.conn.ssl.*", "javax.net.ssl.*" , "javax.crypto.*", "javax.script.*"})
public class StatisticsVisitorTest extends TestBase {

  private static StatisticsVisitor statisticsVisitor;

  @BeforeClass
  public static void statisticsVisitorTestSetup() {
    statisticsVisitor = new StatisticsVisitor();
  }

  @Test
  public void testDeriveStatInfo() {
    // create a plan tree
    String sql = "select name, age from user where age > 20 and salary > 7000";
    RelOperator logicalPlan = buildPlanAndLogicalOptimizeOnly(sql);

    // mock stat data
    OperatorStatsInfo mockedStatInfo = mock(OperatorStatsInfo.class);
    when(mockedStatInfo.getEstimatedRowCount()).thenReturn(20.0);
    when(mockedStatInfo.newCardinalityList(anyDouble())).thenReturn(new double[]{ 1.0 });
    when(mockedStatInfo.getCardinality(anyInt())).thenReturn(1.0);

    // table source
    logicalPlan.getChildren()[0].setStatInfo(mockedStatInfo);

    // run method
    statisticsVisitor.deriveStatInfo(session, logicalPlan);

    // check result
    Assert.assertEquals("row count should be equal for projection stat info",
            20.0, logicalPlan.getStatInfo().getEstimatedRowCount(), 0.1);
  }

  @Test
  public void testVisitOperatorByDefault() {
    // create a plan tree
    String sql = "select name, age from user where age > 20 and salary > 7000";
    RelOperator logicalPlan = buildPlanAndLogicalOptimizeOnly(sql);
    Projection projection = ((Projection) logicalPlan).copy();

    // run method
    OperatorStatsInfo statInfo = statisticsVisitor.visitOperatorByDefault(session, projection, new OperatorStatsInfo[0]);

    // check result
    Assert.assertEquals("check cardinality list size",
            2, statInfo.getCardinalityListSize());
  }

  @Test
  public void testVisitProjectionOperator() {
    // create a plan tree
    String sql = "select name, age from user where age > 20 and salary > 7000";
    RelOperator logicalPlan = buildPlanAndLogicalOptimizeOnly(sql);
    Projection projection = ((Projection) logicalPlan).copy();

    // mock stat data
    OperatorStatsInfo[] childStats = new OperatorStatsInfo[1];
    OperatorStatsInfo mockedStatInfo = mock(OperatorStatsInfo.class);
    childStats[0] = mockedStatInfo;

    when(mockedStatInfo.getEstimatedRowCount()).thenReturn(10.0);
    when(mockedStatInfo.newCardinalityList(anyDouble())).thenReturn(new double[]{ 1.0 });
    when(mockedStatInfo.getCardinality(anyInt())).thenReturn(1.0);

    // run method
    statisticsVisitor.visitOperator(session, projection, childStats);

    // check result
    Assert.assertEquals("row count should be equal",
            10.0, projection.getStatInfo().getEstimatedRowCount(), 0.1);
  }

  @Test
  public void testVisitLimitOperator() {
    Limit limit = new Limit(0L, 100L);

    OperatorStatsInfo[] childStats = new OperatorStatsInfo[1];
    OperatorStatsInfo mockedStatInfo = mock(OperatorStatsInfo.class);
    childStats[0] = mockedStatInfo;

    when(mockedStatInfo.getEstimatedRowCount()).thenReturn(30.0);
    when(mockedStatInfo.newCardinalityList(anyDouble())).thenReturn(new double[]{ 1.0 });

    statisticsVisitor.visitOperator(session, limit, childStats);

    Assert.assertEquals("row count should be equal",
            30.0, limit.getStatInfo().getEstimatedRowCount(), 0.1);
  }

  @Test
  public void testVisitTopNOperator() {
    // create operator
    TopN topN = new TopN(0L, 10L);

    OperatorStatsInfo[] childStats = new OperatorStatsInfo[1];
    OperatorStatsInfo mockedStatInfo = mock(OperatorStatsInfo.class);
    childStats[0] = mockedStatInfo;

    when(mockedStatInfo.getEstimatedRowCount()).thenReturn(10.0);
    when(mockedStatInfo.newCardinalityList(anyDouble())).thenReturn(new double[]{ 1.0 });

    // run method
    statisticsVisitor.visitOperator(session, topN, childStats);

    // check result
    Assert.assertEquals("row count should be equal",
            10.0, topN.getStatInfo().getEstimatedRowCount(), 0.1);
  }

  @Test
  public void testVisitDualTableOperator() {
    // create a plan tree
    String sql = "select name, age, salary from user";
    RelOperator logicalPlan = buildPlanAndLogicalOptimizeOnly(sql);
    DualTable dualTable = new DualTable(1);
    dualTable.setSchema(logicalPlan.getSchema());

    // run method
    OperatorStatsInfo statInfo = statisticsVisitor.visitOperator(session, dualTable, new OperatorStatsInfo[0]);

    // check result
    Assert.assertEquals("check cardinality list size",
            3, statInfo.getCardinalityListSize());
  }

  @Test
  public void testVisitSelectionOperator() {
    // create operator
    Selection selection = new Selection(new ArrayList<>());

    OperatorStatsInfo statInfo = new OperatorStatsInfo(50, new double[]{ 2.0 });

    // run method
    statInfo = statisticsVisitor.visitOperator(session, selection, new OperatorStatsInfo[]{ statInfo });

    // check result
    Assert.assertEquals("row count should be equal",
            40.0, statInfo.getEstimatedRowCount(), 0.1);
  }

  @Test
  public void testVisitAggregationOperator() {
    // create a plan tree
    String sql = "select sum(salary) from user";
    RelOperator logicalPlan = buildPlanAndLogicalOptimizeOnly(sql);
    Aggregation aggregation = ((Aggregation) (logicalPlan.getChildren()[0])).copy();

    // mock stat data
    OperatorStatsInfo[] childStats = new OperatorStatsInfo[1];
    OperatorStatsInfo mockedStatInfo = mock(OperatorStatsInfo.class);
    childStats[0] = mockedStatInfo;

    when(mockedStatInfo.getEstimatedRowCount()).thenReturn(60.0);
    when(mockedStatInfo.newCardinalityList(anyDouble())).thenReturn(new double[]{ 1.0 });
    when(mockedStatInfo.getCardinality(anyInt())).thenReturn(1.0);

    // run method
    statisticsVisitor.visitOperator(session, aggregation, childStats);

    Assert.assertEquals("row count should be equal",
            60.0, aggregation.getStatInfo().getEstimatedRowCount(), 0.1);
  }

  @Test
  public void testVisitTableSourceOperator() throws Exception {
    // create a plan tree
    String sql = "select name, age from user where age > 20 and salary > 7000";
    RelOperator logicalPlan = buildPlanAndLogicalOptimizeOnly(sql);
    TableSource tableSource = (TableSource) logicalPlan.getChildren()[0];

    TableSourceStatsInfo tableSourceStatInfo = PowerMockito.mock(TableSourceStatsInfo.class);
    PowerMockito.whenNew(TableSourceStatsInfo.class).withAnyArguments().thenReturn(tableSourceStatInfo);

    when(tableSourceStatInfo.getEstimatedRowCount()).thenReturn(10.0);
    when(tableSourceStatInfo.calculateSelectivity(session, tableSource.getPushDownPredicates())).thenReturn(2.0);
    when(tableSourceStatInfo.adjust(2.0)).thenReturn(tableSourceStatInfo);
    when(tableSourceStatInfo.estimateRowCountByNonIndexedRanges(any(), anyLong(), anyList())).thenReturn(10.0);
    when(tableSourceStatInfo.estimateRowCountByIndexedRanges(any(), anyLong(), anyList())).thenReturn(5.0);

    TableStats tableStats = PowerMockito.mock(TableStats.class);
    TableStatsManager.addToCache(tableSource.getTable(), tableStats);
    statisticsVisitor = new StatisticsVisitor();

    // run method
    statisticsVisitor.visitOperator(session, tableSource, new OperatorStatsInfo[0]);

    // check result
    Assert.assertEquals("row count should be equal",
            10.0, tableSource.getStatInfo().getEstimatedRowCount(), 0.1);
    Assert.assertEquals("check count on access for path",
            5.0, tableSource.getTableAccessPaths().get(1).getCountOnAccess(), 0.1);
  }
}

