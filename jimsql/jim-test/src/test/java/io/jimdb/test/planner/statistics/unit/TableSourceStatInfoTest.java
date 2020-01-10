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
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.ValueRange;
import io.jimdb.core.model.meta.MetaData;
import io.jimdb.core.model.meta.Table;
import io.jimdb.sql.operator.RelOperator;
import io.jimdb.sql.operator.TableSource;
import io.jimdb.sql.optimizer.statistics.ColumnStats;
import io.jimdb.sql.optimizer.statistics.Histogram;
import io.jimdb.sql.optimizer.statistics.IndexStats;
import io.jimdb.sql.optimizer.statistics.TableSourceStatsInfo;
import io.jimdb.sql.optimizer.statistics.TableStats;
import io.jimdb.test.planner.TestBase;
import io.jimdb.core.values.LongValue;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TableSourceStatInfoTest extends TestBase {

  private static TableSourceStatsInfo tableSourceStatInfo;
  private static Table table = MetaData.Holder.getMetaData().getTable(CATALOG, USER_TABLE);

  private static List<ColumnExpr> columnExprs = Arrays.stream(table.getReadableColumns())
          .map(column -> new ColumnExpr(session.allocColumnID(), column))
          .collect(Collectors.toList());

  @Mock
  private static Histogram histogram;

  @Mock
  private static TableStats tableStats;

  @Mock
  private static IndexStats indexStats;

  @Mock
  private static ColumnStats columnStats;

  @Before
  public void tableSourceStatInfoTestSetup() {

    MockitoAnnotations.initMocks(this);
    when(tableStats.getNonIndexedColumnStatsMap())
            .thenReturn(Collections.singletonMap(columnExprs.get(0).getId(), columnStats));

    when(tableStats.getIndexStatsMap()).thenReturn(Collections.singletonMap(table.getReadableIndices()[0].getId(), indexStats));
    when(tableStats.getNonIndexedColumnStatsMap()).thenReturn(Collections.singletonMap(table.getReadableColumns()[0].getId(), columnStats));
    when(tableStats.getEstimatedRowCount()).thenReturn(10L);
    when(tableStats.getModifiedCount()).thenReturn(10L);
    when(tableStats.getColumnStats(anyLong())).thenReturn(columnStats);

    when(indexStats.getIndexInfo()).thenReturn(table.getReadableIndices()[0]);
    when(indexStats.getHistogram()).thenReturn(histogram);

    when(columnStats.getHistogram()).thenReturn(histogram);
    when(columnStats.getTotalRowCount()).thenReturn(10L);
    when(histogram.getNdv()).thenReturn(10L);
    when(histogram.estimateIncreasingFactor(anyLong())).thenReturn(10.0);

    tableSourceStatInfo = new TableSourceStatsInfo(tableStats, table.getReadableColumns(), columnExprs);
  }

  @Test
  public void testCalculateSelectivity() {
    String sql = "select name, age from user where age > 20 and salary > 7000";
    RelOperator logicalPlan = buildPlanAndLogicalOptimizeOnly(sql);
    TableSource tableSource = (TableSource) logicalPlan.getChildren()[0];
    double selectivity = tableSourceStatInfo.calculateSelectivity(session, tableSource.getPushDownPredicates());

    Assert.assertEquals("selectivity should be equal", 0.8, selectivity, 0.1);
  }

  @Test
  public void testEstimateRowCountByNonIndexedRanges() {
    final List<ValueRange> ranges =
            Collections.singletonList(new ValueRange(LongValue.getInstance(0L), LongValue.getInstance(10L)));

    when(columnStats.estimateRowCount(any(), anyList(), anyLong())).thenReturn(0.3);

    double rowCount = tableSourceStatInfo.estimateRowCountByNonIndexedRanges(session, columnExprs.get(0).getUid(), ranges);
    Assert.assertEquals("row count should be equal", 3.0, rowCount, 0.1);
  }

  @Test
  public void testEstimateRowCountByIndexedRanges() {
    final List<ValueRange> ranges =
            Collections.singletonList(new ValueRange(LongValue.getInstance(0L), LongValue.getInstance(10L)));

    when(indexStats.isInValid(session)).thenReturn(false);
    when(indexStats.estimateRowCount(any(), anyList(), anyLong())).thenReturn(0.3);

    double rowCount = tableSourceStatInfo.estimateRowCountByIndexedRanges(session, table.getReadableIndices()[0].getId(), ranges);
    Assert.assertEquals("row count should be equal", 3.0, rowCount, 0.1);
  }
}
