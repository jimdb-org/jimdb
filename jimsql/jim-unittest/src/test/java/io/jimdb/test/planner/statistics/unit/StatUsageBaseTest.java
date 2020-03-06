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
import static org.mockito.Mockito.when;

import io.jimdb.core.model.meta.MetaData;
import io.jimdb.core.model.meta.Table;
import io.jimdb.core.plugin.SQLEngine;
import io.jimdb.sql.planner.Planner;
import io.jimdb.sql.operator.RelOperator;
import io.jimdb.sql.optimizer.physical.StatisticsVisitor;
import io.jimdb.sql.optimizer.statistics.TableSourceStatsInfo;
import io.jimdb.sql.optimizer.statistics.TableStats;
import io.jimdb.sql.optimizer.statistics.TableStatsManager;
import io.jimdb.test.planner.TestBase;

import org.junit.Assert;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ StatisticsVisitor.class, Planner.class })
@PowerMockIgnore({ "javax.management.*", "org.apache.http.conn.ssl.*", "javax.net.ssl.*", "javax.crypto.*", "javax"
        + ".script.*" })
public class StatUsageBaseTest extends TestBase {

  protected static TableSourceStatsInfo tableSourceStatInfo;
  protected static TableStats tableStats;

  protected static TableStatsManager tableStatsManager;

  @Before
  public void statUsageTestSetup() {
    tableStatsManager = PowerMockito.mock(TableStatsManager.class);
    planner = new Planner(SQLEngine.DBType.MYSQL);
    tableStats = PowerMockito.mock(TableStats.class);

    tableSourceStatInfo = PowerMockito.mock(TableSourceStatsInfo.class);
    try {
      PowerMockito.whenNew(TableSourceStatsInfo.class).withAnyArguments().thenReturn(tableSourceStatInfo);
    } catch (Exception e) {
      e.printStackTrace();
    }

    when(tableSourceStatInfo.getEstimatedRowCount()).thenReturn(10.0);
    when(tableSourceStatInfo.adjust(anyDouble())).thenReturn(tableSourceStatInfo);
    when(tableSourceStatInfo.estimateRowCountByColumnRanges(any(), anyLong(), anyList())).thenReturn(10.0);

    when(tableSourceStatInfo.newCardinalityList(anyDouble())).thenReturn(new double[]{ 1.0 });
    when(tableSourceStatInfo.getCardinality(anyInt())).thenReturn(1.0);
  }

  protected void run(Checker checker) {
    try {
      Table table = MetaData.Holder.get().getTable(CATALOG, USER_TABLE);
      TableStatsManager.addToCache(table, tableStats);

      RelOperator relOperator = buildPlanAndOptimize(checker.getSql());
      Assert.assertNotNull(relOperator);
      checker.doCheck(relOperator);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertTrue(checker.isHasException());
    }
  }
}
