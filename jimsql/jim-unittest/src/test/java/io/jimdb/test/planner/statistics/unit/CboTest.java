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

import java.util.Collections;
import java.util.List;

import io.jimdb.common.exception.BaseException;
import io.jimdb.core.model.meta.MetaData;
import io.jimdb.core.model.meta.Table;
import io.jimdb.sql.operator.RelOperator;
import io.jimdb.sql.optimizer.statistics.TableStats;
import io.jimdb.sql.optimizer.statistics.TableStatsManager;
import io.jimdb.test.mock.store.MockTableData;
import io.jimdb.test.planner.TestBase;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class CboTest extends TestBase {

  // get table from catalog.
  static final Table table = MetaData.Holder.get().getTable(CATALOG, USER_TABLE);
  static final List<MockTableData.User> userDataList = MockTableData.getUserDataList();

  @BeforeClass
  public static void cboTestSetup() {
  }

  private void run(Checker checker) {
    try {
      RelOperator relOperator = buildPlanAndOptimize(checker.getSql());
      Assert.assertNotNull(relOperator);
      checker.doCheck(relOperator);
    } catch (BaseException e) {
      e.printStackTrace();
      Assert.assertTrue(checker.isHasException());
    }
  }

  @Test
  public void testCBOWithNullTableStats() {
    TableStatsManager.resetAllTableStats();
    Checker checker = Checker.build().sql("select name, age from user where age > 20 and salary > 7000");

    run(checker);
  }

  //@Test
  public void testGetAllTableStats() {
    TableStatsManager.updateTableStatsCache(Collections.singleton(table));
    TableStats tableStats = TableStatsManager.getTableStats(table);
    Assert.assertEquals("Row count in table stats should be equal to the actual row count",
            userDataList.size(), tableStats.getEstimatedRowCount());

    Checker checker = Checker.build().sql("select * from user");
    run(checker);
  }
}
