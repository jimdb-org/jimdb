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

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.Schema;
import io.jimdb.core.model.meta.Column;
import io.jimdb.core.model.meta.Index;
import io.jimdb.core.model.result.impl.QueryExecResult;
import io.jimdb.sql.optimizer.statistics.TableStats;
import io.jimdb.test.mock.store.MockStoreEngine;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TableStatsTest extends CboTest {

  private static QueryExecResult queryExecResult;

  private static Schema schema;

  private static TableStats tableStats;

  @BeforeClass
  public static void tableStatsTestSetup() {
    schema = new Schema(session, table.getReadableColumns());
    queryExecResult = (QueryExecResult) ((MockStoreEngine) session.getStoreEngine()).getExecResult(table, schema.getColumns().toArray(new ColumnExpr[0]), null);
    tableStats = new TableStats(table, 0);
  }

  @Test
  public void testUpdate() {
    final long expectedRowCount = userDataList.size();
    Assert.assertEquals("number of rows should be equal.", expectedRowCount, queryExecResult.size());

    final Index[] indices = table.getReadableIndices();
    Assert.assertNotNull("table indices should not be null", indices);

    tableStats.update(session, queryExecResult);

    final int expectedIndexCount = table.getReadableIndices().length;
    Set<Integer> indexedColOffsets = Arrays.stream(indices)
                                             .flatMap(i -> Arrays.stream(i.getColumns()).map(Column::getOffset))
                                             .collect(Collectors.toSet());

    final int[] nonIndexColOffsets = Arrays.stream(table.getReadableColumns())
                                             .map(Column::getOffset)
                                             .filter(i -> !indexedColOffsets.contains(i)).mapToInt(i -> i).toArray();

    final int expectedNonIndexedColumnCount = nonIndexColOffsets.length;

    Assert.assertEquals("indexStatsMap size should be equal", expectedIndexCount, tableStats.getIndexStatsMap().size());
    Assert.assertEquals("nonIndexedColumnStatsMap size should be equal", expectedNonIndexedColumnCount, tableStats.getColumnStatsMap().size());

    Assert.assertEquals("rowCount should be equal", expectedRowCount, tableStats.getEstimatedRowCount());
  }
}
