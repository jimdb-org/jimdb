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

package io.jimdb.test.planner.statistics.unit;/*
 * Copyright 2019 The Chubao Authors.
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

import java.util.List;
import java.util.stream.Collectors;

import io.jimdb.core.model.meta.Column;
import io.jimdb.sql.optimizer.statistics.ColumnSampleCollector;
import io.jimdb.sql.optimizer.statistics.Histogram;
import io.jimdb.core.values.LongValue;
import io.jimdb.core.values.Value;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit tests for histogram related tests
 */
public class HistogramTest extends CboTest {

  private static ColumnSampleCollector sampleCollector;
  private static Histogram histogram;

  @BeforeClass
  public static void histogramTestSetup() {

    final List<Value> values = userDataList.stream()
                   .map(user -> LongValue.getInstance(user.score))
                   .collect(Collectors.toList());

    sampleCollector = new ColumnSampleCollector(values.size());
    values.forEach(sampleCollector::collect);

    final Column columnInfo = table.getReadableColumn("score");
    histogram = new Histogram.DefaultIndexOrColumnHistogramBuilder(sampleCollector, columnInfo.getId(), 1).withSession(session).build();
  }

  @Test
  public void testTotalRowCount() {
    Assert.assertEquals("row count should be equal", sampleCollector.sampleCount(), (int) histogram.getTotalRowCount());
  }

  @Test
  public void testEstimateRowCountEqualsTo() {
    // 40, 50, 60, 70, 80, 80, 90, 90, 100, 100
    double actualCount = histogram.estimateRowCountEqualsTo(session, LongValue.getInstance(70L));
    Assert.assertEquals("row count should be equal", 2.0, actualCount, 0.1);
  }

  @Test
  public void testEstimateRowCountBetween() {
    // 40, 50, 60, 70, 80, 80, 90, 90, 100, 100
    double actualCount = histogram.estimateRowCountBetween(session, LongValue.getInstance(40L), LongValue.getInstance(70L));
    Assert.assertEquals("row count should be equal", 4.0, actualCount, 0.1);
  }
}
