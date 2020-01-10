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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.stream.Collectors;

import io.jimdb.core.expression.ValueRange;
import io.jimdb.core.model.meta.Column;
import io.jimdb.sql.optimizer.statistics.ColumnSampleCollector;
import io.jimdb.sql.optimizer.statistics.ColumnStats;
import io.jimdb.sql.optimizer.statistics.CountMinSketch;
import io.jimdb.sql.optimizer.statistics.Histogram;
import io.jimdb.core.values.LongValue;
import io.jimdb.core.values.Value;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

public class ColumnStatsTest extends CboTest {

  private static ColumnStats columnStats;

  @BeforeClass
  public static void columnStatsTestSetup() throws NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
    Constructor<ColumnStats> constructor = ColumnStats.class.getDeclaredConstructor(CountMinSketch.class, Histogram.class, Column.class);
    constructor.setAccessible(true);

    columnStats = constructor.newInstance(mockCountMinSketch(), mockHistogram(), table.getReadableColumn("score"));
  }

  private static Histogram mockHistogram() {
    final List<Value> values = userDataList.stream()
                                       .map(user -> LongValue.getInstance(user.score))
                                       .collect(Collectors.toList());

    ColumnSampleCollector sampleCollector = new ColumnSampleCollector(values.size());
    values.forEach(sampleCollector::collect);

    final Column columnInfo = table.getReadableColumn("score");
    return new Histogram.DefaultIndexOrColumnHistogramBuilder(sampleCollector, columnInfo.getId(), 1).withSession(session).build();
  }

  private static CountMinSketch mockCountMinSketch() {
    final List<byte[]> data = userDataList.stream()
                                      .map(user -> LongValue.getInstance(user.score).toByteArray())
                                      .collect(Collectors.toList());

    return new CountMinSketch.CountMinSketchBuilder(data, data.size()).build();
  }

  @Test
  public void testEstimateRowCountWithRange() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
    /* Example of using reflection to call private method */
    // Method privateMethod = ColumnStats.class.getDeclaredMethod("estimateRowCount", Session.class, List.class, long.class);
    // privateMethod.setAccessible(true);
    // final List<ValueRange> ranges = Lists.newArrayList(new ValueRange(LongValue.getInstance(0L), LongValue.getInstance(10L)));
    // double estimatedRowCount = (double) privateMethod.invoke(columnStats, session, ranges, 10L);

    final List<ValueRange> ranges = Lists.newArrayList(new ValueRange(LongValue.getInstance(0L), LongValue.getInstance(10L)));
    double estimatedRowCount = columnStats.estimateRowCount(session, ranges, 10L);
    Assert.assertEquals("row count should be equal", 10.0, estimatedRowCount, 0.1);
  }

  @Test
  public void testEstimateRowCount() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
    /* Example of using reflection to call private method */
    // Method privateMethod = ColumnStats.class.getDeclaredMethod("estimateRowCount", Session.class, Value.class, long.class);
    // privateMethod.setAccessible(true);
    // double estimatedRowCount = (double) privateMethod.invoke(columnStats, session, LongValue.getInstance(40L), 10L);

    double estimatedRowCount = columnStats.estimateRowCount(session, LongValue.getInstance(40L), 10L);
    Assert.assertEquals("row count should be equal", 1.0, estimatedRowCount, 0.1);
  }
}
