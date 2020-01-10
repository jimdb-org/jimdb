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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.stream.Collectors;

import io.jimdb.core.expression.ValueRange;
import io.jimdb.core.model.meta.Index;
import io.jimdb.sql.optimizer.statistics.CountMinSketch;
import io.jimdb.sql.optimizer.statistics.Histogram;
import io.jimdb.sql.optimizer.statistics.IndexSampleCollector;
import io.jimdb.sql.optimizer.statistics.IndexStats;
import io.jimdb.core.values.LongValue;
import io.jimdb.core.values.Value;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

public class IndexStatsTest extends CboTest {

  private static IndexStats indexStats;
  private static Index indexInfo;

  @BeforeClass
  public static void indexStatsTestSetup() throws NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
    Constructor<IndexStats> constructor = IndexStats.class.getDeclaredConstructor(CountMinSketch.class, Histogram.class, Index.class);
    constructor.setAccessible(true);

    indexInfo = retrieveIndexInfo();
    indexStats = constructor.newInstance(mockCountMinSketch(), mockHistogram(), indexInfo);
  }

  private static Index retrieveIndexInfo() {
    for (Index index : table.getReadableIndices()) {
      if (index.getName().equals("idx_age")) {
        return index;
      }
    }

    // This should never happen
    return null;
  }

  private static Histogram mockHistogram() {
    final List<Value[]> values = userDataList.stream()
                                       .map(user -> new Value[]{LongValue.getInstance(user.age)})
                                       .collect(Collectors.toList());

    IndexSampleCollector sampleCollector = new IndexSampleCollector(values.size(), indexInfo.getColumns().length);
    values.forEach(sampleCollector::collect);

    return new Histogram.DefaultIndexOrColumnHistogramBuilder(sampleCollector, indexInfo.getId(), 1).withSession(session).build();
  }

  private static CountMinSketch mockCountMinSketch() {
    final List<byte[]> data = userDataList.stream()
                                      .map(user -> LongValue.getInstance(user.score).toByteArray())
                                      .collect(Collectors.toList());

    return new CountMinSketch.CountMinSketchBuilder(data, data.size()).build();
  }

  @Test
  public void testEstimateRowCountWithRange() {
    // age: 10, 20, 20, 30, 30, 30, 40, 40, 50, 60
    final List<ValueRange> ranges = Lists.newArrayList(new ValueRange(LongValue.getInstance(0L), LongValue.getInstance(40L)));
    double estimatedRowCount = indexStats.estimateRowCount(session, ranges, 10L);
    Assert.assertEquals("row count should be equal", 5.68, estimatedRowCount, 0.1);
  }

  @Test
  public void testEstimateRowCount() {
    // age: 10, 20, 20, 30, 30, 30, 40, 40, 50, 60
    double estimatedRowCount = indexStats.estimateRowCount(session, LongValue.getInstance(40L), 10L);
    Assert.assertEquals("row count should be equal", 1.0, estimatedRowCount, 0.1);
  }

  @Test
  public void testNewIndexStatsFromSelectivity () {
    // TODO currently this method has not been used
  }
}
