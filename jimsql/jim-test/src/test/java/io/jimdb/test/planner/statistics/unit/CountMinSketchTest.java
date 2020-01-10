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

import java.util.List;
import java.util.stream.Collectors;

import io.jimdb.sql.optimizer.statistics.CountMinSketch;
import io.jimdb.core.values.LongValue;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit tests for Count-Min Sketch related operations
 */
public class CountMinSketchTest extends CboTest {

  private static CountMinSketch countMinSketch;

  @BeforeClass
  public static void countMinSketchTestSetup() {
    final List<byte[]> data = userDataList.stream()
                                       .map(user -> LongValue.getInstance(user.score).toByteArray())
                                       .collect(Collectors.toList());

    countMinSketch = new CountMinSketch.CountMinSketchBuilder(data, data.size()).build();
  }

  @Test
  public void testQueryValue() {
    long count = countMinSketch.queryValue(LongValue.getInstance(50L));

    // 40, 50, 60, 70, 80, 80, 90, 90, 100, 100
    Assert.assertEquals("row count should be equal", 1L, count);
  }
}
