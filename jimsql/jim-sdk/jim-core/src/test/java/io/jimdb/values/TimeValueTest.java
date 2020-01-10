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
package io.jimdb.values;

import io.jimdb.core.values.TimeValue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

/**
 * TimeValue Tester.
 *
 * @version 1.0
 */
public class TimeValueTest {

  @Before
  public void before() throws Exception {
  }

  @After
  public void after() throws Exception {
  }

  @Test
  public void testGetInstanceFromString() throws Exception {
    Tuple2<String, Integer>[] strs = new Tuple2[11];
    strs[0] = Tuples.of("", 3); //00:00:00
    strs[1] = Tuples.of(" ", 3); //00:00:00
    strs[2] = Tuples.of("  ", 3); //00:00:00
    strs[3] = Tuples.of("-", 3); //00:00:00
    strs[4] = Tuples.of("- ", 3); //00:00:00
    strs[5] = Tuples.of("-   ", 3); //00:00:00
    strs[6] = Tuples.of(" -  ", 3); //00:00:00
    strs[7] = Tuples.of("11:30:45.111", 3); //11:30:45.111
    strs[8] = Tuples.of("-11:30:45", 3);   //-11:30:45
    strs[9] = Tuples.of("838:59:59.000000", 3); //838:59:59.000
    strs[10] = Tuples.of("-838:59:59.000000", 6); //-838:59:59.000

    TimeValue value;
    for (Tuple2<String, Integer> str : strs) {
      value = TimeValue.getInstance(str.getT1(), str.getT2());
      System.out.println(value.convertToString());
    }
  }

  @Test
  public void testGetInstanceFromLong() throws Exception {

    Tuple2<Long, Integer>[] longs = new Tuple2[4];
    longs[0] = Tuples.of(-172222L, 3); //-17:22:22.000000
    longs[1] = Tuples.of(172222L, 3); //17:22:22.000000
    longs[2] = Tuples.of(1217172222L, 3); //838:59:59.000000
    longs[3] = Tuples.of(15708451193L, 3); //45:11:93 -> over range check, err

    TimeValue value;
    for (Tuple2<Long, Integer> l : longs) {
      value = TimeValue.convertToTimeValue(l.getT1(), l.getT2());
      System.out.println(value);
    }
  }
}

