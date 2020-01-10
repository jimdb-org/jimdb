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

import io.jimdb.core.values.YearValue;

import org.junit.Test;

/**
 * YearValue Tester.
 *
 * @version 1.0
 */
public class YearValueTest {
  /**
   * Method: getInstance(String sValue)
   */
  @Test
  public void testGetInstance() throws Exception {
    //0000,1901,2000,2155
    int[] iValues = new int[]{ 0, 1901, 2000, 2155 };
    YearValue value;
    for (int temp : iValues) {
      value = YearValue.getInstance(temp);
      System.out.println(value);
    }

    //00~69 change to 2000~2069,  70~99 change to 1970~1999
    //2000,1901,2000,2155, 2000, 2032, 2069, 1970, 1988, 1999
    String[] sValues = new String[]{ "0", "1901", "2000", "2155", "00", "32", "69", "70", "88", "99" };
    for (String temp : sValues) {
      value = YearValue.getInstance(temp);
      System.out.println(value);
    }

    //error
    value = YearValue.getInstance(1000);
    System.out.println(value);
  }
}
