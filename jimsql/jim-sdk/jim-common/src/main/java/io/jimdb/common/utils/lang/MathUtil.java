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
package io.jimdb.common.utils.lang;

/**
 * Math related utils
 */
public final class MathUtil {
  private MathUtil() {
  }

  /**
   * Rounds up the value to the nearest power of two.
   *
   * @param v given value
   * @return result of power of two
   */
  public static int powerTwo(int v) {
    if (v == 0 || v == 1) {
      return v + 1;
    }

    int highestBit = Integer.highestOneBit(v);
    if (highestBit == v) {
      return v;
    } else {
      return highestBit << 1;
    }
  }
}
