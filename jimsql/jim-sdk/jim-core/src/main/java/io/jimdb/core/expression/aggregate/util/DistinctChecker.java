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
package io.jimdb.core.expression.aggregate.util;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @version V1.0
 */
public class DistinctChecker {
  private ConcurrentMap<String, Boolean> maps;

  public DistinctChecker() {
    maps = new ConcurrentHashMap<>();
  }

  /**
   * put a val if not exists
   *
   * @param val key of distinct value
   * @return true exists
   */
  public boolean checkOrPut(String val) {
    return maps.putIfAbsent(val, Boolean.TRUE) != null;
  }

  public Object size() {
    return maps.size();
  }
}
