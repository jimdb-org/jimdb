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
package io.jimdb.common.utils.generator;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Connection ID generator
 */
public final class ConnectionIdGenerator {
  private static int current;
  private static final Lock LOCK = new ReentrantLock();

  private ConnectionIdGenerator() {
  }

  /**
   * Get next connection id.
   *
   * @return next connection id
   */
  public static int next() {
    LOCK.lock();
    try {
      if (current >= Integer.MAX_VALUE) {
        current = 0;
      }
      return ++current;
    } finally {
      LOCK.unlock();
    }
  }
}
