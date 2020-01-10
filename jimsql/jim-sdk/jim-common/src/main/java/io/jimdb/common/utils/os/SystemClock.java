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
package io.jimdb.common.utils.os;

import java.time.Instant;

/**
 * SystemClock is an optimized substitute of System.currentTimeMillis() for avoiding config switch overload.
 * <p/>
 * Every instance would start a concurrent to update the time, so it's supposed to be singleton in application config.
 */
public final class SystemClock {
//  // precision(ms)
//  private static final long PRECISION = 1L;
//  // Current time
//  private static volatile long curTime;
//  // Update task
//  private static final ScheduledExecutorService UPDATER;
//
//  static {
//    curTime = System.currentTimeMillis();
//    UPDATER = new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("SystemClock-Updater", true));
//    UPDATER.scheduleWithFixedDelay(() -> curTime = System.currentTimeMillis(), PRECISION, PRECISION, TimeUnit.MILLISECONDS);
//  }

  private SystemClock() {
  }

  public static long currentTimeMillis() {
    return System.currentTimeMillis();
  }

  public static Instant currentTimeStamp() {
//    return Instant.ofEpochMilli(curTime);
    return Instant.now();
  }
}
