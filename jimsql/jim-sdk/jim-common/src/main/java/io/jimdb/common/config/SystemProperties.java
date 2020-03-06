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
package io.jimdb.common.config;

import java.util.Properties;

import io.netty.util.ResourceLeakDetector;

import org.apache.commons.lang3.StringUtils;

/**
 * System properties
 */
public final class SystemProperties {
  private static final String REACTOR_DEBUG = "jim.reactor.debug";
  // 0:DISABLED,1:SIMPLE,2:ADVANCED,3:PARANOID
  private static final String NETTY_LEAK_LEVEL = "jim.netty.leak";

  public static final String VALUE_CACHE_SIZE = "jim.value.cache.size";

  public static final String VALUE_CACHE_THRESHOLD = "jim.value.cache.threshold";

  private static final String AGG_EXECUTE_THREADS = "jim.aggexecute.threads";

  private static boolean reactorDebug;
  // Netty leak level, Default is SIMPLE
  private static ResourceLeakDetector.Level leakLevel;

  private static int valueCacheSize;
  private static int valueCacheThreshold;
  private static int aggThreads;

  private SystemProperties() {
  }

  public static void init(Properties props) {
    props = props == null ? new Properties() : props;
    String value = props.getProperty(VALUE_CACHE_SIZE);
    if (StringUtils.isEmpty(value)) {
      value = System.getProperty(VALUE_CACHE_SIZE, "1024");
    }
    valueCacheSize = Integer.parseInt(value);

    value = props.getProperty(VALUE_CACHE_THRESHOLD);
    if (StringUtils.isEmpty(value)) {
      value = System.getProperty(VALUE_CACHE_THRESHOLD, "4096");
    }
    valueCacheThreshold = Integer.parseInt(value);

    value = props.getProperty(AGG_EXECUTE_THREADS);
    if (StringUtils.isEmpty(value)) {
      value = System.getProperty(AGG_EXECUTE_THREADS, "4");
    }
    aggThreads = Integer.parseInt(value);

    value = props.getProperty(REACTOR_DEBUG);
    if (StringUtils.isEmpty(value)) {
      value = System.getProperty(REACTOR_DEBUG, "false");
    }
    reactorDebug = Boolean.parseBoolean(value);

    value = props.getProperty(NETTY_LEAK_LEVEL);
    if (StringUtils.isEmpty(value)) {
      value = System.getProperty(NETTY_LEAK_LEVEL, "1");
    }
    int level = Integer.parseInt(value);
    switch (level) {
      case 0:
        leakLevel = ResourceLeakDetector.Level.DISABLED;
        break;
      case 1:
        leakLevel = ResourceLeakDetector.Level.SIMPLE;
        break;
      case 2:
        leakLevel = ResourceLeakDetector.Level.ADVANCED;
        break;
      case 3:
        leakLevel = ResourceLeakDetector.Level.PARANOID;
        break;
      default:
        leakLevel = ResourceLeakDetector.Level.SIMPLE;
        break;
    }
  }

  public static boolean getReactorDebug() {
    return reactorDebug;
  }

  public static ResourceLeakDetector.Level getLeakLevel() {
    return leakLevel;
  }

  public static int getValueCacheSize() {
    return valueCacheSize;
  }

  public static int getByteCacheThreshold() {
    return valueCacheThreshold;
  }

  public static int getAggExecuteThreads() {
    return aggThreads;
  }
}
