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

import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OperatingSystemInfo captures information about JVM and its running environment.
 *
 */
public final class OperatingSystemInfo {
  private static final Logger LOG = LoggerFactory.getLogger(OperatingSystemInfo.class);

  private static final Pattern MAX_DIRECT_MEMORY_MATCHER = Pattern.compile("-XX:MaxDirectMemorySize=(?<amount>\\d+)(?<unit>[kKmMgGtT]?)");

  private static final long MAX_DIRECT_MEMORY = initMaxDirectMemory();

  private OperatingSystemInfo() {
  }

  public static String getPid() {
    String runName = ManagementFactory.getRuntimeMXBean().getName();
    return runName.substring(0, runName.indexOf('@'));
  }

  public static long getMaxDirectMemory() {
    return MAX_DIRECT_MEMORY;
  }

  private static long initMaxDirectMemory() {
    try {
      return invokeMaxDirectMemory("sun.misc.VM");
    } catch (ReflectiveOperationException ignored) {
    }

    try {
      return invokeMaxDirectMemory("jdk.internal.misc.VM");
    } catch (ReflectiveOperationException ignored) {
    }

    final List<String> args = ManagementFactory.getRuntimeMXBean().getInputArguments();
    final long maxDirectMemory = matchMaxDirectMemory(args);
    return maxDirectMemory != 0 ? maxDirectMemory : Runtime.getRuntime().maxMemory();
  }

  private static long invokeMaxDirectMemory(String className) throws ReflectiveOperationException {
    Class<?> clazz = Class.forName(className, true, ClassLoader.getSystemClassLoader());
    Method m = clazz.getDeclaredMethod("maxDirectMemory");
    return ((Number) m.invoke(null)).longValue();
  }

  private static long matchMaxDirectMemory(final List<String> args) {
    try {
      for (String arg : args) {
        final Matcher matcher = MAX_DIRECT_MEMORY_MATCHER.matcher(arg);
        if (matcher.matches()) {
          long radix = 1;
          switch (matcher.group("unit")) {
            case "t":
              radix *= 1024 * 1024 * 1024 * 1024;
              break;
            case "T":
              radix *= 1024 * 1024 * 1024 * 1024;
              break;
            case "g":
              radix *= 1024 * 1024 * 1024;
              break;
            case "G":
              radix *= 1024 * 1024 * 1024;
              break;
            case "m":
              radix *= 1024 * 1024;
              break;
            case "M":
              radix *= 1024 * 1024;
              break;
            case "k":
              radix *= 1024;
              break;
            case "K":
              radix *= 1024;
              break;
            default:
              break;
          }

          return Long.parseLong(matcher.group("amount")) * radix;
        }
      }
    } catch (NumberFormatException e) {
      LOG.error("Error occurs when extracting the value of MaxDirectMemorySize.", e);
    }

    return 0;
  }
}
