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
package io.jimdb.test;

import java.lang.reflect.InvocationTargetException;

/**
 * @version V1.0
 */
public final class TestUtil {
  private TestUtil() {
  }

  public static void rethrow(Throwable e) throws RuntimeException {
    if (e instanceof InvocationTargetException) {
      e = e.getCause();
    }

    if (e instanceof RuntimeException) {
      throw (RuntimeException) e;
    }
    throw new RuntimeException(e);
  }

  public static String linuxString(String str) {
    return str.replaceAll("\r\n", "\n");
  }
}
