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
package io.jimdb.mysql.util;

import java.security.SecureRandom;

/**
 * @version V1.0
 */
public final class RandomGenerator {
  private static final byte[] BYTES = { 'a', 'b', 'e', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's',
          't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
          'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', };

  private static final SecureRandom RANDOM = new SecureRandom();

  private RandomGenerator() {
  }

  /**
   * Generate random bytes for a given length
   *
   * @param length length
   * @return random bytes
   */
  public static byte[] generate(final int length) {
    byte[] result = new byte[length];
    for (int i = 0; i < length; i++) {
      result[i] = BYTES[RANDOM.nextInt(BYTES.length)];
    }
    return result;
  }
}
