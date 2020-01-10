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
package io.jimdb.core.values;

import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;

import io.jimdb.core.types.ValueType;
import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;

/**
 * @version V1.0
 */
public final class LongValue extends Value<LongValue> {
  private static final int STATIC_CACHE_SIZE = 1024;
  private static final LongValue[] STATIC_CACHE;

  public static final LongValue MAX_VALUE = new LongValue(Long.MAX_VALUE);
  public static final LongValue MIN_VALUE = new LongValue(Long.MIN_VALUE);

  private static volatile SoftReference<LongValue[]> longCache = new SoftReference<>(new LongValue[VALUE_CACHE_SIZE]);

  private final long value;

  static {
    STATIC_CACHE = new LongValue[STATIC_CACHE_SIZE];
    for (int i = 0; i < STATIC_CACHE_SIZE; i++) {
      STATIC_CACHE[i] = new LongValue(i);
    }
  }

  private LongValue(final long value) {
    this.value = value;
  }

  public long getValue() {
    return value;
  }

  public static LongValue getInstance(long value) {
    if (value >= 0 && value < STATIC_CACHE_SIZE) {
      return STATIC_CACHE[(int) value];
    }

    LongValue result = null;
    if (VALUE_CACHE_SIZE > 0) {
      LongValue[] cache;
      if ((cache = longCache.get()) == null) {
        cache = new LongValue[VALUE_CACHE_SIZE];
        longCache = new SoftReference<>(cache);
      }

      final int index = ((int) (value ^ (value >> 32))) & VALUE_CACHE_MASK;
      result = cache[index];
      if (result != null && value == result.value) {
        return result;
      }

      result = new LongValue(value);
      cache[index] = result;
    }

    return result == null ? new LongValue(value) : result;
  }

  @Override
  public ValueType getType() {
    return ValueType.LONG;
  }

  @Override
  public int signum() {
    return Long.signum(value);
  }

  @Override
  public int compareToSafe(LongValue v2) {
    return (value < v2.value) ? -1 : ((value == v2.value) ? 0 : 1);
  }

  @Override
  protected Value plusSafe(LongValue v2) {
    final long result = value + v2.value;
    if (((value ^ result) & (v2.value ^ result)) < 0) {
      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OVER_FLOW, "BIGINT", String.format("(%s + %s)", String.valueOf(value), String.valueOf(v2.value)));
    }

    return getInstance(result);
  }

  @Override
  protected Value subtractSafe(LongValue v2) {
    final long result = value - v2.value;
    if (((value ^ v2.value) & (value ^ result)) < 0) {
      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_STD_OUT_OF_RANGE_ERROR, Long.toString(value));
    }

    return getInstance(result);
  }

  @Override
  protected Value multiplySafe(LongValue v2) {
    final long result = value * v2.value;
    if ((Math.abs(value) | Math.abs(v2.value)) >>> 31 != 0 && v2.value != 0
        && (result / v2.value != value || value == Long.MIN_VALUE && v2.value == -1)) {
      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_STD_OUT_OF_RANGE_ERROR, Long.toString(value));
    }
    return getInstance(result);
  }

  @Override
  protected Value divideSafe(LongValue v2) {
    if (v2.value == 0) {
      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_DIVISION_BY_ZERO);
    }

    if (value == Long.MIN_VALUE && v2.value == -1) {
      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_STD_OUT_OF_RANGE_ERROR, Long.toString(value));
    }
    return getInstance(value / v2.value);
  }

  @Override
  protected Value modSafe(LongValue v2) {
    if (v2.value == 0) {
      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_DIVISION_BY_ZERO);
    }
    return getInstance(value % v2.value);
  }

  @Override
  public String getString() {
    return Long.toString(value);
  }

  @Override
  public byte[] toByteArray() {
    ByteBuffer byteBuffer = ByteBuffer.allocate(Long.BYTES);
    byteBuffer.putLong(value);
    return byteBuffer.array();
  }

  @Override
  public boolean isMax() {
    return this == MAX_VALUE;
  }

  @Override
  public boolean isMin() {
    return this == MIN_VALUE;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    LongValue that = (LongValue) o;
    return value == that.value;
  }

  @Override
  public int hashCode() {
    return (int) (value ^ (value >>> 32));
  }
}
