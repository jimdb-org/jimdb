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
public final class DoubleValue extends Value<DoubleValue> {
  private static volatile SoftReference<DoubleValue[]> doubleCache =
          new SoftReference(new DoubleValue[VALUE_CACHE_SIZE]);

  protected static final DoubleValue ZERO = new DoubleValue(0.0d);

  protected static final DoubleValue ONE = new DoubleValue(1.0d);

  protected static final DoubleValue NAN = new DoubleValue(Double.NaN);

  public static final DoubleValue MAX_VALUE = new DoubleValue(Double.MAX_VALUE);

  public static final DoubleValue MIN_VALUE = new DoubleValue(Double.MIN_VALUE);

  private final double value;

  private DoubleValue(final double value) {
    this.value = value;
  }

  public static DoubleValue getInstance(final double value) {
    if (Double.isNaN(value)) {
      return NAN;
    }
    if (value == 0.0d) {
      return ZERO;
    }
    if (value == 1.0d) {
      return ONE;
    }

    DoubleValue result = null;
    if (VALUE_CACHE_SIZE > 0) {
      DoubleValue[] cache;
      if ((cache = doubleCache.get()) == null) {
        cache = new DoubleValue[VALUE_CACHE_SIZE];
        doubleCache = new SoftReference<>(cache);
      }

      final long hash = Double.doubleToRawLongBits(value);
      final int index = ((int) (hash ^ (hash >>> 32))) & VALUE_CACHE_MASK;
      result = cache[index];
      if (result != null && value == result.value) {
        return result;
      }

      result = new DoubleValue(value);
      cache[index] = result;
    }

    return result == null ? new DoubleValue(value) : result;
  }

  @Override
  protected int compareToSafe(DoubleValue v2) {
    return (value < v2.value) ? -1 : ((value == v2.value) ? 0 : 1);
  }

  @Override
  protected Value plusSafe(DoubleValue v2) {
    if ((value > 0 && v2.value > Double.MAX_VALUE - value) || (value < 0 && v2.value < -Double.MAX_VALUE - value)) {
      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OVER_FLOW, "DOUBLE", String.format("(%s + %s)", String.valueOf(value), String.valueOf(v2.value)));
    }
    return getInstance(value + v2.value);
  }

  @Override
  protected Value subtractSafe(DoubleValue v2) {
    return getInstance(value - v2.value);
  }

  @Override
  protected Value multiplySafe(DoubleValue v2) {
    return getInstance(value * v2.value);
  }

  @Override
  protected Value divideSafe(DoubleValue v2) {
    if (v2.value == 0.0d) {
      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_DIVISION_BY_ZERO);
    }
    return getInstance(value / v2.value);
  }

  @Override
  protected Value modSafe(DoubleValue v2) {
    if (v2.value == 0.0d) {
      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_DIVISION_BY_ZERO);
    }
    return DoubleValue.getInstance(value % v2.value);
  }

  public double getValue() {
    return value;
  }

  @Override
  public ValueType getType() {
    return ValueType.DOUBLE;
  }

  @Override
  public int signum() {
    return value == 0.0d ? 0 : (value < 0.0d ? -1 : 1);
  }

  @Override
  public String getString() {
    return Double.toString(value);
  }

  @Override
  public byte[] toByteArray() {
    ByteBuffer byteBuffer = ByteBuffer.allocate(Double.BYTES);
    byteBuffer.putDouble(value);
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
    DoubleValue that = (DoubleValue) o;
    return value == that.value;
  }

  @Override
  public int hashCode() {
    return Double.hashCode(value);
  }
}
