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
import java.math.BigInteger;
import java.util.Objects;

import io.jimdb.core.types.ValueType;
import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * The largest Long value for unsigned, as a BigInteger.
 *
 * @version V1.0
 */
@SuppressFBWarnings("FCCD_FIND_CLASS_CIRCULAR_DEPENDENCY")
public final class UnsignedLongValue extends Value<UnsignedLongValue> {
  public static final BigInteger MAX_BIGINT = new BigInteger("18446744073709551615");
  private static volatile SoftReference<UnsignedLongValue[]> bigintCache =
      new SoftReference(new UnsignedLongValue[VALUE_CACHE_SIZE]);

  public static final UnsignedLongValue MAX_VALUE = new UnsignedLongValue(MAX_BIGINT);

  public static final UnsignedLongValue MIN_VALUE = new UnsignedLongValue(new BigInteger("0"));

  private final BigInteger value;

  private UnsignedLongValue(final BigInteger value) {
    this.value = value;
  }

  public static UnsignedLongValue getInstance(BigInteger value) {
    UnsignedLongValue result = null;

    if (VALUE_CACHE_SIZE > 0) {
      UnsignedLongValue[] cache;
      if ((cache = bigintCache.get()) == null) {
        cache = new UnsignedLongValue[VALUE_CACHE_SIZE];
        bigintCache = new SoftReference<>(cache);
      }

      final int index = value.hashCode() & VALUE_CACHE_MASK;
      result = cache[index];
      if (result != null && value.equals(result.value)) {
        return result;
      }

      result = new UnsignedLongValue(value);
      cache[index] = result;
    }

    return result == null ? new UnsignedLongValue(value) : result;
  }

  @Override
  public int compareToSafe(UnsignedLongValue v2) {
    return this.value.compareTo(v2.value);
  }

  @Override
  protected Value plusSafe(UnsignedLongValue v2) {
    BigInteger result = value.add(v2.value);
    if (result.compareTo(MAX_BIGINT) > 0) {
      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OVER_FLOW, "UNSIGNBIGINT", String.format("(%s"
          + " + %s)", value.toString(), v2.value.toString()));
    }
    return getInstance(result);
  }

  @Override
  protected Value subtractSafe(UnsignedLongValue v2) {
    BigInteger result = value.subtract(v2.value);
    if (result.compareTo(MAX_BIGINT) > 0) {
      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OVER_FLOW, "UNSIGNBIGINT", String.format("(%s"
          + " - %s)", value.toString(), v2.value.toString()));
    }
    return getInstance(result);
  }

  @Override
  protected Value multiplySafe(UnsignedLongValue v2) {
    BigInteger result = value.multiply(v2.value);
    if (result.compareTo(MAX_BIGINT) > 0) {
      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OVER_FLOW, "UNSIGNBIGINT", String.format("(%s"
          + " * %s)", value.toString(), v2.value.toString()));
    }
    return getInstance(result);
  }

  @Override
  protected Value divideSafe(UnsignedLongValue v2) {
    if (v2.value.signum() == 0) {
      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_DIVISION_BY_ZERO);
    }

    return getInstance(value.divide(v2.value));
  }

  @Override
  protected Value modSafe(UnsignedLongValue v2) {
    if (v2.value.signum() == 0) {
      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_DIVISION_BY_ZERO);
    }

    return getInstance(value.mod(v2.value));
  }

  public BigInteger getValue() {
    return value;
  }

  @Override
  public int signum() {
    return value.signum();
  }

  @Override
  public ValueType getType() {
    return ValueType.UNSIGNEDLONG;
  }

  @Override
  public String getString() {
    return value.toString();
  }

  @Override
  public byte[] toByteArray() {
    return value.toByteArray();
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
    UnsignedLongValue that = (UnsignedLongValue) o;
    return Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return value.hashCode();
  }
}
