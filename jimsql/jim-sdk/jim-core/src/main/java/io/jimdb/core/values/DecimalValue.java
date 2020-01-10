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
import java.math.BigDecimal;

import io.jimdb.core.types.ValueType;
import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;

import com.google.common.base.Preconditions;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings("MDM_BIGDECIMAL_EQUALS")
public final class DecimalValue extends Value<DecimalValue> {
  private static volatile SoftReference<DecimalValue[]> decimalCache =
          new SoftReference(new DecimalValue[VALUE_CACHE_SIZE]);

  protected static final DecimalValue ZERO = new DecimalValue(BigDecimal.ZERO);

  protected static final DecimalValue ONE = new DecimalValue(BigDecimal.ONE);

  public static final DecimalValue MAX_VALUE = new DecimalValue(new BigDecimal(Long.toString(Long.MAX_VALUE)));
  public static final DecimalValue MIN_VALUE = new DecimalValue(new BigDecimal(Long.toString(Long.MIN_VALUE)));

  private final BigDecimal value;
  private int precision;
  private int scale;

  private DecimalValue(final BigDecimal value) {
    this.scale = value.scale();
    this.precision = value.precision();
    this.value = value;
  }

  private DecimalValue(final BigDecimal value, int precision, int scale) {
    this.value = value;
    this.precision = precision;
    this.scale = scale;
  }

  public static DecimalValue getInstance(final BigDecimal value, int precision, int scale) {
    return new DecimalValue(value, precision, scale);
  }

  public static DecimalValue getInstance(final BigDecimal value) {
    Preconditions.checkArgument(value != null, "Decimal Value must not be null");

//    if (BigDecimal.ZERO.compareTo(value) == 0) {
//      return ZERO;
//    }
//    if (BigDecimal.ONE.compareTo(value) == 0) {
//      return ONE;
//    }

    DecimalValue result = null;
    if (VALUE_CACHE_SIZE > 0) {
      DecimalValue[] cache;
      if ((cache = decimalCache.get()) == null) {
        cache = new DecimalValue[VALUE_CACHE_SIZE];
        decimalCache = new SoftReference(cache);
      }

      final int index = value.hashCode() & VALUE_CACHE_MASK;
      result = cache[index];
      if (result != null && value.equals(result.value)) {
        return result;
      }

      result = new DecimalValue(value);
      cache[index] = result;
    }

    return result == null ? new DecimalValue(value) : result;
  }

  @Override
  protected int compareToSafe(DecimalValue v2) {
    return value.compareTo(v2.value);
  }

  @Override
  protected Value plusSafe(DecimalValue v2) {
    return getInstance(value.add(v2.value));
  }

  @Override
  protected Value subtractSafe(DecimalValue v2) {
    return getInstance(value.subtract(v2.value));
  }

  @Override
  protected Value multiplySafe(DecimalValue v2) {
    return getInstance(value.multiply(v2.value));
  }

  @Override
  protected Value divideSafe(DecimalValue v2) {
    if (v2.value.signum() == 0) {
      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_DIVISION_BY_ZERO);
    }

    BigDecimal dec = value.divide(v2.value, value.scale(), BigDecimal.ROUND_HALF_DOWN);
    if (dec.signum() == 0) {
      dec = BigDecimal.ZERO;
    } else if (dec.scale() > 0) {
      if (!dec.unscaledValue().testBit(0)) {
        dec = dec.stripTrailingZeros();
      }
    }
    return getInstance(dec);
  }

  public BigDecimal getValue() {
    return value;
  }

  @Override
  public ValueType getType() {
    return ValueType.DECIMAL;
  }

  @Override
  public int signum() {
    return value.signum();
  }

  @Override
  public String getString() {
    return value.toPlainString();
  }

  @Override
  public byte[] toByteArray() {
    return value.unscaledValue().toByteArray();
  }

  @Override
  public boolean isMax() {
    return this == MAX_VALUE;
  }

  @Override
  public boolean isMin() {
    return this == MIN_VALUE;
  }

  public int getPrecision() {
    return precision;
  }

  public int getScale() {
    return scale;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DecimalValue that = (DecimalValue) o;
    return value.equals(that.value);
  }

  @Override
  public int hashCode() {
    return value.hashCode();
  }

}
