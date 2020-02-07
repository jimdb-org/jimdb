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

import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.core.types.Types;
import io.jimdb.core.types.ValueType;

import com.google.common.base.Preconditions;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings("MDM_BIGDECIMAL_EQUALS")
public final class DecimalValue extends Value<DecimalValue> {
  private static volatile SoftReference<DecimalValue[]> decimalCache = new SoftReference(new DecimalValue[VALUE_CACHE_SIZE]);

  protected static final DecimalValue ZERO = new DecimalValue(BigDecimal.ZERO, BigDecimal.ZERO.precision(), BigDecimal.ZERO.scale());
  protected static final DecimalValue ONE = new DecimalValue(BigDecimal.ONE, BigDecimal.ONE.precision(), BigDecimal.ONE.scale());

  public static final DecimalValue MAX_VALUE = new DecimalValue(Types.MAX_DEC_SIGNEDLONG, Types.MAX_DEC_SIGNEDLONG.precision(), Types.MAX_DEC_SIGNEDLONG.scale());
  public static final DecimalValue MIN_VALUE = new DecimalValue(Types.MIN_DEC_SIGNEDLONG, Types.MIN_DEC_SIGNEDLONG.precision(), Types.MIN_DEC_SIGNEDLONG.scale());

  /**
   *  In division performed with /, the scale of the result when using two exact-value operands is the scale of the first
   *  operand plus the value of the div_precision_increment system variable (which is 4 by default).
   *  For example, the result of the expression 5.05 / 0.014 has a scale of six decimal places (360.714286).
   *  @See https://dev.mysql.com/doc/refman/5.7/en/arithmetic-functions.html}
   */
  public static final int DIV_FRAC_INCR = 4;
  private final BigDecimal value;
  private int precision;
  private int scale;

  private DecimalValue(final BigDecimal value, int precision, int scale) {
    this.value = value;
    this.precision = precision;
    this.scale = scale;
  }

  public static DecimalValue getInstance(final BigDecimal value, int precision, int scale) {
    Preconditions.checkArgument(value != null, "Decimal Value must not be null");

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

      result = new DecimalValue(value, precision, scale);
      cache[index] = result;
    }

    return result == null ? new DecimalValue(value, precision, scale) : result;
  }

  @Override
  protected int compareToSafe(DecimalValue v2) {
    return value.compareTo(v2.value);
  }

  @Override
  protected Value plusSafe(DecimalValue v2) {
    return getInstance(value.add(v2.value), Math.max(precision, v2.precision), Math.max(scale, v2.scale));
  }

  @Override
  protected Value subtractSafe(DecimalValue v2) {
    return getInstance(value.subtract(v2.value), Math.max(precision, v2.precision), Math.max(scale, v2.scale));
  }

  @Override
  protected Value multiplySafe(DecimalValue v2) {
    return getInstance(value.multiply(v2.value), Math.max(precision, v2.precision), Math.max(scale, v2.scale));
  }

  @Override
  protected Value divideSafe(DecimalValue v2) {
    if (v2.value.signum() == 0) {
      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_DIVISION_BY_ZERO);
    }

    int prec = Math.max(value.precision(), v2.value.precision());
    int scale = value.scale();
    BigDecimal dec = value.divide(v2.value, scale + DIV_FRAC_INCR, BigDecimal.ROUND_HALF_EVEN);
    if (dec.signum() == 0) {
      dec = BigDecimal.ZERO;
    }
    return getInstance(dec, prec, scale);
  }

  @Override
  protected Value divSafe(DecimalValue v2) {
    if (v2.value.signum() == 0) {
      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_DIVISION_BY_ZERO);
    }

    BigDecimal dec = value.divideToIntegralValue(v2.value);
    if (dec.signum() == 0) {
      dec = BigDecimal.ZERO;
    } else if (dec.scale() > 0) {
      if (!dec.unscaledValue().testBit(0)) {
        dec = dec.stripTrailingZeros();
      }
    }
    return getInstance(dec, dec.precision(), 0);
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
