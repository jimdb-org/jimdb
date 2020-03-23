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
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import io.jimdb.common.utils.lang.ByteUtil;
import io.jimdb.core.types.ValueType;

import com.google.common.primitives.Longs;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings("EI_EXPOSE_REP")
public final class BinaryValue extends Value<BinaryValue> {
  private static volatile SoftReference<BinaryValue[]> binaryCache = new SoftReference(new BinaryValue[VALUE_CACHE_SIZE]);

  public static final BinaryValue EMPTY = new BinaryValue(new byte[]{});
  public static final BinaryValue MAX_VALUE = new BinaryValue(Longs.toByteArray(Long.MAX_VALUE));
  public static final BinaryValue MIN_VALUE = new BinaryValue(Longs.toByteArray(Long.MIN_VALUE));

  private final byte[] value;

  private BinaryValue(final byte[] value) {
    this.value = value;
  }

  public static BinaryValue getInstance(byte[] value) {
    if (value.length == 0) {
      return EMPTY;
    }
    return new BinaryValue(value);
  }

  @Override
  protected int compareToSafe(BinaryValue v2) {
    return ByteUtil.compareSigned(value, v2.value);
  }

  @Override
  // This function should only be used when calculating the fractions in histogram
  protected Value subtractSafe(BinaryValue v2) {
    DoubleValue converted1 = DoubleValue.getInstance(ByteBuffer.wrap(this.getValue()).getDouble());
    DoubleValue converted2 = DoubleValue.getInstance(ByteBuffer.wrap(v2.getValue()).getDouble());
    return converted1.subtractSafe(converted2);
  }

  public byte[] getValue() {
    return value;
  }

  @Override
  public ValueType getType() {
    return ValueType.BINARY;
  }

  @Override
  public String getString() {
    return new String(this.value, StandardCharsets.UTF_8);
  }

  @Override
  public byte[] toByteArray() {
    return value;
  }

  public BinaryValue prefixNext() {
    if (this.isMax()) {
      return this;
    }

    byte[] bytes = Arrays.copyOf(this.value, this.value.length);
    int i = bytes.length - 1;
    for (; i >= 0; i--) {
      bytes[i]++;
      if (bytes[i] != 0) {
        break;
      }
    }

    if (i == -1) {
      bytes = Arrays.copyOf(this.value, this.value.length + 1);
    }

    return BinaryValue.getInstance(bytes);
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
    BinaryValue that = (BinaryValue) o;
    return Arrays.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(value);
  }
}
