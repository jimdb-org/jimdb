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
import java.nio.charset.StandardCharsets;

import io.jimdb.core.types.ValueType;

/**
 * @version V1.0
 */
public final class StringValue extends Value<StringValue> {
  protected static final StringValue EMPTY = new StringValue("");
  public static final StringValue MAX_VALUE = new StringValue("MAX_VALUE");
  public static final StringValue MIN_VALUE = new StringValue("MIN_VALUE");
  private static volatile SoftReference<StringValue[]> stringCache = new SoftReference(new StringValue[VALUE_CACHE_SIZE]);
  private final String value;

  private StringValue(final String value) {
    this.value = value;
  }

  public static StringValue getInstance(final String value) {
    if (value == null || value.length() == 0) {
      return EMPTY;
    }

    StringValue result = null;
    if (VALUE_CACHE_SIZE > 0 && value.length() <= BYTE_CACHE_THRESHOLD) {
      StringValue[] cache;
      if ((cache = stringCache.get()) == null) {
        cache = new StringValue[VALUE_CACHE_SIZE];
        stringCache = new SoftReference<>(cache);
      }

      final int index = value.hashCode() & VALUE_CACHE_MASK;
      result = cache[index];
      if (result != null && value.equals(result.value)) {
        return result;
      }

      result = new StringValue(value);
      cache[index] = result;
    }

    return result == null ? new StringValue(value) : result;
  }

  @Override
  protected int compareToSafe(StringValue v2) {
    return value.compareTo(v2.value);
  }

  public String getValue() {
    return value;
  }

  public int getLength() {
    return value == null ? 0 : value.length();
  }

  @Override
  public ValueType getType() {
    return ValueType.STRING;
  }

  @Override
  public String getString() {
    return value;
  }

  @Override
  public byte[] toByteArray() {
    return value.getBytes(StandardCharsets.UTF_8);
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
    StringValue that = (StringValue) o;
    return value.equals(that.value);
  }

  @Override
  public int hashCode() {
    return value.hashCode();
  }

}
