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

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

import io.jimdb.core.types.ValueType;
import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;

import org.apache.commons.lang3.StringUtils;

import com.alibaba.fastjson.JSON;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
public final class JsonValue extends Value {
  protected static final JsonValue NULL = new JsonValue("null");

  protected static final JsonValue TRUE = new JsonValue("true");

  protected static final JsonValue FALSE = new JsonValue("false");

  protected static final JsonValue ZERO = new JsonValue("0");

  public static final JsonValue MAX_VALUE = new JsonValue("MAX_VALUE");

  public static final JsonValue MIN_VALUE = new JsonValue("MIN_VALUE");

  private final String value;

  private JsonValue(final String value) {
    this.value = value;
  }

  public static JsonValue getInstance(final boolean value) {
    return value ? TRUE : FALSE;
  }

  @SuppressFBWarnings("ITU_INAPPROPRIATE_TOSTRING_USE")
  public static JsonValue getInstance(final BigDecimal value) {
    String str = null;
    if (value != null) {
      str = value.toString();
      int idx = str.indexOf('E');
      if (idx >= 0 && str.charAt(++idx) == '+') {
        final int length = str.length();
        str = new StringBuilder(length - 1).append(str, 0, idx).append(str, idx + 1, length).toString();
      }
    }

    return getInstance(str);
  }

  public static JsonValue getInstance(final String value) {
    if (StringUtils.isBlank(value)) {
      return NULL;
    }

    if (!JSON.isValid(value)) {
      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_INVALID_JSON_TEXT);
    }

    switch (value.length()) {
      case 1:
        if ("0".equals(value)) {
          return ZERO;
        }
        break;
      case 4:
        if ("true".equalsIgnoreCase(value)) {
          return TRUE;
        } else if ("null".equalsIgnoreCase(value)) {
          return NULL;
        }
        break;
      case 5:
        if ("false".equalsIgnoreCase(value)) {
          return FALSE;
        }
    }
    return new JsonValue(value);
  }

  public String getValue() {
    return value;
  }

  @Override
  public ValueType getType() {
    return ValueType.JSON;
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
    JsonValue that = (JsonValue) o;
    return value.equals(that.value);
  }

  @Override
  public int hashCode() {
    return value.hashCode();
  }
}
