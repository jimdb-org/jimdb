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

import io.jimdb.core.types.ValueType;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings("FCCD_FIND_CLASS_CIRCULAR_DEPENDENCY")
public final class NullValue extends Value<NullValue> {
  private static final NullValue INSTANCE = new NullValue();

  NullValue() {
  }

  public static NullValue getInstance() {
    return INSTANCE;
  }

  @Override
  public boolean isNull() {
    return true;
  }

  @Override
  public ValueType getType() {
    return ValueType.NULL;
  }

  @Override
  public String getString() {
    return "";
  }

  @Override
  public byte[] toByteArray() {
    return null;
  }

  @Override
  public int hashCode() {
    return INSTANCE.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return obj == this;
  }

}
