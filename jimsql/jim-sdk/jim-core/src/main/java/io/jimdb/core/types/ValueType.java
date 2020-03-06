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
package io.jimdb.core.types;

/**
 * @version V1.0
 */
public enum ValueType {
  NULL(0),
  LONG(1),
  UNSIGNEDLONG(2),
  DOUBLE(3),
  DECIMAL(4),
  STRING(5),
  BINARY(6),
  DATE(7),
  TIME(8),
  YEAR(9),
  //  BOOLEAN(10),
  JSON(50);

  private final int value;

  ValueType(final int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }

  public boolean isString() {
    return this == STRING || this == DATE || this == TIME || this == BINARY || this == YEAR || this == JSON;
  }
}
