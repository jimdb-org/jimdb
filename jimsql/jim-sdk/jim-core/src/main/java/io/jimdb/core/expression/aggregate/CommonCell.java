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
package io.jimdb.core.expression.aggregate;

import java.util.HashSet;
import java.util.Set;

import io.jimdb.core.Session;
import io.jimdb.core.values.NullValue;
import io.jimdb.core.values.Value;

/**
 * @version V1.0
 */
public class CommonCell implements Cell {

  private Value value;
  // firstValue: used by distinctValue
  private boolean isFirstValue = false;

  private boolean hasDistinct = false;
  private Set distinctSet;

  public CommonCell(Value value) {
    this.value = value;
  }

  public CommonCell(Value value, boolean hasDistinct) {
    this.value = value;
    this.hasDistinct = hasDistinct;
    if (hasDistinct) {
      distinctSet = new HashSet();
    }
  }

  @Override
  public Cell plus(Session session, Cell src) {
    return plus(session, src.getValue());
  }

  @Override
  public Value getValue() {
    return value;
  }

  @Override
  public Cell plus(Session session, Value src) {
    if (src == null || src.isNull()) {
      return this;
    }
    if (this.value == NullValue.getInstance()) {
      this.value = src;
    } else {
      this.value = this.value.plus(session, src);
    }
    return this;
  }

  @Override
  public void setValue(Value value) {
    this.value = value;
  }

  public boolean isFirstValue() {
    return isFirstValue;
  }

  public void setFirstValue(boolean firstValue) {
    isFirstValue = firstValue;
  }

  @Override
  public Set getDistinctSet() {
    return distinctSet;
  }

  @Override
  public boolean hasDistinct() {
    return hasDistinct;
  }
}
