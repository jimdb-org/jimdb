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
package io.jimdb.core.expression.aggregate;

import java.util.HashSet;
import java.util.Set;

import io.jimdb.core.Session;
import io.jimdb.core.values.Value;

/**
 * @version V1.0
 */
public class AvgCell implements Cell {
  private Value count;
  private Value sum;
  private boolean hasDistinct;
  private Set distinctSet;

  public AvgCell(Value count, Value sum, boolean hasDistinct) {
    this.count = count;
    this.sum = sum;
    this.hasDistinct = hasDistinct;
    if (hasDistinct) {
      distinctSet = new HashSet();
    }
  }

  public Value getCount() {
    return count;
  }

  public void setCount(Value count) {
    this.count = count;
  }

  public Value getSum() {
    return sum;
  }

  public void setSum(Value sum) {
    this.sum = sum;
  }

  @Override
  public Value getValue() {
    // throw exception: do not support
    return null;
  }

  @Override
  public void setValue(Value value) {
    // throw exception: do not support
  }

  @Override
  public Cell plus(Session session, Value src) {
    // throw exception: do not support
    return null;
  }

  @Override
  public Cell plus(Session session, Cell src) {
    AvgCell srcAvg = (AvgCell) src;
    if (srcAvg.getSum().isNull() || srcAvg.getCount().isNull()) {
      return this;
    }

    if (this.getSum().isNull() || this.getCount().isNull()) {
      return src;
    }
    this.count = this.count.plus(session, srcAvg.getCount());
    this.sum = this.sum.plus(session, srcAvg.getSum());
    return this;
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
