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
package io.jimdb.core.expression;

import io.jimdb.core.Session;
import io.jimdb.core.types.ValueType;
import io.jimdb.core.values.Value;
import io.jimdb.core.values.ValueConvertor;
import io.jimdb.pb.Metapb.SQLType;

/**
 * Point is a data type that represents the isStart and end points of a range (interval).
 *
 */
public class Point {

  private Value value;
  private boolean inclusive = true;
  private boolean isStart = true;

  public Point(Value value, boolean isStart) {
    this.value = value;
    this.isStart = isStart;
  }

  public Point(Value value, boolean isStart, boolean inclusive) {
    this.value = value;
    this.inclusive = inclusive;
    this.isStart = isStart;
  }

  public Value getValue() {
    return value;
  }

  public boolean isInclusive() {
    return inclusive;
  }

  public boolean isStart() {
    return isStart;
  }

  boolean isLessThan(Session session, Point b) {
    int cmp = this.getValue().compareTo(session, b.getValue());
    if (cmp != 0) {
      return cmp < 0;
    }
    return this.rangePointEqualValueLess(b);
  }

  private boolean rangePointEqualValueLess(Point other) {
    if (this.isStart() && other.isStart()) {

      // both are starts
      return this.isInclusive() && !other.isInclusive();
    } else if (this.isStart()) {

      // this is start and other is end
      return this.isInclusive() && other.isInclusive();
    } else if (other.isStart()) {

      // this is end and other is start
      return !this.isInclusive() || !other.isInclusive();
    }

    // this excludes equal && other includes equal -> this is lessThan other
    return !this.isInclusive() && other.isInclusive();
  }

  // Convert the given point to a specific type
  public Point castType(Session session, SQLType type, ValueType valueType) {

    // TODO if the point's value is 'max' value or 'min not null' value we should simply return itself
    if (this.value.isMin() || this.value.isMax()) {
      return this;
    }

    Value originalValue = this.value;
    Value convertedValue = ValueConvertor.convertType(session, originalValue, valueType, type);

    final int compared = originalValue.compareTo(session, convertedValue);
    if (compared == 0) {
      return new Point(convertedValue, isStart, inclusive);
    }

    boolean isInclusive = inclusive;

    if (isStart && !inclusive && compared < 0) {
      // e.g. "a > 1.99" convert to "a >= 2"
      isInclusive = false;
    }

    if (isStart && !inclusive && compared > 0) {
      // e.g. "a >= 1.11 convert to "a > 1"
      isInclusive = true;
    }

    if (!isStart && !inclusive && compared > 0) {
      // e.g. "a < 1.11" convert to "a <= 1
      isInclusive = false;
    }

    if (!isStart && !inclusive && compared < 0) {
      // e.g. "a <= 1.99" convert to "a < 2"
      isInclusive = true;
    }

    return new Point(convertedValue, isStart, isInclusive);
  }
}
