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

import java.util.ArrayList;
import java.util.List;

import io.jimdb.core.Session;
import io.jimdb.core.values.BinaryValue;
import io.jimdb.core.values.Value;

import com.google.common.primitives.Bytes;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

/**
 * Represent the start and end of multiple columns
 *
 * @version V1.0
 */
@SuppressFBWarnings("PL_PARALLEL_LISTS")
public class ValueRange {
  private List<Value> starts = new ArrayList<>();
  private List<Value> ends = new ArrayList<>();
  private boolean startInclusive = true;
  private boolean endInclusive = true;

  /* TODO refactor constructors */
  public ValueRange(Value start, Value end) {
    this.starts.add(start);
    this.ends.add(end);
  }

  public ValueRange(Point start, Point end) {
    this.starts.add(start.getValue());
    this.ends.add(end.getValue());
    this.startInclusive = start.isInclusive();
    this.endInclusive = end.isInclusive();
  }

  public ValueRange(List<Value> starts, List<Value> ends, boolean startInclusive, boolean endInclusive) {
    this.starts = starts;
    this.ends = ends;
    this.startInclusive = startInclusive;
    this.endInclusive = endInclusive;
  }

  private static BinaryValue encodeValues(List<Value> values) {
    if (values.isEmpty()) {
      return BinaryValue.getInstance(new byte[0]);
    }

    if (values.get(0).isMin()) {
      return BinaryValue.MIN_VALUE;
    }

    if (values.get(0).isMax()) {
      return BinaryValue.MAX_VALUE;
    }

    byte[] bytes = values.get(0).toByteArray();

    for (int i = 1; i < values.size(); i++) {
      Value v = values.get(i);
      if (v.isMax() && v.isNull()) {
        bytes = Bytes.concat(bytes, BinaryValue.MAX_VALUE.toByteArray());
      } else {
        bytes = Bytes.concat(bytes, v.toByteArray());
      }
    }

    return BinaryValue.getInstance(bytes);
  }

  public Value getFirstStart() {
    return starts.get(0);
  }

  public Value getFirstEnd() {
    return ends.get(0);
  }

  public List<Value> getStarts() {
    return starts;
  }

  public void setStarts(List<Value> starts) {
    this.starts = starts;
  }

  public List<Value> getEnds() {
    return ends;
  }

  public void setEnds(List<Value> ends) {
    this.ends = ends;
  }

  public boolean isStartInclusive() {
    return startInclusive;
  }

  public void setStartInclusive(boolean startInclusive) {
    this.startInclusive = startInclusive;
  }

  public boolean isEndInclusive() {
    return endInclusive;
  }

  public void setEndInclusive(boolean endInclusive) {
    this.endInclusive = endInclusive;
  }

  /**
   * Consolidate the list of ranges into a single binary format range
   *
   * @param session session used for comparison
   * @return tuple of the consolidated range
   */
  public Tuple2<BinaryValue, BinaryValue> consolidate() {
    BinaryValue lower = encodeValues(this.starts);
    BinaryValue upper = encodeValues(this.ends);
    return Tuples.of(lower, upper);
  }

  public boolean isPoint(Session session) {

    // this should never happen
    if (starts.size() != ends.size()) {
      return false;
    }

    final int size = starts.size();
    for (int i = 0; i < size; i++) {
      Value start = starts.get(i);
      Value end = ends.get(i);
      if (start.isMin() || end.isMax()) {
        return false;
      }

      // TODO add check for start == MinNotNull && end == MaxValue

      if (start.isNull()) {
        return false;
      }

      if (start.compareTo(session, end) != 0) {
        return false;
      }
    }

    return this.startInclusive && this.endInclusive;
  }

  public boolean isNull() {
    return this.getStarts().size() == 1 && this.getEnds().size() == 1
                   && this.getStarts().get(0).isNull() && this.getEnds().get(0).isNull();
  }
  public boolean isFullRange() {
    if (getFirstStart().isMin() && getFirstEnd().isMax()) {
      return true;
    }
    return false;
  }

  @Override
  public String toString() {
    return "ValueRange{"
            + "starts=" + starts
            + ", ends=" + ends
            + ", startInclusive=" + startInclusive
            + ", endInclusive=" + endInclusive
            + '}';
  }
}
