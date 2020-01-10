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

package io.jimdb.sql.optimizer.statistics;

import java.util.Objects;

import io.jimdb.core.values.Value;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * SampleItem is an item of sampled column value.
 */
@SuppressFBWarnings({ "UUF_UNUSED_FIELD", "URF_UNREAD_FIELD" })
public class SampleItem implements Comparable<SampleItem> {
  private long rowID; // row id of the sample
  private Value value; // sampled column value

  // The original position of this item in the SampleCollector before sorting. This
  // is used for computing correlation.
  private long ordinal;

  public SampleItem(Value value, long ordinal) {
    this.value = value;
    this.ordinal = ordinal;
  }

  public SampleItem(Value value, int ordinal, long rowID) {
    this.value = value;
    this.ordinal = ordinal;
    this.rowID = rowID;
  }

  long getOrdinal() {
    return ordinal;
  }

  public void setOrdinal(long ordinal) {
    this.ordinal = ordinal;
  }

  public Value getValue() {
    return value;
  }

  long getRowID() {
    return rowID;
  }

  @Override
  public int compareTo(SampleItem o) {
    return this.value.compareTo(null, o.value);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof SampleItem)) {
      return false;
    }

    SampleItem that = (SampleItem) o;
    return getOrdinal() == that.getOrdinal()
                   && getRowID() == that.getRowID()
                   && Objects.equals(getValue(), that.getValue());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getValue(), getOrdinal(), getRowID());
  }
}
