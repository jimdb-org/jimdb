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


package io.jimdb.sql.optimizer.statistics;

import java.util.List;

import io.jimdb.core.expression.ValueRange;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;


/**
 *
 */
@SuppressFBWarnings({ "UUF_UNUSED_FIELD" })
public class Selectivity {
  private long id;

  private SelectivityType type;

  private long mask; // the ith bit indicates whether the ith expression is covered by this index/column.

  private double value; // the actual selectivity value

  private List<ValueRange> ranges; // all the ranges for the selected columns

  private int numColumns; // the number of columns contained in the index or column (which is always 1)

  // whether the bit in the mask is for a full coverage or partial coverage.
  // This filed can only be true when the condition is a DNF form on index but cannot be fully extracted as an access condition
  boolean isPartialCoverage;

  public Selectivity(long id, SelectivityType type, long mask, List<ValueRange> ranges, int numColumns, double value, boolean isPartialCoverage) {
    this.id = id;
    this.type = type;
    this.mask = mask;
    this.ranges = ranges;
    this.numColumns = numColumns;
    this.value = value;
    this.isPartialCoverage = isPartialCoverage;
  }

  public Selectivity(SelectivityType type, long mask, int numColumns) {
    this.type = type;
    this.mask = mask;
    this.numColumns = numColumns;
  }

  public long getMask() {
    return mask;
  }

  public long getId() {
    return id;
  }

  public double getValue() {
    return value;
  }

  public void setValue(double value) {
    this.value = value;
  }

  public List<ValueRange> getRanges() {
    return ranges;
  }

  public boolean isPartialCoverage() {
    return isPartialCoverage;
  }

  public SelectivityType getType() {
    return type;
  }

  public int getNumColumns() {
    return numColumns;
  }
}

/**
 * Type of the selectivity
 */
enum SelectivityType {
  INDEX(0),
  PRIMARY_KEY(1),
  COLUMN(2);

  private final int value;

  SelectivityType(final int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }
}
