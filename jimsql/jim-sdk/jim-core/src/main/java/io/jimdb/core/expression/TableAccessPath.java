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
package io.jimdb.core.expression;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import io.jimdb.core.model.meta.Index;

import com.google.common.collect.Lists;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * TableAccessPath
 *
 * @since 2019-08-07
 */
@SuppressFBWarnings({ "EI_EXPOSE_REP", "EI_EXPOSE_REP2", "URF_UNREAD_FIELD", "FCBL_FIELD_COULD_BE_LOCAL" })
public class TableAccessPath {
  private Index index;
  private List<ColumnExpr> indexColumns;
  private boolean isTablePath;
  private boolean useOrForceHint;
  private List<Expression> accessConditions; // conditions to build the ranges of this access path

  private List<Expression> indexConditions; // detached conditions for the index of the access path
  private List<Expression> tableConditions; // the remaining conditions
  private List<ValueRange> ranges;
  private boolean isSingleScan;

  private double countOnAccess;
  private double countOnIndex;

  public TableAccessPath() {
    // FIXME find a better way to initialize ranges
    ranges = Lists.newArrayListWithCapacity(8);
  }

  public TableAccessPath(Index index, Schema schema) {
    this();
    this.index = index;
    this.isTablePath = index.isPrimary();
    this.indexColumns = ExpressionUtil.indexToColumnExprs(index, schema.getColumns());
  }

  public Index getIndex() {
    return index;
  }

  public List<ColumnExpr> getIndexColumns() {

    return indexColumns;
  }

  public boolean isTablePath() {
    return isTablePath;
  }

  public TableAccessPath setIndex(Index index) {
    this.index = index;
    return this;
  }

  public void setIndexColumns(List<ColumnExpr> indexColumns) {
    this.indexColumns = indexColumns;
  }

  public TableAccessPath setTablePath(boolean tablePath) {
    isTablePath = tablePath;
    return this;
  }

  public List<Expression> getIndexConditions() {
    return indexConditions;
  }

  public void setIndexConditions(List<Expression> indexConditions) {
    this.indexConditions = indexConditions;
  }

  public List<Expression> getTableConditions() {
    return tableConditions;
  }

  public void setTableConditions(List<Expression> tableConditions) {
    this.tableConditions = tableConditions;
  }

  public List<Expression> getAccessConditions() {
    return accessConditions;
  }

  public void setAccessConditions(List<Expression> accessConditions) {
    this.accessConditions = accessConditions;
  }

  public TableAccessPath setUseOrForceHint(boolean useOrForceHint) {
    this.useOrForceHint = useOrForceHint;
    return this;
  }

  public boolean isUseOrForceHint() {
    return useOrForceHint;
  }

  public List<ValueRange> getRanges() {
    return ranges;
  }

  public void setRanges(@Nonnull List<ValueRange> valueRanges) {
    Objects.requireNonNull(valueRanges, "rangeList is null");
    // FIXME should we use addAll or set
    this.ranges = valueRanges;
    //ranges.addAll(valueRanges);
  }

  public void setRange(@Nonnull ValueRange valueRange) {
    Objects.requireNonNull(valueRange, "range is null");
    this.ranges = Collections.singletonList(valueRange);
    // FIXME should we use add or set
    //ranges.add(valueRange);
  }

  public boolean isSingleScan() {
    return isSingleScan;
  }

  public void setSingleScan(boolean singleScan) {
    isSingleScan = singleScan;
  }

  public double getCountOnAccess() {
    return countOnAccess;
  }

  public void setCountOnAccess(double countOnAccess) {
    this.countOnAccess = countOnAccess;
  }

  public double getCountOnIndex() {
    return countOnIndex;
  }

  public void setCountOnIndex(double countOnIndex) {
    this.countOnIndex = countOnIndex;
  }

  public void update(double rowCount, List<Expression> accessConditions,
                              List<Expression> tableConditions, List<ValueRange> ranges) {

    this.countOnAccess = rowCount;
    this.accessConditions = accessConditions;
    this.tableConditions = tableConditions;
    this.ranges = ranges;
  }
}
