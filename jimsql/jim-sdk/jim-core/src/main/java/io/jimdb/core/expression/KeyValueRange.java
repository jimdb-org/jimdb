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

import java.util.ArrayList;
import java.util.List;

import io.jimdb.core.model.meta.Index;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings("LII_LIST_INDEXED_ITERATING")
public class KeyValueRange {
  private Index index;
  private List<ColumnExpr> indexColumns;
  private List<ValueRange> valueRanges;

  public KeyValueRange(Index index, List<ColumnExpr> indexColumns, List<ValueRange> valueRanges) {
    this.index = index;
    this.indexColumns = indexColumns;
    this.valueRanges = valueRanges;
  }

  public KeyValueRange(Index index, ColumnExpr indexColumn, List<ValueRange> valueRanges) {
    this.index = index;
    this.indexColumns = new ArrayList<>();
    this.indexColumns.add(indexColumn);
    this.valueRanges = valueRanges;
  }

  public Index getIndex() {
    return index;
  }

  public void add(ValueRange range) {
    this.valueRanges.add(range);
  }

  public List<ValueRange> getValueRanges() {
    return valueRanges;
  }

  public void setValueRanges(List<ValueRange> valueRanges) {
    this.valueRanges = valueRanges;
  }

  public List<ColumnExpr> getIndexColumns() {
    return indexColumns;
  }

  public boolean isFullRange() {
    for (int i = 0; i < valueRanges.size(); i++) {
      if (valueRanges.get(i).getFirstStart().isMin() && valueRanges.get(i).getFirstEnd().isMax()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public String toString() {
    final StringBuilder builder = new StringBuilder();
    builder.append("KeyValueRange{index=").append(index.getName()).append(',');
    builder.append("values{");
    for (ValueRange range : valueRanges) {
      builder.append(range.isStartInclusive() ? '[' : '(');
      range.getStarts().forEach(e -> builder.append(e).append(','));
      range.getEnds().forEach(e -> builder.append(e));
      builder.append(range.isEndInclusive() ? ']' : ')');
    }
    return builder.toString();
  }
}
