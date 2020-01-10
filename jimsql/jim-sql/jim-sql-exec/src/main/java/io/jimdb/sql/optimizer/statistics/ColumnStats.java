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

import static io.jimdb.sql.optimizer.statistics.StatsUtils.OUT_OF_RANGE_BETWEEN_RATE;

import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import io.jimdb.core.Session;
import io.jimdb.core.expression.ValueRange;
import io.jimdb.core.model.meta.Column;
import io.jimdb.core.values.Value;

import com.google.common.annotations.VisibleForTesting;

/**
 * Statistics for non-indexed column
 */
public class ColumnStats extends AbstractColumnStats {

  private Column columnInfo;

  ColumnStats(@Nonnull CountMinSketch cms, @Nonnull Histogram histogram, @Nonnull Column columnInfo) {
    Objects.requireNonNull(cms, "cms should not be null");
    Objects.requireNonNull(histogram, "histogram should not be null");
    Objects.requireNonNull(columnInfo, "columnInfo should not be null");
    this.cms = cms;
    this.histogram = histogram;
    this.columnInfo = columnInfo;
  }

  public void reset(CountMinSketch countMinSketch, Histogram histogram) {
    this.cms = countMinSketch;
    this.histogram = histogram;
  }

  @Override
  @VisibleForTesting
  public double estimateRowCount(Session session, @Nonnull List<ValueRange> ranges, long modifiedCount) {
    Objects.requireNonNull(ranges, "ranges should not be null when estimating the row count");

    double rowCount = 0;

    for (ValueRange range : ranges) {
      final Value firstStart = range.getFirstStart();
      final Value firstEnd = range.getFirstEnd();

      // point case
      if (firstStart.compareTo(session, firstEnd) == 0 && range.isStartInclusive() && range.isEndInclusive()) {
        rowCount += this.estimateRowCount(session, firstStart, modifiedCount);

        continue;
      }

      // interval case
      double count = this.histogram.estimateRowCountBetween(session, firstStart, firstEnd);
      if ((this.histogram.isOutOfRange(session, firstStart) && !firstStart.isNull())
                  || this.histogram.isOutOfRange(session, firstEnd)) {
        count += (double) modifiedCount / OUT_OF_RANGE_BETWEEN_RATE;
      }

      if (!range.isStartInclusive() && !firstStart.isNull()) {
        count -= this.estimateRowCount(session, firstStart, modifiedCount);
      }

      if (range.isStartInclusive() && firstStart.isNull()) {
        count += (double) this.histogram.getNullCount();
      }

      if (range.isEndInclusive()) {
        count += this.estimateRowCount(session, firstEnd, modifiedCount);
      }
      rowCount += count;
    }

    return Math.min(Math.max(0, rowCount), histogram.getTotalRowCount());
  }

  @Override
  @VisibleForTesting
  public double estimateRowCount(Session session, Value value, long modifiedCount) {
    if (value.isNull()) {
      return (double) this.histogram.getNullCount();
    }

    if (this.histogram.getBucketCount() == 0) {
      return 0.0;
    }

    long ndv = this.histogram.getNdv();
    if (ndv > 0 && this.histogram.isOutOfRange(session, value)) {
      return (double) modifiedCount / (double) ndv;
    }

    if (this.cms != null) {
      return (double) this.cms.queryValue(value);
    }

    return this.histogram.estimateRowCountEqualsTo(session, value);
  }

  @Override
  public double estimatePseudoRowCount(Session session, double totalRowCount, List<ValueRange> ranges, int columnIndex) {
    return 0;
  }

  @VisibleForTesting
  public Column getColumnInfo() {
    return columnInfo;
  }
}
