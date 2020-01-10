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
import static io.jimdb.sql.optimizer.statistics.StatsUtils.binarySearchLowerBound;
import static io.jimdb.sql.optimizer.statistics.StatsUtils.binarySearchUpperBound;

import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import io.jimdb.core.Session;
import io.jimdb.core.expression.ValueRange;
import io.jimdb.core.model.meta.Index;
import io.jimdb.core.values.BinaryValue;
import io.jimdb.core.values.Value;
import io.jimdb.core.values.ValueConvertor;

import com.google.common.annotations.VisibleForTesting;

import reactor.util.function.Tuple2;

/**
 * Statistics for an index
 */
public class IndexStats extends AbstractColumnStats {

  private Index indexInfo;

  @VisibleForTesting
  public IndexStats(@Nonnull CountMinSketch cms, @Nonnull Histogram histogram, @Nonnull Index indexInfo) {
    Objects.requireNonNull(cms, "cms should not be null");
    Objects.requireNonNull(histogram, "histogram should not be null");
    Objects.requireNonNull(indexInfo, "indexInfo should not be null");
    this.cms = cms;
    this.histogram = histogram;
    this.indexInfo = indexInfo;
  }

  public void reset(CountMinSketch countMinSketch, Histogram histogram) {
    this.cms = countMinSketch;
    this.histogram = histogram;
  }

  // Estimate the row count of indexed Column from the given ranges
  @Override
  @VisibleForTesting
  public double estimateRowCount(Session session, @Nonnull List<ValueRange> ranges, long modifiedCount) {
    Objects.requireNonNull(ranges, "ranges should not be null when estimating the row count");

    double totalCount = 0;
    for (ValueRange range : ranges) {

      Tuple2<BinaryValue, BinaryValue> tuple2 = range.consolidate();
      BinaryValue lower = tuple2.getT1();
      BinaryValue upper = tuple2.getT2();

      if (lower.compareTo(session, upper) == 0) {
        if (!range.isStartInclusive() || !range.isEndInclusive()) {
          continue;
        }

        // full length
        if (range.getStarts().size() == this.getIndexInfo().getColumns().length) {
          double count = this.estimateRowCount(session, lower, modifiedCount);

          totalCount += count;
          continue;
        }
      }

      if (!range.isStartInclusive()) {
        lower = lower.prefixNext();
      }

      if (range.isEndInclusive()) {
        upper = upper.prefixNext();
      }

      // TODO see TiDB histogram.go -> index.GetRowCount, we need more accurate estimation here
      totalCount += this.histogram.estimateRowCountBetween(session, lower, upper);

      if ((this.histogram.isOutOfRange(session, lower) && !(indexInfo.getColumns().length == 1 && lower.isNull()))
              || (this.histogram.isOutOfRange(session, upper))) {
        totalCount += (double) modifiedCount / OUT_OF_RANGE_BETWEEN_RATE;
      }

      if (indexInfo.getColumns().length == 1 && lower.isNull()) {
        totalCount += (double) this.histogram.getNullCount();
      }
    }

    return Math.min(totalCount, this.getTotalRowCount());
  }

  @Override
  @VisibleForTesting
  public double estimateRowCount(Session session, Value value, long modifiedCount) {
    if (indexInfo.getColumns().length == 1 && value.isNull()) {
      return this.histogram.getNullCount();
    }

    final double ndv = (double) this.histogram.getNdv();
    // we need to convert the value to binary here because for index the histogram is based on consolidated bytes
    // TODo convertToBinary needs to be refactored, currently type is not used
    BinaryValue binaryValue =  ValueConvertor.convertToBinary(session, value, null);
    if (this.histogram.getNdv() > 0 && this.histogram.isOutOfRange(session, binaryValue)) {
      return (double) modifiedCount / ndv;
    }

    if (this.cms != null) {
      return (double) this.cms.queryValue(value);
    }

    return this.histogram.estimateRowCountEqualsTo(session, value);
  }

  /**
   * Generate a new column stats based on the given selectivity
   *
   * @param session     the given session info
   * @param selectivity the given selectivity
   * @return updated stats for the indexed column
   */
  @VisibleForTesting
  public IndexStats newIndexStatsFromSelectivity(Session session, Selectivity selectivity) {
    long ndv = (long) ((double) this.getHistogram().getNdv() * selectivity.getValue());
    Histogram histogram = new Histogram(indexInfo.getId(), ndv, 0);
    IndexStats newIndexStats = new IndexStats(this.cms, histogram, this.indexInfo);

    final List<Bucket> buckets = this.histogram.getBuckets();
    long totalCount = 0;

    for (ValueRange range : selectivity.getRanges()) {
      Value firstStart = range.getFirstStart();
      Value lastEnd = range.getEnds().get(range.getEnds().size() - 1);

      final int lowerBucketIdx = binarySearchLowerBound(session, buckets, firstStart);
      final int upperBucketIdx = binarySearchUpperBound(session, buckets, lastEnd);

      for (int i = lowerBucketIdx; i < upperBucketIdx; i++) {
        totalCount += this.histogram.bucketCount(i);
        Bucket oldBucket = buckets.get(i);
        Bucket newBucket = new Bucket(oldBucket.getUpper(), oldBucket.getLower(), totalCount, oldBucket
                .getRepeatedCount());
        newIndexStats.histogram.addBucket(newBucket);
      }
    }

    return newIndexStats;
  }

  @Override
  public double estimatePseudoRowCount(Session session, double totalRowCount, List<ValueRange> ranges, int
          columnIndex) {
    return 0;
  }

  @VisibleForTesting
  public Index getIndexInfo() {
    return indexInfo;
  }
}
