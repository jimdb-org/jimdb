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

import static io.jimdb.sql.optimizer.statistics.StatsUtils.binarySearchLowerBound;
import static io.jimdb.sql.optimizer.statistics.StatsUtils.calculateFraction;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import io.jimdb.core.Session;
import io.jimdb.core.expression.ValueRange;
import io.jimdb.pb.Statspb;
import io.jimdb.core.values.Value;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Histogram represents statistics for a column or index.
 */
@SuppressFBWarnings({ "UUF_UNUSED_FIELD", "UWF_UNWRITTEN_FIELD", "URF_UNREAD_FIELD", "ISB_TOSTRING_APPENDING" })
public class Histogram {
  private long id; // ID for the histogram, can be index id or column id

  private long ndv; // number of distinct values

  private long nullCount; // number of null values

  private List<Bucket> buckets;

  // Correlation represents the statistical correlation between physical row ordering and logical ordering,
  // which can be used to estimate row count of table scan
  private double correlation;

  public Histogram(long id, long ndv, long nullCount) {
    this.id = id;
    this.ndv = ndv;
    this.nullCount = nullCount;
    this.buckets = Lists.newArrayListWithExpectedSize(StatsUtils.DEFAULT_NUM_BUCKETS);
  }

  private Histogram(Statspb.HistogramOrBuilder histogram) {
    this.id = -1;
    this.ndv = histogram.getNumDist();
    this.nullCount = 0; // TODO
    this.buckets = histogram.getBucketsList().stream().map(Bucket::new).collect(Collectors.toList());
  }

  public static Histogram fromPb(Statspb.HistogramOrBuilder histogram) {
    return new Histogram(histogram);
  }

  @VisibleForTesting
  public long getNdv() {
    return ndv;
  }

  @VisibleForTesting
  public long getId() {
    return id;
  }

  int getBucketCount() {
    return buckets.size();
  }

  List<Bucket> getBuckets() {
    return buckets;
  }

  @VisibleForTesting
  public String stringifyBuckets() {
    StringBuilder stringBuilder = new StringBuilder().append('|');
    for (Bucket bucket : buckets) {
      stringBuilder.append(bucket.toString()).append('|');
    }

    return stringBuilder.toString();
  }

  @VisibleForTesting
  public long getNullCount() {
    return nullCount;
  }

  boolean isOutOfRange(Session session, Value value) {
    if (this.getBucketCount() == 0) {
      return true;
    }

    return buckets.get(0).getLower().compareTo(session, value) > 0
                   || buckets.get(buckets.size() - 1).getUpper().compareTo(session, value) < 0;
  }

  /**
   * Calculate the total row count of this histogram
   * @return the total count of this histogram
   */
  @VisibleForTesting
  public double getTotalRowCount() {
    return this.nullCount + this.nonNullCount();
  }

  private double nonNullCount() {
    if (this.buckets.isEmpty()) {
      return 0;
    }

    return this.buckets.get(this.buckets.size() - 1).getTotalCount();
  }

  /**
   * Estimate the row count where the values are equal to the given value.
   * Note that this method is a very rough estimation. It simply finds the bucket that contains this given value and
   * return the repeated count of that bucket. This function should only be called when CMS could not find the count.
   *
   * @param value the given value for comparison
   * @return the estimated row count
   */
  @VisibleForTesting
  public double estimateRowCountEqualsTo(Session session, Value value) {
    if (isOutOfRange(session, value)) {
      return this.nonNullCount() / (double) this.ndv;
    }

    // we can also use binarySearchUpperBound here
    final int index = binarySearchLowerBound(session, this.buckets, value);

    return index != -1 ? this.buckets.get(index).getRepeatedCount() : this.nonNullCount() / (double) this.ndv;
  }

  /**
   * Estimate the row count where the values are greater than the given value.
   * @param value the given value for comparison
   * @return the estimated row count
   */
  private double estimateRowCountGreaterThan(Session session, Value value) {
    return Math.max(0, this.nonNullCount() - this.estimateRowCountLessThan(session, value) - this.estimateRowCountEqualsTo(session, value));
  }

  /**
   * Estimate the row count where the values are less than the given value.
   * @param value the given value for comparison
   * @return the estimated row count
   */
  private double estimateRowCountLessThan(Session session, Value value) {
    if (this.buckets.isEmpty() || buckets.get(0).getLower().compareTo(session, value) > 0) {
      return 0;
    }

    final int index = binarySearchLowerBound(session, this.buckets, value);
    if (index == buckets.size()) {
      return this.nonNullCount();
    }

    long currentCount = this.buckets.get(index).getTotalCount();
    long currentRepeats = this.buckets.get(index).getRepeatedCount();
    long preCount = index > 0 ? this.buckets.get(index - 1).getTotalCount() : 0;
    final Bucket bucket = this.buckets.get(index);

    final double fraction = calculateFraction(session, bucket.getLower(), bucket.getUpper(), value);
    return (double) preCount + (double) (currentCount - currentRepeats - preCount) * fraction;
  }

  /**
   * Estimate the row count where the values are between the given two values.
   * @param lower lower bound of the query
   * @param upper upper bound of the query
   * @return the estimated row count
   */
  @VisibleForTesting
  public double estimateRowCountBetween(Session session, @Nonnull Value lower, @Nonnull Value upper) {
    final double lowerCount = estimateRowCountLessThan(session, lower);
    final double upperCount = estimateRowCountLessThan(session, upper);

    if (lowerCount >= upperCount && this.ndv > 0) {
      return Math.min(this.nonNullCount() / (double) this.ndv, Math.min(upperCount, this.nonNullCount() - lowerCount));
    }
    return upperCount - lowerCount;
  }

  private Bucket getBucket(int index) {
    return this.buckets.get(index);
  }

  void addBucket(Bucket bucket) {
    buckets.add(bucket);
  }

  private void setBucket(int index, Bucket bucket) {
    this.buckets.set(index, bucket);
  }

  private void updateCorrelation(long itemCount, double corrXYSum) {
    final double corrXSum = (itemCount - 1) * itemCount / 2.0;
    final double corrXSumSquare = corrXSum * corrXSum;
    final double corrXSquareSum = (itemCount - 1) * itemCount * (2 * itemCount - 1) / 6.0;
    this.correlation = (itemCount * corrXYSum - corrXSumSquare) / (itemCount * corrXSquareSum - corrXSumSquare);
  }

  private void appendBucket(Value upper, Value lower, long count, long repeatedCount) {
    this.buckets.add(new Bucket(upper, lower, count, repeatedCount));
  }

  private void updateLastBucket(Value upper, long count, long repeatedCount) {
    Bucket lastBucket = buckets.get(buckets.size() - 1);
    lastBucket.setUpper(upper);
    lastBucket.setCounts(count, repeatedCount);
  }

  private void updateBucket(int index, long count, long sampleFactor, long ndvFactor) {
    Bucket bucket = this.buckets.get(index);
    bucket.setTotalCount(count);

    if (bucket.getRepeatedCount() == ndvFactor) {
      bucket.setRepeatedCount(2 * sampleFactor);
    } else {
      bucket.setRepeatedCount(bucket.getRepeatedCount() + sampleFactor);
    }
  }

  long bucketCount(int idx) {
    if (idx == 0) {
      return buckets.get(0).getTotalCount();
    }

    return buckets.get(idx).getTotalCount() - buckets.get(idx - 1).getTotalCount();
  }

  @VisibleForTesting
  public double estimateIncreasingFactor(long totalCount) {
    double count = this.getTotalRowCount();

    if (count == 0) {
      return 1.0;
    }

    return (double) totalCount / count;
  }

  /**
   * Merge every two buckets
   * @param limit upper bound of the buckets to be merged
   */
  private void mergeBuckets(int limit) {
    int index = 0;
    for (int i = 0; i + 1 <= limit; i += 2) {
      Value lower = this.getBucket(i).getLower();
      Value upper = this.getBucket(i + 1).getUpper();
      Bucket bucket = this.getBucket(i + 1);
      bucket.setBounds(lower, upper);

      this.setBucket(index, bucket);
      index++;
    }

    if (limit % 2 == 0) {
      Bucket bucket = this.getBucket(limit);
      this.setBucket(index++, bucket);
    }

    while (buckets.size() > index) {
      buckets.remove(buckets.size() - 1);
    }
  }

  /**
   * Split the range based on the upper bound of the histogram. Note that the last bucket's upper bound is defined as inf.
   * @param session current session of the request
   * @param oldRanges the original range
   * @return the new range
   */
  List<ValueRange> splitRange(Session session, List<ValueRange> oldRanges) {
    return null;
  }


  Histogram updateByRanges(Session session, List<ValueRange> ranges) {
    return null;
  }

  public Histogram merge(Session session, Histogram histogram, int bucketSize) {
    if (this.getBucketCount() == 0) {
      return histogram;
    }

    if (histogram.getBucketCount() == 0) {
      return this;
    }

    this.ndv += histogram.ndv;

    int length = this.getBucketCount();
    long offset = 0L;
    if (0 == this.getBucket(length - 1).getUpper().compareTo(session, histogram.getBucket(0).getLower())) {
      this.ndv--;
      final long updatedCount = this.getBucket(length - 1).getTotalCount() + histogram.getBucket(0).getTotalCount();
      final long updatedRepeatedCount = histogram.getBucket(0).getRepeatedCount();
      this.updateLastBucket(histogram.getBucket(0).getUpper(), updatedCount, updatedRepeatedCount);
      offset = histogram.getBucket(0).getTotalCount();
      histogram.popFirstBucket();
    }

    while (this.getBucketCount() > bucketSize) {
      this.mergeBuckets(this.getBucketCount() - 1);
    }

    if (histogram.getBucketCount() == 0) {
      return this;
    }

    while (histogram.getBucketCount() > bucketSize) {
      histogram.mergeBuckets(histogram.getBucketCount() - 1);
    }

    long thisCount = this.getBucket(this.getBucketCount() - 1).getTotalCount();
    long histogramCount = histogram.getBucket(histogram.getBucketCount() - 1).getTotalCount() - offset;
    double thisAvg = (double) thisCount / (double) this.getBucketCount();
    double histogramAvg = (double) histogramCount / (double) histogram.getBucketCount();

    while (this.getBucketCount() > 1 && thisAvg * 2 <= histogramAvg) {
      this.mergeBuckets(this.getBucketCount() - 1);
      thisAvg *= 2;
    }

    while (histogram.getBucketCount() > 1 && histogramAvg * 2 <= thisAvg) {
      histogram.mergeBuckets(histogram.getBucketCount() - 1);
      histogramAvg *= 2;
    }

    for (int i = 0; i < histogram.getBucketCount(); i++) {
      Bucket bucket = histogram.getBucket(i);
      this.appendBucket(bucket.getLower(), bucket.getUpper(), bucket.getTotalCount() + thisCount - offset, bucket.getRepeatedCount());
    }

    while (this.getBucketCount() > bucketSize) {
      this.mergeBuckets(this.getBucketCount() - 1);
    }

    return this;
  }

  private void popFirstBucket() {
    // TODO optimize this pop operation.
    this.buckets.remove(0);
  }

  /**
   * Default builder for index or column histogram
   */
  public static class DefaultIndexOrColumnHistogramBuilder {
    Session session;

    // number of buckets for the histogram
    int numBuckets = 0;

    // columnID of the corresponding column
    long id;

    SampleCollector sampleCollector;

    // value type of the corresponding column
    long rowCount;

    // number of distinct values in this column
    long ndv;

    public DefaultIndexOrColumnHistogramBuilder(@Nonnull SampleCollector sampleCollector, @Nonnegative long id, long ndv) {
      Objects.requireNonNull(sampleCollector, "it is meaningless to build histogram with a null sample collector");

      this.sampleCollector = sampleCollector;
      this.id = id;
      this.ndv = Math.max(1L, ndv); // ndv should never be zero
      this.numBuckets = StatsUtils.DEFAULT_NUM_BUCKETS; // numBuckets should never be zero
      this.rowCount = sampleCollector.sampleCount();

      // TODO do we really need the session here?
      this.session = null;
    }

    /* Add any optional fields below */

    public DefaultIndexOrColumnHistogramBuilder withSession(Session session) {
      this.session = session;
      return this;
    }

    public DefaultIndexOrColumnHistogramBuilder withNumBuckets(int numBuckets) {
      this.numBuckets = numBuckets;
      return this;
    }

    public DefaultIndexOrColumnHistogramBuilder withRowCount(long rowCount) {
      this.rowCount = rowCount;
      return this;
    }

    /**
     * Build a histogram for an index or column.
     * TODO when call this function, ensure ndv = Math.min(ndv, rowCount)
     *
     * @return the built Histogram
     */
    public Histogram build() {

      final long nullCount = sampleCollector.sampleCount() - sampleCollector.getNonNullCount();
      Histogram histogram = new Histogram(id, ndv, nullCount);

      final int sampleCount = sampleCollector.sampleCount();
      if (rowCount == 0 || sampleCount == 0) {
        return histogram;
      }

      if (sampleCount == 1) {
        histogram.correlation = 1;
        return histogram;
      }

      // As we use samples to build the histogram, the bucket number should be multiplied by a factor.
      final double sampleFactor = (double) rowCount / sampleCount;

      // Note that the bucket count is increased by sampleFactor. Therefore the actual bucket capacity should be
      // floor(bucketCapacity/sampleFactor)*sampleFactor, which may less than bucketCapacity itself.
      // The sampleFactor is used to avoid generating too many buckets.
      final double bucketCapacity = (double) rowCount / numBuckets + sampleFactor;

      final double ndvFactor = Math.min((double) rowCount / ndv, sampleFactor);

      double corrXYSum = 0;
      int bucketIndex = 0;

      // Samples must be sorted in order to build correct histogram
      // TODO for indexed column the samples should already be sorted
      sampleCollector.sort();

      SampleItem sampleItem0 = sampleCollector.getSample(0);
      histogram.appendBucket(sampleItem0.getValue(), sampleItem0.getValue(), (long) sampleFactor, (long) ndvFactor);

      for (int i = 1; i < sampleCount; i++) {
        SampleItem sampleItem = sampleCollector.getSample(i);
        corrXYSum += (double) i * sampleItem.getOrdinal();

        double totalCount = (double) (i + 1) * sampleFactor;
        double lastCount = histogram.getBucket(bucketIndex).getTotalCount();

        // The new item has the same value as current value
        if (0 == histogram.getBucket(bucketIndex).getUpper().compareTo(session, sampleItem.getValue())) {
          // To ensure the same value can only be stored into a single bucket, don't increase bucket index even if it
          // exceeds bucket capacity.
          histogram.updateBucket(bucketIndex, (long) totalCount, (long) sampleFactor, (long) ndvFactor);

        } else if (totalCount - lastCount <= bucketCapacity) {
          // Still have room to update the current bucket
          histogram.updateLastBucket(sampleItem.getValue(), (long) totalCount, (long) ndvFactor);

        } else {
          bucketIndex++;

          histogram.appendBucket(sampleItem.getValue(), sampleItem.getValue(), (long) totalCount, (long) ndvFactor);
        }
      }

      // TODO The following comments copied from TiDB
      // X means the ordinal of the item in original sequence, Y means the ordinal of the item in the
      // sorted sequence, we know that X and Y value sets are both:
      // 0, 1, ..., sampleNum-1
      // we can simply compute sum(X) = sum(Y) =
      //    (sampleNum-1)*sampleNum / 2
      // and sum(X^2) = sum(Y^2) =
      //    (sampleNum-1)*sampleNum*(2*sampleNum-1) / 6
      // We use "Pearson correlation coefficient" to compute the order correlation of columns,
      // the formula is based on https://en.wikipedia.org/wiki/Pearson_correlation_coefficient.
      // Note that (itemsCount*corrX2Sum - corrXSum*corrXSum) would never be zero when sampleNum is larger than 1.
      histogram.updateCorrelation(sampleCount, corrXYSum);

      return histogram;
    }

  }

  /**
   * Builder for index's histogram. Note that an index can be primary, and can contains multiple columns
   */
  public static class IndexHistogramBuilder {
    long numBuckets;

    long bucketCapacity;

    long bucketIndex; // current bucket index

    long lastCount;

    long count; // number of values been sampled

    Histogram histogram;

    Session session;

    /**
     * Iteratively update the histogram
     * @param value the value to be used for update
     * @return the builder itself
     */
    public IndexHistogramBuilder iterate(Value value) {
      return this;
    }

    public Histogram build() {
      return this.histogram;
    }

    public IndexHistogramBuilder update(Value value) {
      this.count++;

      if (this.count == 1) {
        histogram.appendBucket(value, value, 1, 1);
        histogram.ndv = 1;
        return this;
      }

      final Bucket bucket = this.histogram.getBucket((int) bucketIndex);
      if (0 == bucket.getUpper().compareTo(session, value)) {
        // To ensure the same value can only be stored into a single bucket, don't increase bucket index even if it
        // exceeds bucket capacity.
        histogram.getBucket((int) bucketIndex).accumulateCounts();

      } else if (bucket.getTotalCount() + 1 - lastCount < bucketCapacity) {
        histogram.updateLastBucket(value, bucket.getTotalCount() + 1, 1);
        histogram.ndv++;

      } else {
        if (bucketIndex + 1 == numBuckets) {
          histogram.mergeBuckets((int) bucketIndex);

          // After merge every two buckets, the capacity in a bucket needs to be doubled
          bucketCapacity *= 2;
          bucketIndex = bucketIndex / 2;
          lastCount = bucketIndex == 0 ? 0 : histogram.getBucket((int) bucketIndex - 1).getTotalCount();
        }

        final Bucket newBucket = this.histogram.getBucket((int) bucketIndex);
        if (newBucket.getTotalCount() + 1 - lastCount < bucketCapacity) {
          histogram.updateLastBucket(value, newBucket.getTotalCount() + 1, 1);
        } else {
          lastCount = newBucket.getTotalCount();
          bucketIndex++;
          histogram.appendBucket(value, value, lastCount + 1, 1);
        }

        histogram.ndv++;
      }

      return this;
    }
  }
}
