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

import io.jimdb.pb.Statspb;
import io.jimdb.core.values.BinaryValue;
import io.jimdb.core.values.Value;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Class to represent bucket in Histogram
 */
@SuppressFBWarnings({ "UUF_UNUSED_FIELD", "UWF_UNWRITTEN_FIELD", "URF_UNREAD_FIELD" })
public class Bucket {

  private Value upper; // upper bound of the bucket

  private Value lower; // lower bound of the bucket

  private long totalCount; // number of items stored in all previous and current buckets

  private long repeatedCount; // number of the maximum value in the bucket (2 2 4 4 4 9 -> 1)

  public Bucket(Value upper, Value lower, long totalCount, long repeatedCount) {
    this.upper = upper;
    this.lower = lower;
    this.totalCount = totalCount;
    this.repeatedCount = repeatedCount;
  }

  public Bucket(Statspb.BucketOrBuilder bucket) {
    this.upper = BinaryValue.getInstance(bucket.getUpperBound().toByteArray());
    this.lower = BinaryValue.getInstance(bucket.getLowerBound().toByteArray());
    this.totalCount = bucket.getCount();
    this.repeatedCount = bucket.getRepeats();
  }

  Value getUpper() {
    return upper;
  }

  Value getLower() {
    return lower;
  }

  public long getTotalCount() {
    return totalCount;
  }

  public void setTotalCount(long totalCount) {
    this.totalCount = totalCount;
  }

  long getRepeatedCount() {
    return repeatedCount;
  }

  void setRepeatedCount(long repeatedCount) {
    this.repeatedCount = repeatedCount;
  }

  // Set the upper bound of the bucket
  void setUpper(Value upper) {
    this.upper = upper;
  }

  // Set both the totalCount and repeatedCount of the bucket
  void setCounts(long totalCount, long repeatedCount) {
    this.totalCount = totalCount;
    this.repeatedCount = repeatedCount;
  }

  void accumulateCounts() {
    this.totalCount++;
    this.repeatedCount++;
  }

  void setBounds(Value lower, Value upper) {
    this.lower = lower;
    this.upper = upper;
  }

  @Override
  public String toString() {
    return "Bucket{" + "upper=" + upper + ", lower=" + lower + ", totalCount=" + totalCount + ", repeatedCount=" + repeatedCount + '}';
  }
}
