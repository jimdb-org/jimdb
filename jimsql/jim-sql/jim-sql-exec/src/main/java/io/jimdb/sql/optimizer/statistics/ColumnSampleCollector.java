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

import static io.jimdb.sql.optimizer.statistics.StatsUtils.DEFAULT_CMS_DEPTH;
import static io.jimdb.sql.optimizer.statistics.StatsUtils.DEFAULT_CMS_WIDTH;
import static io.jimdb.sql.optimizer.statistics.StatsUtils.MAX_SKETCH_SIZE;

import java.util.ArrayList;

import io.jimdb.pb.Statspb;
import io.jimdb.core.values.BinaryValue;
import io.jimdb.core.values.Value;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Sample collector for a specific column
 */
@SuppressFBWarnings()
public class ColumnSampleCollector extends SampleCollector {
  private long nonNullCount; // number of non-null rows

  @VisibleForTesting
  public ColumnSampleCollector(long maxSampleSize) {
    this.maxSampleSize = maxSampleSize;
    this.totalSampleSize = 0;
    this.samples = new ArrayList<>((int) maxSampleSize);
    this.nonNullCount = 0;
    this.cms = new CountMinSketch(DEFAULT_CMS_DEPTH, DEFAULT_CMS_WIDTH);
    this.fms = new FlajoletMartinSketch(MAX_SKETCH_SIZE);
    this.cursor = 0;
  }

  private ColumnSampleCollector(Statspb.SampleCollector pbData) {
    this.maxSampleSize = pbData.getSamplesCount();
    this.totalSampleSize = pbData.getTotalSize();
    this.nonNullCount = 0;
    this.cms = CountMinSketch.fromPb(pbData.getCmSketch());
    this.fms = FlajoletMartinSketch.fromPb(pbData.getFmSketch());
    this.cursor = 0;

    // update samples
    this.samples = new ArrayList<>((int) maxSampleSize);
    pbData.getSamplesList().stream()
            .map(ByteString::toByteArray)
            .map(BinaryValue::getInstance)
            .forEach(this::collect);
  }

  /**
   * Collect the samples from the given value
   *
   * @param value the sampled value to be collected
   */
  public void collect(Value value) {
    this.totalSampleSize += value.toByteArray().length;
    SampleItem sampleItem = new SampleItem(value, this.cursor++);
    samples.add(sampleItem);
    if (!value.isNull()) {
      this.nonNullCount++;
    }
  }

  @Override
  void calculateTotalSize(long rowCount) {
    // TODO should we use double here
    this.totalSampleSize *= (double) rowCount / (long) samples.size();
  }

  /**
   * Merge two sample collectors
   * @param sampleCollector the other sample collector
   */
  public void merge(ColumnSampleCollector sampleCollector) {
    this.nonNullCount += sampleCollector.nonNullCount;
    this.totalSampleSize += sampleCollector.totalSampleSize;
    this.fms.merge(sampleCollector.fms);
    this.cms.merge(sampleCollector.cms);

    // TODO what if error happens?
    if (!sampleCollector.samples.isEmpty()) {
      sampleCollector.samples.stream().filter(sampleItem -> sampleItem != null && sampleItem.getValue() != null)
              .map(SampleItem::getValue).forEach(this::collect);
    }

  }

  @Override
  public long getNonNullCount() {
    return nonNullCount;
  }

  public static ColumnSampleCollector fromPb(Statspb.SampleCollector pbData) {
    return new ColumnSampleCollector(pbData);
  }

}
