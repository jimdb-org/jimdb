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

import java.util.Collections;
import java.util.List;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;


/**
 * SampleCollector collects the samples based on given sampled values.
 * It also performs operations such as merge to merge two collectors together.
 */
@SuppressFBWarnings()
public abstract class SampleCollector {
  long maxSampleSize;
  long totalSampleSize; // total size of the samples in byte length
  long cursor; // current number of samples that have been collected

  FlajoletMartinSketch fms;

  CountMinSketch cms;

  List<SampleItem> samples; // collected sample items

  SampleCollector() {
    this.maxSampleSize = 0;
    this.totalSampleSize = 0;
    this.fms = null;
    this.cms = null;
    this.cursor = 0;
  }

  public SampleItem getSample(int i) {
    if (samples == null || i >= samples.size()) {
      return null;
    }

    return samples.get(i);
  }

  void sort() {
    Collections.sort(samples);
  }

  public int sampleCount() {
    return samples.size();
  }

  abstract void calculateTotalSize(long rowCount);

  public abstract long getNonNullCount();

  /**
   * Extracts TopN items and update the corresponding CM Sketch fields
   * @param topCount the number of top items to return
   */
  public void extractTopN(int topCount) {

  }

  public long getMaxSampleSize() {
    return maxSampleSize;
  }

  public FlajoletMartinSketch getFms() {
    return fms;
  }

  public CountMinSketch getCms() {
    return cms;
  }

  public long getTotalSampleSize() {
    return totalSampleSize;
  }

}
