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

import java.util.Arrays;
import java.util.List;

import io.jimdb.core.values.BinaryValue;
import io.jimdb.core.values.Value;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.primitives.Bytes;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Sample collector for a specific index
 */
@SuppressFBWarnings()
public class IndexSampleCollector extends SampleCollector {
  private List<List<byte[]>> prefixBytesList;

  @VisibleForTesting
  public IndexSampleCollector(long maxSampleSize, int columnCount) {
    this.maxSampleSize = maxSampleSize;
    this.samples = Lists.newArrayListWithCapacity((int) maxSampleSize);
    this.prefixBytesList = Lists.newArrayListWithExpectedSize(columnCount);

    for (int i = 0; i < columnCount; i++) {
      this.prefixBytesList.add(Lists.newArrayListWithCapacity((int) maxSampleSize));
    }
  }

  /**
   * Collect the samples from the given list of values
   * @param  values the sampled values to be collected
   */
  public void collect(Value[] values) {
    byte[] bytes = values[0].toByteArray();
    prefixBytesList.get(0).add(Arrays.copyOf(bytes, bytes.length));
    for (int i = 1; i < values.length; i++) {
      bytes = Bytes.concat(bytes, values[i].toByteArray());
      prefixBytesList.get(i).add(Arrays.copyOf(bytes, bytes.length));
    }

    Value newValue = BinaryValue.getInstance(bytes);
    this.totalSampleSize += bytes.length;

    samples.add(new SampleItem(newValue, this.cursor++));
  }

  @Override
  void calculateTotalSize(long rowCount) {
    this.totalSampleSize *= (double) rowCount / (long) samples.size();
  }

  @Override
  public long getNonNullCount() {
    return samples.size();
  }

  List<byte[]> getPrefixBytes(int index) {
    return this.prefixBytesList.get(index);
  }
}
