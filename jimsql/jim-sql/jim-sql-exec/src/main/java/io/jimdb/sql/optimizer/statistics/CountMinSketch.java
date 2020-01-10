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

import static io.jimdb.sql.optimizer.statistics.StatsUtils.DEFAULT_CMS_DEPTH;
import static io.jimdb.sql.optimizer.statistics.StatsUtils.DEFAULT_CMS_WIDTH;
import static io.jimdb.sql.optimizer.statistics.StatsUtils.DEFAULT_TOPN;
import static io.jimdb.sql.optimizer.statistics.StatsUtils.TOP_N_THRESHOLD;
import static io.jimdb.sql.optimizer.statistics.StatsUtils.count;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.jimdb.pb.Statspb;
import io.jimdb.core.values.Value;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.util.function.Tuple2;

/**
 * CMS is used to query estimated row count on equality query for a table column or index.
 */
@SuppressFBWarnings({ "MRC_METHOD_RETURNS_CONSTANT", "NP_UNWRITTEN_FIELD",
        "UUF_UNUSED_FIELD", "UP_UNUSED_PARAMETER", "UWF_UNWRITTEN_FIELD", "EI_EXPOSE_REP" })
public class CountMinSketch {

  // the number of hash tables used in this CMS (row number, depth)
  private int hashTableNum;

  // the number of hash values for each hash table (column number, width)
  private int hashValueNum;

  // the total count of records, this should be the same as the row count of the corresponding histogram
  private long totalCount;

  // estimated number of distinct values
  private long estimatedNDV;

  // the default value for each query on a column value
  // if sampled data returns a small value (less than avg value / 2), then this will be returned (see queryHashValue).
  private long defaultValue;

  // a double dimension array X[i][j], in which i is the i-th hash table,
  // and j is the j-th hash value in it
  private int[][] hashTables;

  // topN data
  private Map<Long, List<TopNMetadata>> topN;

//  public CountMinSketch(Statspb.CMSketch cms) {
//    this(cms.getRowsCount(), cms.getTopNCount());
//    // TODO update topN map and hash table based on cms
//  }

  public CountMinSketch(int hashTableNum, int hashValueNum) {
    this.hashTableNum = hashTableNum;
    this.hashValueNum = hashValueNum;
    hashTables = new int[hashTableNum][];
    for (int i = 0; i < hashTableNum; i++) {
      hashTables[i] = new int[hashValueNum];
    }

    this.topN = Maps.newHashMapWithExpectedSize(hashValueNum);
    this.totalCount = 0;
  }

  @SuppressFBWarnings
  private CountMinSketch(int depth, int width, long defaultValue, long estimatedNDV) {
    this(depth, width);
    this.defaultValue = defaultValue;
    this.estimatedNDV = estimatedNDV;
  }

  @VisibleForTesting
  public long queryValue(Value value) {
    return queryBytes(value.toByteArray());
  }

  private long queryBytes(byte[] bytes) {
    final Tuple2<Long, Long> h1h2 = MurmurHash3.getHash128AsLongTuple(bytes);
    final long count = queryTopN(bytes, h1h2.getT1(), h1h2.getT2());

    if (count > 0) {
      return count;
    }
    return queryHashValue(h1h2.getT1(), h1h2.getT2());
  }

  private boolean updateTopN(long h1, long h2, byte[] bytes, long delta) {
    if (this.topN == null) {
      return false;
    }

    TopNMetadata topNMetadata = this.findTopNMetadata(h1, h2, bytes);
    if (topNMetadata != null) {
      topNMetadata.count += delta;
      return true;
    }

    return false;
  }

  private long queryTopN(byte[] bytes, long h1, long h2) {
    if (topN == null) {
      return 0;
    }
    TopNMetadata topNMetadata = findTopNMetadata(h1, h2, bytes);
    if (topNMetadata != null) {
      return topNMetadata.count;
    }
    return 0;
  }

  // FIXME refactor this function and the structure of the TOPNMetadata to reduce the search complexity to O(1) or log(N)
  private TopNMetadata findTopNMetadata(long h1, long h2, byte[] bytes) {
    if (!this.topN.containsKey(h1)) {
      return null;
    }

    for (TopNMetadata topNMetadata : this.topN.get(h1)) {
      if (topNMetadata.hashH2 == h2 && Arrays.equals(bytes, topNMetadata.data)) {
        return topNMetadata;
      }
    }

    return null;
  }

  private int calculateHashIndex(long h1, long h2, int i) {
    return (int) ((h1 % hashValueNum + (h2 % hashValueNum * i) % hashValueNum) % hashValueNum);
  }

  private long queryHashValue(long h1, long h2) {
    int[] values = new int[this.hashTableNum];
    int min = Integer.MAX_VALUE;

    for (int i = 0; i < this.hashTableNum; i++) {
      final int j = calculateHashIndex(h1, h2, i);
      if (min > this.hashTables[i][j]) {
        min = this.hashTables[i][j];
      }

      final long noise = (this.totalCount - (long) this.hashTables[i][j]) / ((long) this.hashValueNum - 1);
      if ((long) this.hashTables[i][j] < noise) {
        values[i] = 0;
      } else {
        values[i] = this.hashTables[i][j] - (int) noise;
      }
    }

    Arrays.sort(values);

    int result = values[(this.hashTableNum - 1) / 2] + (values[this.hashTableNum / 2] - values[(this.hashTableNum - 1) / 2]) / 2;
    result = Math.min(result, min);

    if ((result == 0 || (result > this.defaultValue && result < 2 * (this.totalCount / (long) this.hashValueNum))) && this.defaultValue > 0) {
      return defaultValue;
    }

    return result;
  }

  private void insertBytes(byte[] bytes) {
    insertBytesWithCount(bytes, 1);
  }

  private void insertBytesWithCount(byte[] bytes, long count) {
    Tuple2<Long, Long> h1h2 = MurmurHash3.getHash128AsLongTuple(bytes);

    if (updateTopN(h1h2.getT1(), h1h2.getT2(), bytes, count)) {
      return;
    }
    totalCount += count;
    for (int i = 0; i < hashTableNum; i++) {
      final int j = calculateHashIndex(h1h2.getT1(), h1h2.getT2(), i);
      hashTables[i][j] += count;
    }
  }

  /**
   * Merge two CMS together
   *
   * @param cms       the other CMS to be merged
   * @param topNCount number of top n elements to be merged
   */
  public void merge(CountMinSketch cms, int topNCount) {
    if (this.hashTableNum != cms.hashTableNum || this.hashValueNum != cms.hashValueNum) {
      // TODO add log
      return;
    }

    if (this.topN != null || cms.topN != null) {
      this.mergeTopN(cms.topN, topNCount);
    }

    this.totalCount += cms.totalCount;
    for (int i = 0; i < this.hashTables.length; i++) {
      for (int j = 0; j < this.hashTables[i].length; j++) {
        this.hashTables[i][j] += cms.hashTables[i][j];
      }
    }
  }

  private void mergeTopN(Map<Long, List<TopNMetadata>> topN, int topNCount) {
    Map<String, Long> counter = Maps.newHashMap();

    for (List<TopNMetadata> topNMetadataList : this.topN.values()) {
      for (TopNMetadata topNMetadata : topNMetadataList) {
        final String key = new String(topNMetadata.data, StandardCharsets.UTF_8);
        counter.put(key, counter.getOrDefault(key, 0L) + topNMetadata.count);
      }
    }

    for (List<TopNMetadata> topNMetadataList : topN.values()) {
      for (TopNMetadata topNMetadata : topNMetadataList) {
        final String key = new String(topNMetadata.data, StandardCharsets.UTF_8);
        counter.put(key, counter.getOrDefault(key, 0L) + topNMetadata.count);
      }
    }

    final List<Map.Entry<String, Long>> counterList = counter.entrySet().stream()
                                                               .sorted((o1, o2) -> o2.getValue().compareTo(o1.getValue()))
                                                               .collect(Collectors.toList());

    // prevent the overflow
    final int limit = Math.min(topNCount, counterList.size());
    this.topN = Maps.newHashMapWithExpectedSize(limit);

    for (int i = 0; i < limit; i++) {
      byte[] bytes = counterList.get(i).getKey().getBytes(StandardCharsets.UTF_8);
      Tuple2<Long, Long> h1h2 = MurmurHash3.getHash128AsLongTuple(bytes);
      List<TopNMetadata> topNMetadataList = this.topN.getOrDefault(h1h2.getT1(), Lists.newArrayListWithCapacity(DEFAULT_TOPN));
      topNMetadataList.add(new TopNMetadata(h1h2.getT2(), bytes, counterList.get(i).getValue()));

      this.topN.put(h1h2.getT1(), topNMetadataList);
    }

    for (int i = limit; i < counterList.size(); i++) {
      this.insertBytesWithCount(counterList.get(i).getKey().getBytes(StandardCharsets.UTF_8), counterList.get(i).getValue());
    }
  }

  /**
   * Merge two CMS together
   *
   * @param cms the other CMS to be merged
   */
  void merge(CountMinSketch cms) {

  }

  @VisibleForTesting
  public long getEstimatedNDV() {
    return this.estimatedNDV;
  }

  @VisibleForTesting
  public int getHashTableNum() {
    return hashTableNum;
  }

  @VisibleForTesting
  public int getHashValueNum() {
    return hashValueNum;
  }

  @VisibleForTesting
  public long getTotalCount() {
    return totalCount;
  }

  @VisibleForTesting
  public long getDefaultValue() {
    return defaultValue;
  }

  @VisibleForTesting
  public int[][] getHashTables() {
    return hashTables;
  }

  public String stringifyHashTables() {
    StringBuilder stringBuilder = new StringBuilder();
    for (int i = 0; i < hashTableNum; i++) {
      for (int j = 0; j < hashValueNum; j++) {
        if (hashTables[i][j] == 0) {
          continue;
        }
        stringBuilder.append('{').append(i).append(", ").append(j).append(", ").append(hashTables[i][j]).append('}');
      }
      stringBuilder.append('#');
    }

    return stringBuilder.toString();
  }

  /**
   * TopN counter internally used by the CountMinSketchBuilder to store the related metadata
   */
  private static class TopNMetadata {
    private long hashH2;
    private byte[] data;
    private long count;

    TopNMetadata(long hashH2, byte[] data, long count) {
      this.hashH2 = hashH2;
      this.data = data;
      this.count = count;
    }
  }

  public static CountMinSketch fromPb(Statspb.CMSketch pbData) {
    if (pbData == null) {
      return null;
    }

    CountMinSketch countMinSketch = new CountMinSketch(pbData.getRowsCount(), pbData.getRows(0).getCountersCount());
    for (int i = 0; i < pbData.getRowsCount(); i++) {
      Statspb.CMSketchRow row = pbData.getRows(i);
      for (int j = 0; j < row.getCountersCount(); j++) {
        final int count = row.getCounters(j);
        countMinSketch.hashTables[i][j] = count;
        countMinSketch.totalCount += count;
      }
    }

    countMinSketch.defaultValue = pbData.getDefaultValue();
    if (pbData.getTopNCount() == 0) {
      return countMinSketch;
    }

    for (Statspb.CMSketchTopN topN : pbData.getTopNList()) {
      final byte[] data = topN.getData().toByteArray();
      Tuple2<Long, Long> h1h2 = MurmurHash3.getHash128AsLongTuple(data);

      if (!countMinSketch.topN.containsKey(h1h2.getT1())) {
        countMinSketch.topN.put(h1h2.getT1(), Collections.singletonList(new TopNMetadata(h1h2.getT2(), data, topN.getCount())));
        continue;
      }

      countMinSketch.topN.get(h1h2.getT1()).add(new TopNMetadata(h1h2.getT2(), data, topN.getCount()));
    }

    return countMinSketch;
  }

  /**
   * CountMinSketch Builder
   */
  @SuppressFBWarnings
  public static class CountMinSketchBuilder {
    private int nValue; // the value of "N" in top n
    private long topNCount; // the total count of the top N items
    private long rowCount;
    private List<byte[]> samples;

    private int depth;
    private int width;

    private long sampleSize;
    private Map<String, Long> counterMap;
    private long[] sortedCounts;
    private long singleOccurrenceCount;
    private long lastCount;

    @VisibleForTesting
    public CountMinSketchBuilder(List<byte[]> samples, long rowCount) {
      this.samples = samples;
      this.rowCount = rowCount;
      this.depth = DEFAULT_CMS_DEPTH;
      this.width = DEFAULT_CMS_WIDTH;
      this.nValue = DEFAULT_TOPN;
      this.sampleSize = samples.size();
      this.counterMap = samples.stream().map(String::new).collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
      this.sortedCounts = counterMap.values().stream().sorted().mapToLong(Long::longValue).toArray();
      this.singleOccurrenceCount = count(sortedCounts, 1, sortedCounts.length);
      this.topNCount = 0L;
    }

    /* Add optional fields here */

    CountMinSketchBuilder withDepth(int depth) {
      this.depth = depth;
      return this;
    }

    CountMinSketchBuilder withWidth(int width) {
      this.width = width;
      return this;
    }

    CountMinSketchBuilder withTopNCount(long topNCount) {
      this.topNCount = topNCount;
      return this;
    }

    CountMinSketchBuilder withTopNValue(int topNValue) {
      this.nValue = topNValue;
      return this;
    }

    public CountMinSketch build() {
      this.nValue = Math.min(this.nValue, sortedCounts.length);

      for (int i = 0; i < sortedCounts.length && i < 2 * this.nValue; i++) {
        final boolean shouldContinue = sortedCounts[i] * 3 < sortedCounts[nValue - 1] * 2 && this.lastCount != sortedCounts[i];
        if (sortedCounts[i] == 1 || (i > nValue && shouldContinue)) {
          break;
        }

        this.lastCount = sortedCounts[i];
        this.topNCount += sortedCounts[i];
      }

      final long estimatedNdv = estimateNdv();
      final long size = Math.max(sampleSize, 1);
      final long scaleRatio = this.rowCount / size;
      final long defaultValue = calculateDefaultValue(estimatedNdv, scaleRatio, Math.max(this.rowCount, size));

      return buildCountMinSketchWithTopN(defaultValue, scaleRatio, estimatedNdv);
    }

    private long estimateNdv() {
      if (this.singleOccurrenceCount == sampleSize) {
        // Assume this is a unique column, so do not scale up the count of element
        return this.rowCount;
      } else if (this.singleOccurrenceCount == 0) {
        // Assume data only consists of sampled data. Nothing to do, no change with scale ratio
        return this.sortedCounts.length;
      }

      // Charikar, Moses, et al. "Towards estimation error guarantees for distinct values."
      // Proceedings of the nineteenth ACM SIGMOD-SIGACT-SIGART symposium on Principles of database systems. ACM, 2000.
      // This is GEE in that paper.
      // estimateNDV = sqrt(N/n) f_1 + sum_2..inf f_i
      // f_i = number of elements occurred i times in sample
      final int sampleNdv = this.sortedCounts.length;
      final double f1 = (double) this.singleOccurrenceCount;
      final double n = (double) this.sampleSize;
      final double r = (double) this.rowCount;

      long ndv = (long) (Math.sqrt(r / n) * f1 + sampleNdv - f1 + 0.5);

      return Math.min(Math.max(ndv, sampleNdv), rowCount);
    }


    private long calculateDefaultValue(long estimatedNdv, long scaleRatio, long rowCount) {
      final long estimatedRemainingCount = rowCount - (sampleSize - singleOccurrenceCount) * scaleRatio;

      if (estimatedRemainingCount < 0) {
        return 1;
      }

      return estimatedRemainingCount / Math.max(1, estimatedNdv - sortedCounts.length + singleOccurrenceCount);
    }

    private CountMinSketch buildCountMinSketchWithTopN(long defaultValue, long scaleRatio, long estimatedNDV) {
      CountMinSketch cms = new CountMinSketch(this.depth, this.width, defaultValue, estimatedNDV);

      final boolean enableTopN = this.sampleSize / TOP_N_THRESHOLD <= this.topNCount;

      for (Map.Entry<String, Long> entry : counterMap.entrySet()) {
        final long count = entry.getValue();
        final long rowCount = count > 1 ? count * scaleRatio : defaultValue;
        final byte[] bytes = entry.getKey().getBytes();

        if (enableTopN && count >= lastCount) {
          Tuple2<Long, Long> h1h2 = MurmurHash3.getHash128AsLongTuple(bytes);
          cms.topN.getOrDefault(h1h2.getT1(), Lists.newArrayList()).add(new TopNMetadata(h1h2.getT2(), bytes, rowCount));
        } else {
          cms.insertBytesWithCount(bytes, rowCount);
        }
      }

      return cms;
    }
  }
}
