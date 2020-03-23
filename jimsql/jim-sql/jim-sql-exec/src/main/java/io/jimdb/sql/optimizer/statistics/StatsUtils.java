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

import static io.jimdb.pb.Basepb.DataType.BigInt;
import static io.jimdb.pb.Basepb.DataType.Double;
import static io.jimdb.pb.Basepb.DataType.Int;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import io.jimdb.core.Session;
import io.jimdb.core.model.meta.Catalog;
import io.jimdb.core.model.meta.Table;
import io.jimdb.pb.Basepb;
import io.jimdb.pb.Metapb;
import io.jimdb.core.values.DecimalValue;
import io.jimdb.core.values.DoubleValue;
import io.jimdb.core.values.LongValue;
import io.jimdb.core.values.UnsignedLongValue;
import io.jimdb.core.values.Value;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Constants for statistics.
 */
@SuppressFBWarnings("CE_CLASS_ENVY")
public class StatsUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(StatsUtils.class);

  public static final double SELECTIVITY_DEFAULT = 0.8;
  public static final double DISTINCT_RATIO = 0.8;
  public static final double TABLE_SCAN_FACTOR = 2.0;
  public static final double CPU_FACTOR = 0.9;
  public static final double AGGREGATION_DISTINCT_FACTOR = 1.6;
  public static final double AGGREGATION_HASH_FACTOR = 1.7;
  public static final double AGGREGATION_HASH_CONTEXT_FACTOR = 6;
  public static final double MEMORY_FACTOR = 5.0;
  public static final double NETWORK_FACTOR = 1.5;

  public static final int DEFAULT_NUM_BUCKETS = 256;
  public static final int DEFAULT_TOPN = 20;

  // FIXME Set this value greater than 256 could causing OOM ?
  public static final int DEFAULT_CMS_WIDTH = 4;
  public static final int DEFAULT_CMS_DEPTH = 2;

  public static final int MAX_NUM_BUCKETS = 1024;
  public static final int MAX_TOPN = 1024;
  public static final int MAX_CMS_WIDTH = 1024;
  public static final int MAX_CMS_DEPTH = 1024;
  public static final long MAX_SAMPLE_SIZE = 1000;
  public static final int MAX_SKETCH_SIZE = 10000;

  // minimum ratio of the number of top n elements in CMSketch, 10 means 1 / 10 = 10%.
  public static final long TOP_N_THRESHOLD = 10L;

  static final int DEFAULT_ROW_COUNT = 10000;
  static final int DEFAULT_EQUAL_RATE = 1000;
  static final int DEFAULT_LESS_RATE = 3;
  static final int DEFAULT_BETWEEN_RATE = 40;

  public static final double SELECTION_FACTOR = 0.8;

  static final int OUT_OF_RANGE_BETWEEN_RATE = 100;

  public static final int STATS_COLUMN_COUNT = 14; // we have 14 columns in the stats table
  private static final int STATS_TABLE_ID = 904351;
  private static final String STATS_TABLE_NAME = "_StatsTable";
  private static final int STATS_CATALOG_ID = 904306;
  private static final String STATS_CATALOG_NAME = "_StatsCatalog";

  // column names of the stats table
  static final Map<String, Basepb.DataType> STATS_COLUMN_MAP =
      ImmutableMap.<String, Basepb.DataType>builder()
          .put("id", Int)
          .put("isIndex", Int)
          .put("rowCount", BigInt)
          .put("hId", BigInt)
          .put("hNdv", BigInt)
          .put("hNullCount", BigInt)
          .put("hTotalCount", Double)
          .put("hBuckets", BigInt)
          .put("cRowCount", Int)
          .put("cColumnCount", Int)
          .put("cHashTables", Int)
          .put("cDefaultValue", BigInt)
          .put("cNdv", BigInt)
          .put("cTotalCount", BigInt)
          .build();

  private static Metapb.TableInfo tableInfo;
  private static Catalog catalog;

  static {
    List<Metapb.ColumnInfo> columnInfoList = Lists.newArrayListWithExpectedSize(STATS_COLUMN_COUNT);
    Iterator<Map.Entry<String, Basepb.DataType>> iterator = STATS_COLUMN_MAP.entrySet().iterator();
    for (int i = 0; i < STATS_COLUMN_MAP.size(); i++) {
      Map.Entry<String, Basepb.DataType> entry = iterator.next();
      final Metapb.ColumnInfo columnInfo = Metapb.ColumnInfo.newBuilder()
                                         .setName(entry.getKey())
                                         .setId(i + 1)
                                         .setOffset(i)
                                         .setSqlType(Metapb.SQLType.newBuilder().setType(entry.getValue()).build())
                                         .setState(Metapb.MetaState.Public)
                                         .build();
      columnInfoList.add(columnInfo);
    }

    tableInfo = Metapb.TableInfo.newBuilder().setId(STATS_TABLE_ID).setName(STATS_TABLE_NAME).addAllColumns(columnInfoList).build();

    final Metapb.CatalogInfo catalogInfo = Metapb.CatalogInfo.newBuilder().setId(STATS_CATALOG_ID).setName(STATS_CATALOG_NAME).build();

    catalog = new Catalog(catalogInfo, Collections.singletonList(tableInfo));
  }

  // TODO this stats table needs to be persistent to DS
  public static final Table STATS_TABLE = new Table(catalog, tableInfo);

  /**
   * Binary search to find the count of the target value in a sorted array
   * @param values given array to search
   * @param target target value
   * @param n array length
   * @return If target is present in values then return the count of occurrences of target, otherwise return -1.
   */
  public static int count(long[] values, long target, int n) {
    // get the index of first occurrence of target
    int i = first(values, target, n);

    if (i == -1) {
      return i;
    }

    // get the index of last occurrence of target.
    int j = last(values, i, n - 1, target, n);

    return j - i + 1;
  }

  // If target is present in values then returns the index of first occurrence of target in values[0..n-1], otherwise returns -1
  private static int first(long[] values, long target, int n) {
    int low = 0;
    int high = n - 1;
    while (high >= low) {
      int mid = low + (high - low) / 2;
      if ((mid == 0 || target > values[mid - 1]) && values[mid] == target) {
        return mid;
      } else if (target > values[mid]) {
        low = mid + 1;
      } else {
        high = mid - 1;
      }
      LOGGER.debug("finding the index of first occurrence of target in value indices [{}, {}]", low, high);
    }
    return -1;
  }

  // If target is present in values then returns the index of last occurrence of target in values[0..n-1], otherwise returns -1
  private static int last(long[] values, int low, int high, long target, int n) {
    while (high >= low) {
      int mid = low + (high - low) / 2;
      if ((mid == n - 1 || target < values[mid + 1]) && values[mid] == target) {
        return mid;
      } else if (target < values[mid]) {
        high = mid - 1;
      } else {
        low = mid + 1;
      }

      LOGGER.debug("finding the index of last occurrence of target in value indices [{}, {}]", low, high);
    }
    return -1;
  }


  /**
   * Find the index of the bucket that contains the given target value.
   * If we could not find such index, return the closest bucket on the left side of the value
   */
  static int binarySearchLowerBound(Session session, List<Bucket> buckets, Value target) {
    int lo = 0;
    int hi = buckets.size() - 1;
    while (lo < hi - 1) {
      int idx = (hi - lo) / 2 + lo;
      int upperCompare = buckets.get(idx).getUpper().compareTo(session, target);
      int lowerCompare = buckets.get(idx).getLower().compareTo(session, target);

      if (upperCompare == 0 || lowerCompare == 0 || (upperCompare > 0 && lowerCompare < 0)) {
        return idx;
      } else if (upperCompare < 0) {
        // idx_upper | target
        lo = idx;
      } else {
        // target | idx_lower
        hi = idx - 1;
      }

      LOGGER.debug("finding the lower bound of the bucket index that contains the given target value in bucket indices [{}, {}]", lo, hi);
    }

    return lo;
  }

  /**
   * Find the index of the bucket that contains the given target value.
   * If we could not find such index, return the closest bucket on the right side of the value
   */
  static int binarySearchUpperBound(Session session, List<Bucket> buckets, Value target) {
    int lo = 0;
    int hi = buckets.size() - 1;
    while (lo < hi - 1) {
      int idx = (hi - lo) / 2 + lo;
      int upperCompare = buckets.get(idx).getUpper().compareTo(session, target);
      int lowerCompare = buckets.get(idx).getLower().compareTo(session, target);

      if (upperCompare == 0 || lowerCompare == 0 || (upperCompare > 0 && lowerCompare < 0)) {
        return idx;
      } else if (lowerCompare > 0) {
        // target | idx_lower
        hi = idx;
      } else {
        // idx_upper | target
        lo = idx + 1;
      }

      LOGGER.debug("finding the upper bound of the bucket index that contains the given target value in bucket indices [{}, {}]", lo, hi);
    }

    return -1;
  }

  // Calculate the fraction of the interval [lower, upper] that lies within the [lower, target]
  // using the continuous-value assumption.
  static double calculateFraction(Session session, Value lower, Value upper, Value target) {
    if (upper.compareTo(session, lower) <= 0) {
      return 0.5;
    }

    if (target.compareTo(session, lower) <= 0) {
      return 0;
    }

    if (target.compareTo(session, upper) >= 0) {
      return 1;
    }

    final Value delta1 = target.subtract(session, lower);
    final Value delta2 = upper.subtract(session, lower);
    double numerator;
    double denominator;

    switch (target.getType()) {
      case LONG :
        numerator = (double) ((LongValue) delta1).getValue();
        denominator = (double) ((LongValue) delta2).getValue();
        return numerator / denominator;

      case UNSIGNEDLONG :
        numerator = ((UnsignedLongValue) delta1).getValue().doubleValue();
        denominator = ((UnsignedLongValue) delta2).getValue().doubleValue();
        return numerator / denominator;

      case DOUBLE :
      case BINARY :
        numerator = ((DoubleValue) delta1).getValue();
        denominator = ((DoubleValue) delta2).getValue();
        return numerator / denominator;

      case DECIMAL :
        numerator = ((DecimalValue) delta1).getValue().doubleValue();
        denominator = ((DecimalValue) delta2).getValue().doubleValue();
        return numerator / denominator;
    }

    return 0.5;
  }
}
