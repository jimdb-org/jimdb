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

import java.util.List;

import javax.annotation.Nonnull;

import io.jimdb.core.Session;
import io.jimdb.core.expression.ValueRange;
import io.jimdb.core.values.BinaryValue;
import io.jimdb.core.values.DoubleValue;
import io.jimdb.core.values.LongValue;
import io.jimdb.core.values.StringValue;
import io.jimdb.core.values.Value;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * ColumnStatistics represents the statistics for a column
 */
@SuppressFBWarnings({ "MRC_METHOD_RETURNS_CONSTANT", "NP_UNWRITTEN_FIELD",
        "UUF_UNUSED_FIELD", "UP_UNUSED_PARAMETER", "UWF_UNWRITTEN_FIELD", "UUF_UNUSED_PUBLIC_OR_PROTECTED_FIELD" })
public abstract class AbstractColumnStats {

  protected Histogram histogram;

  protected CountMinSketch cms;

  protected long count;

  public boolean isInValid(Session session) {
    if (this.histogram.getNdv() > 0 && this.histogram.getBucketCount() == 0 && session != null) {
      //TODO  we need to reload the histogram
    }

    return this.histogram.getTotalRowCount() == 0
                   || (this.histogram.getNdv() > 0 && this.histogram.getBucketCount() == 0);
  }

  public Histogram getHistogram() {
    return histogram;
  }

  public CountMinSketch getCms() {
    return cms;
  }

  /**
   * Estimate the row count within a list of given ranges
   * @param session session of the request
   * @param ranges the given ranges
   * @param modifiedCount modified count
   * @return estimated row count
   */
  public abstract double estimateRowCount(Session session, @Nonnull List<ValueRange> ranges, long modifiedCount);

  /**
   *  Estimate row count for a given single value
   * @param session session of the query
   * @param value the given value
   * @param modifiedCount modified count
   * @return estimated rwo count
   */
  public abstract double estimateRowCount(Session session, Value value, long modifiedCount);

  /**
   * Calculate the row count by the ranges if there's no statistics information for this column.
   * TODO we can also optimize this estimation by considering signed and unsigned ranges separately
   * @param session session of query
   * @param totalRowCount total row count of the table
   * @param ranges given ranges to estimate
   * @param columnIndex index of the table
   * @return estimated row count
   */
  public abstract double estimatePseudoRowCount(Session session, double totalRowCount, List<ValueRange> ranges, int columnIndex);

  public long getTotalRowCount() {
    return (long) histogram.getTotalRowCount();
  }

  public static Value[] convertToValues(long id, boolean isIndex, long rowCount, Histogram histogram, CountMinSketch countMinSketch) {
    return new Value[]{
        // column id [long] 4
        LongValue.getInstance(id),

        // isIndex [binary] 8
        BinaryValue.getInstance(new byte[]{(byte) (isIndex ? 1 : 0)}),

        // row count [long] 4
        LongValue.getInstance(rowCount),

        // histogram [long], [long], [long], [double], [string] 4, 4, 4, 6, 7
        LongValue.getInstance(histogram.getId()),
        LongValue.getInstance(histogram.getNdv()),
        LongValue.getInstance(histogram.getNullCount()),
        DoubleValue.getInstance(histogram.getTotalRowCount()),
        StringValue.getInstance(histogram.stringifyBuckets()),

        // cms [string], [string], [string], [long], [long], [long] 7, 7, 7, 4, 4, 4
        StringValue.getInstance(String.valueOf(countMinSketch.getHashTableNum())),
        StringValue.getInstance(String.valueOf(countMinSketch.getHashValueNum())),
        StringValue.getInstance(countMinSketch.stringifyHashTables()),
        LongValue.getInstance(countMinSketch.getDefaultValue()),
        LongValue.getInstance(countMinSketch.getEstimatedNDV()),
        LongValue.getInstance(countMinSketch.getTotalCount())
    };
  }
}
