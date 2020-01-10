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
package io.jimdb.sql.optimizer.physical;

import static io.jimdb.core.values.Value.NULL_VALUE;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import io.jimdb.core.Session;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.Point;
import io.jimdb.core.expression.Points;
import io.jimdb.core.expression.ValueRange;
import io.jimdb.core.expression.functions.FuncType;
import io.jimdb.pb.Metapb;
import io.jimdb.core.types.Types;
import io.jimdb.core.types.ValueType;
import io.jimdb.core.values.BinaryValue;
import io.jimdb.core.values.LongValue;
import io.jimdb.core.values.StringValue;
import io.jimdb.core.values.UnsignedLongValue;
import io.jimdb.core.values.Value;
import io.jimdb.core.values.ValueConvertor;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;

import com.google.common.collect.Lists;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

/**
 * Builder for ranges
 */
@SuppressFBWarnings({ "UPM_UNCALLED_PRIVATE_METHOD", "UP_UNUSED_PARAMETER", "UTA_USE_TO_ARRAY" })
public class RangeBuilder {

  public static final int UNSPECIFIED_LENGTH = -1;
  private static final ByteBufAllocator BYTE_BUF_ALLOCATOR = new UnpooledByteBufAllocator(false);

  /**
   * Build ranges for non-primary key column
   *
   * @param session session of the given query
   * @param accessConditions given access conditions
   * @param type SQL type of the corresponding column
   * @return range built for the non-primary key column
   */
  public static List<ValueRange> buildColumnRange(Session session, List<Expression> accessConditions, Metapb.SQLType type) {
    return buildColumnRange(session, accessConditions, type, false, UNSPECIFIED_LENGTH);
  }

  /**
   * Build ranges for the primary key column
   *
   * @param session session of the given query
   * @param accessConditions given access conditions
   * @param type SQL type of the corresponding column
   * @return range built for the primary key column
   */
  public static List<ValueRange> buildPKRange(Session session, List<Expression> accessConditions, Metapb.SQLType type) {
    return buildColumnRange(session, accessConditions, type, true, UNSPECIFIED_LENGTH);
  }

  private static List<ValueRange> buildColumnRange(Session session, List<Expression> accessConditions, Metapb.SQLType type, boolean isPKRange, int colLen) {

    if (accessConditions == null || accessConditions.isEmpty()) {
      return Collections.singletonList(fullRange());
    }

    List<Point> rangePoints = Points.fullRangePoints();

    for (Expression condition : accessConditions) {
      rangePoints = Points.intersect(session, rangePoints, condition.convertToPoints(session));
    }

    // new SqlType
    Metapb.SQLType newSqlType = ValueConvertor.convertToSafe(type);

    List<ValueRange> ranges = convertToRanges(session, rangePoints, newSqlType, isPKRange);

    if (colLen != UNSPECIFIED_LENGTH) {
      for (ValueRange range : ranges) {
        Tuple2<Value, Boolean> startTuple = cutValueByPrefixLen(range.getFirstStart(), colLen, newSqlType);
        range.getStarts().set(0, startTuple.getT1());
        if (startTuple.getT2()) {
          range.setStartInclusive(true);
        }

        Tuple2<Value, Boolean> endTuple = cutValueByPrefixLen(range.getFirstEnd(), colLen, newSqlType);
        range.getEnds().set(0, endTuple.getT1());
        if (endTuple.getT2()) {
          range.setEndInclusive(true);
        }
      }
    }
    return ranges;
  }

  // FIXME this may not be necessary
  public static List<ValueRange> buildFullIntegerRange(boolean signed) {
    List<ValueRange> result = new ArrayList<>();
    if (signed) {
      Point startPoint = new Point(LongValue.MIN_VALUE, true);
      Point endPoint = new Point(LongValue.MAX_VALUE, false);
      result.add(new ValueRange(startPoint, endPoint));
      return result;
    }

    Point startPoint = new Point(UnsignedLongValue.getInstance(BigInteger.ZERO), true);
    Point endPoint = new Point(UnsignedLongValue.getInstance(BigInteger.valueOf(Integer.MAX_VALUE)), false);
    result.add(new ValueRange(startPoint, endPoint));
    return result;
  }


  private static List<ValueRange> convertToRanges(Session session, List<Point> rangePoints, Metapb.SQLType type, boolean isPKRange) {

    // The only different between convertPointsToPKRanges and convertPointsToIndexRanges is that PK range does not allow null
    if (isPKRange) {
      return convertPointsToPKRanges(session, rangePoints, type);
    }

    return convertPointsToIndexRanges(session, rangePoints, type);
  }

  // Build ranges for table scan from range points
  private static List<ValueRange> convertPointsToPKRanges(Session session, List<Point> rangePoints, Metapb.SQLType type) {

    Metapb.SQLType newSqlType = ValueConvertor.convertToSafe(type);
    List<ValueRange> ranges = new ArrayList<>(rangePoints.size() / 2);

    for (int i = 0; i < rangePoints.size(); i += 2) {
      Point startPoint = rangePoints.get(i).castType(session, newSqlType);
      Point endPoint = rangePoints.get(i + 1).castType(session, newSqlType);

      if (isInvalidInterval(session, startPoint, endPoint)) {
        continue;
      }
      ValueRange range = new ValueRange(startPoint, endPoint);
      ranges.add(range);
    }
    return ranges;
  }

  // Build index ranges from the given range points
  static List<ValueRange> convertPointsToIndexRanges(Session session, List<Point> rangePoints, Metapb.SQLType type) {
    List<ValueRange> ranges = new ArrayList<>(rangePoints.size() / 2);
    Metapb.SQLType newSqlType = ValueConvertor.convertToSafe(type);

    for (int i = 0; i < rangePoints.size(); i += 2) {
      Point startPoint = rangePoints.get(i).castType(session, newSqlType);
      Point endPoint = rangePoints.get(i + 1).castType(session, newSqlType);

      if (isInvalidInterval(session, startPoint, endPoint)) {
        continue;
      }

      ValueRange range = new ValueRange(startPoint, endPoint);
      ranges.add(range);
    }
    return ranges;
  }

  // Cut a string according to the specified length
  private static Tuple2<Value, Boolean> cutValueByPrefixLen(Value value, int length, Metapb.SQLTypeOrBuilder type) {

    // we only support utf8 at this point
    if (!type.getCharset().equals(Types.DEFAULT_CHARSET_STR) || length == UNSPECIFIED_LENGTH) {
      return Tuples.of(value, Boolean.FALSE);
    }

    // TODO we need to think about if we really want to make value mutable

    if (value.getType() == ValueType.BINARY) {
      byte[] bytes = value.toByteArray();
      if (bytes.length > length) {
        BinaryValue newValue = BinaryValue.getInstance(Arrays.copyOf(bytes, length));
        return Tuples.of(newValue, Boolean.FALSE);
      }
    }

    if (value.getType() == ValueType.STRING) {
      if (value.getString().length() > length) {
        StringValue newValue = StringValue.getInstance(value.getString().substring(0, length));
        return Tuples.of(newValue, Boolean.FALSE);
      }
    }

    return Tuples.of(value, Boolean.FALSE);
  }

  static List<ValueRange> unionRanges(Session session, List<ValueRange> ranges) {

    if (ranges == null || ranges.isEmpty()) {
      return Collections.emptyList();
    }

    BinaryValue[] encodedStarts = new BinaryValue[ranges.size()];
    BinaryValue[] encodedEnds = new BinaryValue[ranges.size()];

    Integer[] indices = new Integer[ranges.size()];
    for (int i = 0; i < ranges.size(); i++) {
      ValueRange range = ranges.get(i);
      Tuple2<BinaryValue, BinaryValue> tuple2 = range.consolidate();

      encodedStarts[i] = tuple2.getT1();
      if (!range.isStartInclusive()) {
        encodedStarts[i] = encodedStarts[i].prefixNext();
      }

      encodedEnds[i] = tuple2.getT2();
      if (range.isEndInclusive()) {
        encodedEnds[i] = encodedEnds[i].prefixNext();
      }
      indices[i] = i;
    }

    Arrays.sort(indices, (i, j) -> encodedStarts[i].compareTo(session, encodedStarts[j]));

    List<ValueRange> result = Lists.newArrayListWithCapacity(ranges.size());
    int lastRangeIndex = indices[0];
    ValueRange lastRange = ranges.get(lastRangeIndex);

    // [a, b], [c, d] => [a, max(b, d)]
    for (int i = 1; i < ranges.size(); i++) {
      final int index = indices[i];
      if (encodedEnds[lastRangeIndex].compareTo(session, encodedStarts[index]) >= 0) {
        if (encodedEnds[lastRangeIndex].compareTo(session, encodedEnds[index]) < 0) {
          lastRangeIndex = index;
          lastRange.setEnds(ranges.get(index).getEnds());
          lastRange.setEndInclusive(ranges.get(index).isEndInclusive());
        }
      } else {
        result.add(lastRange);
        lastRange = ranges.get(index);
      }
    }
    result.add(lastRange);
    return result;
  }

  // A full range means (-infinity, +infinity)
  public static ValueRange fullRange() {
    Point startPoint = new Point(Value.MIN_VALUE, true);
    Point endPoint = new Point(Value.MAX_VALUE, false);
    return new ValueRange(startPoint, endPoint);
  }

  // A null range is used to count the null values during stats push-down
  public static ValueRange nullRange() {
    return new ValueRange(NULL_VALUE, NULL_VALUE);
  }

  static List<ValueRange> fullRangeList() {
    return Lists.newArrayList(fullRange());
  }


  // TODO we should not make max == min == nullValue
  public static boolean isInvalidInterval(Session session, Point startPoint, Point endPoint) {
    int cmp = startPoint.getValue().compareTo(session, endPoint.getValue());
    if (cmp == 0 && (!startPoint.isInclusive() || !endPoint.isInclusive())) {
      return true;
    }
    return cmp > 0;
  }

  // From TiDB: appendPoints2Ranges appends additional column ranges for multi-column index.
  // The additional column ranges can only be appended to point ranges.
  // for example we have an index (a, b), if the condition is (a > 1 and b = 2)
  // then we can not build a conjunctive ranges for this index.
  private static List<ValueRange> appendPointsToRanges(Session session, List<ValueRange> ranges, List<Point> rangePoints, Metapb.SQLType type) {

    List<ValueRange> result = Lists.newArrayListWithCapacity(ranges.size());
    for (ValueRange range: ranges) {
      if (!range.isPoint(session)) {
        result.add(range);
      } else {
        List<ValueRange> newRanges = appendPointsToIndexRange(session, range, rangePoints, type);
        result.addAll(newRanges);
      }
    }
    return result;
  }

  private static List<ValueRange> appendPointsToIndexRange(Session session, ValueRange range, List<Point> rangePoints, Metapb.SQLType type) {
    List<ValueRange> ranges = Lists.newArrayListWithCapacity(rangePoints.size() / 2);

    for (int i = 0; i < rangePoints.size(); i += 2) {
      Point startPoint = rangePoints.get(i).castType(session, type);
      Point endPoint = rangePoints.get(i + 1).castType(session, type);

      if (isInvalidInterval(session, startPoint, endPoint)) {
        continue;
      }

      List<Value> starts = Lists.newArrayList(range.getStarts());
      starts.add(startPoint.getValue());

      List<Value> ends = Lists.newArrayList(range.getEnds());
      ends.add(endPoint.getValue());

      ValueRange newRange = new ValueRange(starts, ends, startPoint.isInclusive(), endPoint.isInclusive());
      ranges.add(newRange);
    }
    return ranges;
  }

  // TODO add 'in' function support
  // Build ranges for index where the given expressions are in the form of CNF
  static List<ValueRange> buildCNFIndexRanges(Session session, List<Expression> accessConditions,
                                              List<ColumnExpr> columnExprList, int[] prefixLengths, int equalCount, List<Metapb.SQLType> returnTypes) {

    final List<Metapb.SQLType> typeList = accessConditions.stream().map(Expression::getResultType).collect(Collectors.toList());
    // FIXME is this correct?
    returnTypes.addAll(typeList);

    List<ValueRange> ranges = Collections.emptyList();
    for (int i = 0; i < equalCount; i++) {
      Expression expression = accessConditions.get(i);

      // building ranges for equal / in expressions -> if the expression is not equal in expression, then something is wrong here
      if (!expression.isFuncExpr(FuncType.Equality) && !expression.isFuncExpr(FuncType.In)) {
        break;
      }

      // build ranges for equal or in access conditions
      List<Point> points =  expression.convertToPoints(session);
      if (i == 0) {
        ranges = convertPointsToIndexRanges(session, points, returnTypes.get(0));
      } else {
        ranges = appendPointsToRanges(session, ranges, points, returnTypes.get(i));
      }
    }

    List<Point> points = Points.fullRangePoints();
    // build ranges for non-equal conditions
    for (int i = equalCount; i < accessConditions.size(); i++) {
      List<Point> convertedPoints = accessConditions.get(i).convertToPoints(session);
      points = Points.intersect(session, points, convertedPoints);
    }

    if (equalCount == 0) {
      ranges = convertPointsToIndexRanges(session, points, returnTypes.get(0));
    } else if (equalCount < accessConditions.size()) {
      ranges = appendPointsToRanges(session, ranges, points, returnTypes.get(equalCount));
    }

    // this should always be skipped until we support prefix index
    if (hasPrefix(prefixLengths) && fixPrefixColumnRange(ranges, prefixLengths, returnTypes)) {
      ranges = unionRanges(session, ranges);
    }

    return ranges;
  }

  private static boolean hasPrefix(int[] lengths) {
    for (int length : lengths) {
      if (length != UNSPECIFIED_LENGTH) {
        return true;
      }
    }

    return false;
  }

  // TODO Currently we do not support prefix indexing
  // From TiDB: fixPrefixColRange checks whether the range of one column exceeds the length and needs to be cut.
  // It specially handles the last column of each range point. If the last one need to be cut, it will
  // change the exclude status of that point and return `true` to tell
  // that we need do a range merging since that interval may have intersection.
  // e.g. if the interval is (-inf -inf, a xxxxx), (a xxxxx, +inf +inf) and the length of the last column is 3,
  //      then we'll change it to (-inf -inf, a xxx], [a xxx, +inf +inf). You can see that this two interval intersect,
  //      so we need a merge operation.
  // Q: only checking the last column to decide whether the endpoint's exclude status needs to be reset is enough?
  // A: Yes, suppose that the interval is (-inf -inf, a xxxxx b) and only the second column needs to be cut.
  //    The result would be (-inf -inf, a xxx b) if the length of it is 3. Obviously we only need to care about the data
  //    whose the first two key is `a` and `xxx`. It read all data whose index value begins with `a` and `xxx` and the third
  //    value less than `b`, covering the values begin with `a` and `xxxxx` and the third value less than `b` perfectly.
  //    So in this case we don't need to reset its exclude status. The right endpoint case can be proved in the same way
  private static boolean fixPrefixColumnRange(List<ValueRange> ranges, int[] lengths, List<Metapb.SQLType> types) {
    boolean hasCut = false;
    for (ValueRange range : ranges) {
      int lastIndex = range.getStarts().size() - 1;
      for (int i = 0; i < lastIndex; i++) {
        Tuple2<Value, Boolean> startTuple2 = cutValueByPrefixLen(range.getStarts().get(i), lengths[i], types.get(i));
        range.getStarts().set(i, startTuple2.getT1());

        Tuple2<Value, Boolean> endTuple2 = cutValueByPrefixLen(range.getEnds().get(i), lengths[i], types.get(i));
        range.getEnds().set(i, endTuple2.getT1());
      }

      Tuple2<Value, Boolean> startTuple2 = cutValueByPrefixLen(range.getStarts().get(lastIndex), lengths[lastIndex], types.get(lastIndex));
      range.getStarts().set(lastIndex, startTuple2.getT1());
      if (startTuple2.getT2()) {
        hasCut = true;
        range.setStartInclusive(true);
      }

      Tuple2<Value, Boolean> endTuple2 = cutValueByPrefixLen(range.getEnds().get(lastIndex), lengths[lastIndex], types.get(lastIndex));
      range.getEnds().set(lastIndex, endTuple2.getT1());
      if (endTuple2.getT2()) {
        hasCut = true;
        range.setEndInclusive(true);
      }
    }

    return hasCut;
  }

  private static ByteBuf allocBuffer(int capacity) {
    return BYTE_BUF_ALLOCATOR.heapBuffer(capacity);
  }
}
