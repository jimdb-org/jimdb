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
package io.jimdb.core.expression;

import java.util.ArrayList;
import java.util.List;

import io.jimdb.core.Session;
import io.jimdb.core.expression.functions.FuncType;
import io.jimdb.core.values.Value;

import com.google.common.collect.Lists;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Operator of a set of points.
 */
@SuppressFBWarnings({ "CLI_CONSTANT_LIST_INDEX", "UP_UNUSED_PARAMETER", "PDP_POORLY_DEFINED_PARAMETER", "CE_CLASS_ENVY" })
public class Points {

  public static List<Point> fullRangePoints() {
    return Lists.newArrayList(new Point(Value.MIN_VALUE, true), new Point(Value.MAX_VALUE, false));
    //return Collections.singletonList(new Point(Value.MAX_VALUE, true));
  }

  /**
   * Intersect of two lists of points
   *
   * @param session session of the given query
   * @param a       first list to intersect
   * @param b       second list to intersect
   * @return intersected point list
   */
  public static List<Point> intersect(Session session, List<Point> a, List<Point> b) {
    return merge(session, a, b, 2);
  }

  /**
   * Union two lists of points into a single list
   *
   * @param session session of the given query
   * @param a       first list to union
   * @param b       second list to union
   * @return union of the given two point lists
   */
  public static List<Point> union(Session session, List<Point> a, List<Point> b) {
    return merge(session, a, b, 1);
  }

  private static List<Point> merge(Session session, List<Point> a, List<Point> b, final int requiredInRangeCount) {
    int inRangeCount = 0;

    // sort points
    List<Point> mergedPoints = mergeSort(session, a, b);
    List<Point> merged = new ArrayList<>(mergedPoints.size());
    //intersection || isUnion
    for (Point point : mergedPoints) {
      if (point.isStart()) {
        inRangeCount++;
        if (inRangeCount == requiredInRangeCount) {
          merged.add(point);
        }
      } else {
        if (inRangeCount == requiredInRangeCount) {
          merged.add(point);
        }
        inRangeCount--;
      }
    }

    return merged;
  }

  // Implementation of the merge sort algorithm for two lists of points
  private static List<Point> mergeSort(Session session, List<Point> a, List<Point> b) {
    List<Point> points = new ArrayList<>(a.size() + b.size());
    int i = 0;
    int j = 0;
    while (i < a.size() && j < b.size()) {

      Point point1 = a.get(i);
      Point point2 = b.get(j);

      if (point1.isLessThan(session, point2)) {
        points.add(point1);
        i++;
      } else {
        points.add(point2);
        j++;
      }
    }

    if (i < a.size()) {
      points.addAll(a.subList(i, a.size()));
    } else {
      points.addAll(b.subList(j, b.size()));
    }

    return points;
  }

  public static Expression convertPointsToEqualCondition(Session session, List<Point> points, Expression condition) {
    Expression[] args = new Expression[points.size() / 2];
    FuncExpr cond = (FuncExpr) condition;
    int i = 0;
    if (cond.isFuncExpr(FuncType.Equality)) {
      if (cond.getArgs()[0].getExprType() == ExpressionType.COLUMN) {
        args[i++] = cond.getArgs()[0];
      } else if (cond.getArgs()[1].getExprType() == ExpressionType.COLUMN) {
        args[i++] = cond.getArgs()[1];
      }
    } else {
      // in
      args[i++] = cond.getArgs()[0];
    }

    for (int k = 0; k < points.size(); k += 2) {
      args[i++] = new ValueExpr(points.get(k).getValue(), cond.getResultType());
    }

    FuncType funcType = FuncType.Equality;

    if (args.length > 2) {
      funcType = FuncType.In;
    }

    return FuncExpr.build(session, funcType, cond.getResultType(), false, args);
  }
}
