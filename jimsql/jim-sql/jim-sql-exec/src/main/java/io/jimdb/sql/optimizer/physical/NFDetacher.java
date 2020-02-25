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
package io.jimdb.sql.optimizer.physical;

import static io.jimdb.sql.optimizer.physical.RangeBuilder.UNSPECIFIED_LENGTH;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import io.jimdb.core.Session;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.ConditionChecker;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.ExpressionType;
import io.jimdb.core.expression.ExpressionUtil;
import io.jimdb.core.expression.FuncExpr;
import io.jimdb.core.expression.Point;
import io.jimdb.core.expression.Points;
import io.jimdb.core.expression.ValueRange;
import io.jimdb.core.expression.functions.FuncType;
import io.jimdb.pb.Basepb;
import io.jimdb.pb.Metapb;

import com.google.common.collect.Lists;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuple3;
import reactor.util.function.Tuple4;
import reactor.util.function.Tuples;

/**
 * CNF and DNF detacher for the given conditions
 */
@SuppressFBWarnings({ "CLI_CONSTANT_LIST_INDEX", "UP_UNUSED_PARAMETER", "RC_REF_COMPARISON", "URF_UNREAD_FIELD",
        "UUF_UNUSED_FIELD", "RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE", "PDP_POORLY_DEFINED_PARAMETER",
        "EC_UNRELATED_TYPES" })
public class NFDetacher {

  /**
   * Detach access conditions from the given expressions
   *
   * @param session    session of the current query
   * @param conditions given conditions to be detached
   * @param id   id of the corresponding column for the to-be-detached access conditions
   * @return the detached access conditions and the remaining filter conditions
   */
  public static Tuple2<List<Expression>, List<Expression>> detachConditions(Session session, List<Expression> conditions,
                                                                            int id) {
    return detachCNFColumnConditions(session, conditions, new ConditionChecker(id));
  }

  // Detach accessConditions and filterConditions from CNF
  private static Tuple2<List<Expression>, List<Expression>> detachCNFColumnConditions(Session session,
                                                                                      List<Expression> conditions,
                                                                                      ConditionChecker checker) {
    List<Expression> accessConditions = Lists.newArrayListWithCapacity(conditions.size());
    List<Expression> filterConditions = Lists.newArrayListWithCapacity(conditions.size());

    if (conditions == null) {
      return Tuples.of(accessConditions, filterConditions);
    }

    for (Expression condition : conditions) {

      if (condition.getExprType() == ExpressionType.FUNC) {
        FuncExpr funcExpr = (FuncExpr) condition;

        if (funcExpr.getFuncType() == FuncType.BooleanOr) {
          List<Expression> dnfItems = flattenDNFCondition(funcExpr);
          Tuple2<List<Expression>, Boolean> detachedDNFResult = detachDNFColumnConditions(session, dnfItems, checker);
          boolean hasResidual = detachedDNFResult.getT2();

          if (hasResidual) {
            filterConditions.add(condition);
          }
          if (detachedDNFResult.getT1().isEmpty()) {
            continue;
          }
          Expression rebuildDNF = rebuildDNFCondition(session, detachedDNFResult.getT1());
          if (rebuildDNF != null) {
            accessConditions.add(rebuildDNF);
          }
          continue;
        }
      }
      if (checker.check(condition)) {
        accessConditions.add(condition);
      } else {
        filterConditions.add(condition);
      }
    }

    return Tuples.of(accessConditions, filterConditions);
  }

  // Detach accessConditions from DNF
  private static Tuple2<List<Expression>, Boolean> detachDNFColumnConditions(Session session, List<Expression> conditions,
                                                                             ConditionChecker checker) {
    List<Expression> accessConditions = new ArrayList<>();
    boolean hasResidual = false;

    for (Expression condition : conditions) {
      if (condition.getExprType() == ExpressionType.FUNC && ((FuncExpr) condition).getFuncType() == FuncType.BooleanAnd) {
        List<Expression> cnfItems = flattenCNFCondition((FuncExpr) condition);
        Tuple2<List<Expression>, List<Expression>> detachCNFResult = detachCNFColumnConditions(session, cnfItems, checker);
        List<Expression> cnfAccessConditions = detachCNFResult.getT1();
        List<Expression> cnfFilterConditions = detachCNFResult.getT2();
        if (cnfAccessConditions.isEmpty()) {
          return Tuples.of(Collections.emptyList(), Boolean.TRUE);
        }
        if (!cnfFilterConditions.isEmpty()) {
          hasResidual = true;
        }
        Expression rebuildCNF = rebuildCNFCondition(session, cnfItems);
        if (rebuildCNF != null) {
          accessConditions.add(rebuildCNF);
        }
      } else if (checker.check(condition)) {
        accessConditions.add(condition);
      } else {
        return Tuples.of(Collections.emptyList(), Boolean.TRUE);
      }
    }

    return Tuples.of(accessConditions, hasResidual);
  }

  // Flatten DNF condition from tree to list
  private static List<Expression> flattenDNFCondition(FuncExpr condition) {
    List<Expression> list = new ArrayList<>();
    extractNFConditions(condition, FuncType.BooleanOr, list);
    return list;
  }

  // Flatten the CNF condition from tree to list
  private static List<Expression> flattenCNFCondition(FuncExpr condition) {
    List<Expression> list = new ArrayList<>();
    extractNFConditions(condition, FuncType.BooleanAnd, list);
    return list;
  }

  // Flatten NF condition from tree to list
  private static void extractNFConditions(FuncExpr funcExpr, FuncType funcType, List<Expression> list) {
    Expression[] exprArgs = funcExpr.getArgs();
    if (exprArgs != null) {
      for (Expression child : exprArgs) {
        FuncExpr funcChild = (FuncExpr) child;
        if (child.getExprType() == ExpressionType.FUNC && funcChild.getFuncType() == funcType) {
          extractNFConditions(funcChild, funcType, list);
        } else {
          list.add(child);
        }
      }
    }
  }

  // Rebuild CNF condition for a given set of expressions
  private static Expression rebuildCNFCondition(Session session, List<Expression> expressions) {
    return rebuildNFCondition(session, expressions, FuncType.BooleanAnd);
  }

  // Rebuild DNF condition for a given set of expressions
  public static Expression rebuildDNFCondition(Session session, List<Expression> expressions) {
    return rebuildNFCondition(session, expressions, FuncType.BooleanOr);
  }

  // Rebuild NF condition for a given set of expressions
  private static Expression rebuildNFCondition(Session session, List<Expression> expressions, FuncType funcType) {
    if (expressions == null || expressions.isEmpty()) {
      return null;
    } else if (expressions.size() == 1) {
      return expressions.get(0);
    }

    return rebuildFunc(session, funcType, expressions);
  }

  private static Expression rebuildFunc(Session session, FuncType funcType, List<Expression> expressions) {
    if (funcType == FuncType.BooleanAnd || funcType == FuncType.BooleanOr) {
      return buildFunc(session, funcType, expressions);
    }
    return null;
  }

  private static Expression buildFunc(Session session, FuncType funcType, List<Expression> expressions) {
    if (expressions.size() <= 2) {
      return FuncExpr.build(session, funcType, Metapb.SQLType.newBuilder().setType(Basepb.DataType.TinyInt).build(), false,
              expressions.toArray(new Expression[expressions.size()]));
    } else {
      Expression newExpr = expressions.get(0);
      for (int i = 1; i < expressions.size(); i++) {
        Expression[] subArray = new Expression[2];
        subArray[0] = newExpr;
        subArray[1] = expressions.get(i);
        newExpr = FuncExpr.build(session, funcType, Metapb.SQLType.newBuilder().setType(Basepb.DataType.TinyInt).build(), false, subArray);
      }
      return newExpr;
    }
  }

  // From TiDB: ExtractEqAndInCondition will split the given condition into three parts by the information of index columns and their lengths.
  // accesses: The condition will be used to build range.
  // newConditions: We'll simplify the given conditions if there're multiple in conditions or eq conditions on the same column.
  //   e.g. if there're a in (1, 2, 3) and a in (2, 3, 4). This two will be combined to a in (2, 3) and pushed to newConditions.
  // bool: indicate whether there's nil range when merging eq and in conditions.
  private static Tuple3<List<Expression>, List<Expression>, Boolean> detachEqAndInConditions(
      Session session, List<Expression> conditions, List<ColumnExpr> columns) {

    // conditions used to build the index/column range
    Expression[] accessConditions = new Expression[columns.size()];

    Expression[] mergedAccessConditions = new Expression[columns.size()];
    List<Expression> newConditions = Lists.newArrayListWithCapacity(conditions.size());
    List<List<Point>> points = Lists.newArrayListWithCapacity(columns.size());

    for (Expression condition : conditions) {
      int colOffset = getEqOrInColOffset(condition, columns);
      if (colOffset == -1) {
        newConditions.add(condition);
        continue;
      }
      if (accessConditions[colOffset] == null) {
        accessConditions[colOffset] = condition;
        continue;
      }

      // From TiDB: Multiple Eq/In conditions for one column in CNF, apply intersection on them
      // Lazily compute the points for the previously visited Eq/In
      if (mergedAccessConditions[colOffset] == null) {
        final Expression accessCondition = accessConditions[colOffset];
        mergedAccessConditions[colOffset] = accessCondition;
        points.set(colOffset, accessCondition.convertToPoints(session));
      }
      points.set(colOffset, Points.intersect(session, points.get(colOffset), condition.convertToPoints(session)));
      if (points.get(colOffset).isEmpty()) {
        // True indicate whether there's nil range when merging eq and in conditions.
        return Tuples.of(Collections.emptyList(), Collections.emptyList(), Boolean.TRUE);
      }
    }

    for (int i = 0; i < mergedAccessConditions.length; i++) {
      Expression mergedAccessCondition = mergedAccessConditions[i];
      if (mergedAccessCondition == null) {
        if (accessConditions[i] != null) {
          newConditions.add(accessConditions[i]);
        }
        continue;
      }

      // TODO fill convertPointsToEqualCondition
      accessConditions[i] = Points.convertPointsToEqualCondition(session, points.get(i), mergedAccessCondition);
      newConditions.add(accessConditions[i]);
    }

    List<Expression> truncatedAccessConditions = Lists.newArrayListWithExpectedSize(accessConditions.length);
    for (Expression accessCondition : accessConditions) {
      if (accessCondition == null) {
        break;
      }
      // TODO consider UnspecifiedLength (See TiDB), currently we do not have the concept of 'length' in Index
      truncatedAccessConditions.add(accessCondition);
    }

    // remove conditions in truncatedAccessConditions from newConditions
    List<Expression> filteredConditions =
        newConditions.stream().filter(condition -> !ExpressionUtil.containsExpr(truncatedAccessConditions, condition)).collect(Collectors.toList());

    return Tuples.of(truncatedAccessConditions, filteredConditions, Boolean.FALSE);
  }

//  private static Tuple2<List<Expression>, List<Expression>> extractEqAndInConditions(
//          Session session, List<Expression> conditions, List<ColumnExpr> columns) {
//    // conditions used to build ranges
//    Expression[] accessConditions = new Expression[columns.size()];
//
//    // TODO Support 'in'
//    // 'in (1, 2, 3)' and 'in (2, 3, 4)' will be combined into a 'in (2, 3)', which will be added into newConditions.
//    List<Expression> remainingConditions = Lists.newArrayListWithCapacity(conditions.size());
//
//    for (Expression condition : conditions) {
//      int colOffset = getEqOrInColOffset(condition, columns);
//      if (colOffset == -1) {
//        remainingConditions.add(condition);
//        continue;
//      }
//
//      if (accessConditions[colOffset] == null) {
//        accessConditions[colOffset] = condition;
//      }
//    }
//
//    List<Expression> equalConditions =
//            Arrays.stream(accessConditions).filter(Objects::nonNull).collect(Collectors.toList());
//
//    // TODO consider UnspecifiedLength (See TiDB), currently we do not have the concept of 'length' in Index
//    return Tuples.of(equalConditions, remainingConditions);
//  }

  private static int getEqOrInColOffset(@Nonnull Expression condition, @Nonnull List<ColumnExpr> columns) {
    Objects.requireNonNull(condition);
    Objects.requireNonNull(columns);

    FuncExpr funcExpr = (FuncExpr) condition;
    Expression arg0 = funcExpr.getArgs()[0];
    Expression arg1 = funcExpr.getArgs()[1];

    if (condition.isFuncExpr(FuncType.Equality)) {
      if ((arg0.getExprType() == ExpressionType.COLUMN && arg1.getExprType() == ExpressionType.CONST)
          || (arg0.getExprType() == ExpressionType.CONST && arg1.getExprType() == ExpressionType.COLUMN)) {
        ColumnExpr columnExpr = arg0.getExprType() == ExpressionType.COLUMN ? (ColumnExpr) arg0 : (ColumnExpr) arg1;

        for (int i = 0; i < columns.size(); i++) {
          if (columns.get(i).getId().equals(columnExpr.getId())) {
            return i;
          }
        }
      }
    }

    if (condition.isFuncExpr(FuncType.In)) {
      if (arg0.getExprType() != ExpressionType.COLUMN) {
        return -1;
      }

      for (int i = 1; i < funcExpr.getArgs().length; i++) {
        if (funcExpr.getArgs()[i].getExprType() != ExpressionType.CONST) {
          return -1;
        }
      }

      for (int i = 0; i < columns.size(); i++) {
        if (columns.get(i).getId().equals(arg0)) {
          return i;
        }
      }
    }
    return -1;
  }

  /**
   * Detach the index filters from table filters.
   *
   * @param session        the session of the given query
   * @param conditions     conditions to be analyzed
   * @param columnExprList list of expressions used for range analysis
   * @return detached ranges and access conditions
   */
  public static Tuple4<List<ValueRange>, List<Expression>, List<Expression>, Boolean> detachConditionsAndBuildRangeForIndex(
      Session session, List<Expression> conditions, List<ColumnExpr> columnExprList) {

    if (conditions == null || conditions.isEmpty()) {
      return DetacherResult.empty().unwrap();
    }

    List<Metapb.SQLType> returnTypes = columnExprList.stream().map(ColumnExpr::getResultType).collect(Collectors.toList());

    // TODO add prefix length
    // currently we do not support prefix indexing therefore the prefix length of a column is always UNSPECIFIED_LENGTH
    int[] prefixLengths = new int[columnExprList.size()];
    Arrays.fill(prefixLengths, UNSPECIFIED_LENGTH);

    if (conditions.size() == 1 && conditions.get(0).isFuncExpr(FuncType.BooleanOr)) {
      DetacherResult detacherResult = detachDNFConditionsAndBuildRangeForIndex(session, (FuncExpr) conditions.get(0), columnExprList, prefixLengths, returnTypes);
      return detacherResult.unwrap();
    }

    DetacherResult detacherResult = detachCNFConditionsAndBuildRangeForIndex(session, conditions, columnExprList, prefixLengths, returnTypes);
    return detacherResult.unwrap();
  }

  // Detach the index filters from table filters for CNF (conditions are connected with `and`).
  // We first find the point query column and then extract the range query column.
  // considerDNF is true means it will try to extract access conditions from the DNF expressions.
  private static DetacherResult detachCNFConditionsAndBuildRangeForIndex(Session session, @Nonnull  List<Expression> conditions,
                                                                         List<ColumnExpr> columns, int[] lengths, List<Metapb.SQLType> returnTypes) {
    // first find the point query column
    Tuple3<List<Expression>, List<Expression>, Boolean> extracted = detachEqAndInConditions(session, conditions, columns);

    Objects.requireNonNull(conditions, "conditions cannot be null");
    final List<Expression> accessConditions = extracted.getT1();
    List<Expression> remainingConditions = extracted.getT2();
    // whether there's nil range when merging eq and in conditions.
    final boolean hasNullRange = extracted.getT3();

    if (hasNullRange) {
      return DetacherResult.empty();
    }

    final int equalCount = accessConditions.size();
    if (equalCount == columns.size()) {
      List<ValueRange> ranges = RangeBuilder.buildCNFIndexRanges(session, accessConditions, lengths, equalCount, returnTypes);
      return new DetacherResult(ranges, accessConditions, remainingConditions, false);
    }

    ConditionChecker conditionChecker = new ConditionChecker(columns.get(equalCount).getId());

    // then extract the range query column
    Tuple2<List<Expression>, List<Expression>> detached = detachCNFColumnConditions(session, remainingConditions, conditionChecker);
    accessConditions.addAll(detached.getT1());
    remainingConditions = detached.getT2();

    List<ValueRange> ranges = RangeBuilder.buildCNFIndexRanges(session, accessConditions, lengths, equalCount, returnTypes);
    return new DetacherResult(ranges, accessConditions, remainingConditions, false);
  }

  // Detach the index filters from table filters for DNF (conditions are connected with `or`).
  // Conditions of each DNF will be detached and then consolidated into a single DNF.
  private static DetacherResult detachDNFConditionsAndBuildRangeForIndex(Session session, @Nonnull FuncExpr condition, List<ColumnExpr> columns, int[] prefixLength, List<Metapb.SQLType> returnTypes) {
    Objects.requireNonNull(condition, "condition cannot be null");

    List<Expression> dnfItems = condition.flattenDNFCondition();

    ConditionChecker conditionChecker = new ConditionChecker(columns.get(0).getId());
    List<Expression> accessConditions = Lists.newArrayListWithCapacity(dnfItems.size());
    List<ValueRange> ranges = Lists.newArrayListWithCapacity(dnfItems.size());
    List<Expression> remainingConditions = Collections.emptyList();
    for (Expression expression : dnfItems) {
      if (expression.isFuncExpr(FuncType.BooleanAnd)) {
        List<Expression> cnfItems = ((FuncExpr) expression).flattenCNFCondition();
        DetacherResult detacherResult = detachCNFConditionsAndBuildRangeForIndex(session, cnfItems, columns, prefixLength, returnTypes);
        if (detacherResult.accessConditions.isEmpty()) {
          return new DetacherResult(Collections.singletonList(RangeBuilder.fullRange()), Collections.emptyList(), remainingConditions, true);
        }

        if (detacherResult.remainingConditions.size() > 0) {
          remainingConditions = Collections.singletonList(condition);
        }

        ranges.addAll(detacherResult.ranges);
        accessConditions.addAll(detacherResult.accessConditions);

      } else if (conditionChecker.check(expression)) {

        List<Point> points = expression.convertToPoints(session);
        List<ValueRange> indexRanges = RangeBuilder.convertPointsToIndexRanges(session, points, returnTypes.get(0));
        ranges.addAll(indexRanges);
        accessConditions.add(expression);
      } else {
        remainingConditions = Collections.singletonList(condition);
        return new DetacherResult(Collections.singletonList(RangeBuilder.fullRange()), Collections.emptyList(), remainingConditions, true);
      }
    }

    return new DetacherResult(RangeBuilder.unionRanges(session, ranges), accessConditions, remainingConditions, true);
  }

  public static List<ValueRange> buildRangeFromDetachedIndexConditions(Session session, List<Expression> conditions,
                                                                       List<ColumnExpr> columnExprList) {

    Tuple4<List<ValueRange>, List<Expression>, List<Expression>, Boolean> tuple4 =
        detachConditionsAndBuildRangeForIndex(session, conditions, columnExprList);

    return tuple4.getT1();
  }

  /**
   * Wrapper of the results from detaching conditions and building ranges
   */
  private static final class DetacherResult {
    List<ValueRange> ranges; // ranges extracted and built from conditions

    List<Expression> accessConditions; // extracted conditions for access

    List<Expression> remainingConditions; // other filter conditions

    int equalConditionCount; // number of equal conditions that have been extracted

    //int equalOrInCount; // number of equal/in conditions that have been extracted

    boolean isDNFCondition; // if the top layer of conditions are in DNF or not

    DetacherResult(List<ValueRange> ranges, List<Expression> accessConditions,
                   List<Expression> remainingConditions, boolean isDNFCondition) {
      this.ranges = ranges;
      this.accessConditions = accessConditions;
      this.remainingConditions = remainingConditions;
      //this.equalConditionCount = equalConditionCount;
      //this.equalOrInCount = equalOrInCount;
      this.isDNFCondition = isDNFCondition;
    }

    static DetacherResult empty() {
      return new DetacherResult(Collections.singletonList(RangeBuilder.fullRange()), Collections.emptyList(), Collections.emptyList(), false);
    }

    Tuple4<List<ValueRange>, List<Expression>, List<Expression>, Boolean> unwrap() {
      return Tuples.of(ranges, accessConditions, remainingConditions, isDNFCondition);
    }

  }
}
