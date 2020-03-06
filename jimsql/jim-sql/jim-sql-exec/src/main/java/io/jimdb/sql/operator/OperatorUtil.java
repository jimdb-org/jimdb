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
package io.jimdb.sql.operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import io.jimdb.core.Session;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.TableAccessPath;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.expression.aggregate.AggregateExpr;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.values.Value;

import org.apache.commons.lang3.StringUtils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * OperatorUtil
 *
 * @since 2019-08-16
 */
@SuppressFBWarnings({"ITC_INHERITANCE_TYPE_CHECKING", "CLI_CONSTANT_LIST_INDEX",
        "FCCD_FIND_CLASS_CIRCULAR_DEPENDENCY", "PSC_PRESIZE_COLLECTIONS", "STT_TOSTRING_MAP_KEYING",
        "CFS_CONFUSING_FUNCTION_SEMANTICS", "PDP_POORLY_DEFINED_PARAMETER" })
public final class OperatorUtil {
  private OperatorUtil() {
  }

  public static String printRelOperatorTree(Operator operator) {
    List<String> strs = new ArrayList<>();
    toStringIntern(operator, strs, false);
    return String.join(" -> ", strs);
  }

  public static String printRelOperatorTree(Operator operator, boolean detail) {
    List<String> strs = new ArrayList<>();
    toStringIntern(operator, strs, detail);
    return String.join(" -> ", strs);
  }

  private static void toStringIntern(@Nonnull Operator op, List<String> strs, boolean detail) {
    Objects.requireNonNull(op);
    Operator.OperatorType operatorType = op.getOperatorType();
    switch (operatorType) {
      case LOGIC:
        RelOperator logicOp = (RelOperator) op;
        if (logicOp.getChildren() != null) {
          for (RelOperator child : logicOp.getChildren()) {
            toStringIntern(child, strs, detail);
          }
        }
        break;
      default:
        break;
    }

    if (op instanceof Projection) {
      strs.add("Projection");
      return;
    }
    if (op instanceof Order) {
      strs.add("Order");
      return;
    }
    if (op instanceof Limit) {
      strs.add("Limit");
      return;
    }
    if (op instanceof DualTable) {
      strs.add("Dual");
      return;
    }
    if (op instanceof TopN) {
      TopN topN = (TopN) op;
      strs.add(topN.toString());
      return;
    }

    if (op instanceof Aggregation) {
      Aggregation aggregation = (Aggregation) op;
      String aggStr = "HashAgg(";
      if (aggregation.getAggOpType() == Aggregation.AggOpType.StreamAgg) {
        aggStr = "StreamAgg(";
      }
      final StringBuilder stringBuilder = new StringBuilder(aggStr);

      for (AggregateExpr aggregateExpr : aggregation.getAggregateExprs()) {
        stringBuilder.append(aggregateExpr).append(',');
      }
      strs.add(stringBuilder.deleteCharAt(stringBuilder.length() - 1).append(')').toString());
      return;
    }

    if (op instanceof Selection) {
      Selection selection = (Selection) op;
      final StringBuilder builder = new StringBuilder("Selection(");
      selection.getConditions().forEach(condition -> {
        builder.append(condition).append(',');
      });
      strs.add(builder.deleteCharAt(builder.length() - 1).append(')').toString());
      return;
    }

    if (op instanceof TableSource) {
      TableSource tableSource = (TableSource) op;
      final StringBuilder builder = new StringBuilder();
      String aliasName = tableSource.getAliasName();
      builder.append(String.format("TableSource(%s)", StringUtils.isNotEmpty(aliasName) ? aliasName : tableSource.getTable().getName()));

      if (detail) {
        if (tableSource.getTableAccessPaths() != null && !tableSource.getTableAccessPaths().isEmpty()) {
          builder.append(",path:[");
          for (TableAccessPath path : tableSource.getTableAccessPaths()) {
            builder.append("{index:").append(path.getIndex().getName());
            if (path.getAccessConditions() != null) {
              builder.append(",access:").append(path.getAccessConditions());
            }
            if (path.getIndexConditions() != null) {
              builder.append(",index:").append(path.getIndexConditions());
            }
            if (path.getTableConditions() != null) {
              builder.append(",table:").append(path.getTableConditions());
            }
            builder.append('}');
          }
          builder.append(']');
        }
        if (tableSource.getPushDownPredicates() != null) {
          builder.append(",pushPredicates:[");
          for (Expression expression : tableSource.getPushDownPredicates()) {
            builder.append(expression).append(',');
          }
          builder.deleteCharAt(builder.length() - 1).append(']');
        }
        if (tableSource.getAllPredicates() != null) {
          builder.append(",allPredicates:[");
          for (Expression expression : tableSource.getAllPredicates()) {
            builder.append(expression).append(',');
          }
          builder.deleteCharAt(builder.length() - 1).append(']');
        }

      }
      strs.add(builder.toString());
      return;
    }

    if (op instanceof IndexSource) {
      IndexSource indexSource = (IndexSource) op;
      String str = String.format("IndexSource(%s)", indexSource.getTable().getName());
      strs.add(str);
      return;
    }

    if (op instanceof KeyGet) {
      KeyGet keyGet = (KeyGet) op;
      strs.add(keyGet.toString());
      return;
    }

    if (op instanceof IndexLookup) {
      IndexLookup indexLookup = (IndexLookup) op;
      toStringIntern(indexLookup.getPushedDownIndexPlan(), strs, detail);
      toStringIntern(indexLookup.getPushedDownTablePlan(), strs, detail);
      strs.add("IndexLookUp");
      return;
    }

    strs.add("Unknown");
  }

  public static String printPushDownedProcessors(Operator op) {
    Objects.requireNonNull(op);
    RelOperator operator = (RelOperator) op;
    Operator.OperatorType operatorType = operator.getOperatorType();
    if (operatorType != Operator.OperatorType.LOGIC) {
      return "";
    }

    if (operator instanceof TableSource) {
      List processors = ((TableSource) operator).getProcessors();
      if (processors != null) {
        return " table-plan-proto: " + processors;
      }
    } else if (operator instanceof IndexSource) {
      List processors = ((IndexSource) operator).getProcessors();
      if (processors != null) {
        return " index-plan-proto: " + processors;
      }
    } else if (operator instanceof IndexLookup) {
      IndexLookup indexLookup = (IndexLookup) operator;
      return "IndexLookup: " + printPushDownedProcessors(indexLookup.getTableSource()) + ","
              + printPushDownedProcessors(indexLookup.getIndexSource());
    }

    StringBuilder result = new StringBuilder();
    for (RelOperator child : operator.getChildren()) {
      result.append(printPushDownedProcessors(child));
    }
    return result.toString();
  }

  public static String printRelOperatorTreeAsc(Operator operator, boolean detail) {
    List<String> strs = new ArrayList<>();
    toStringIntern(operator, strs, detail);
    Collections.reverse(strs);
    return String.join(" -> ", strs);
  }

  /**
   * Order by Multiple
   */
  public static ExecResult orderBy(Session session, ExecResult result, Order.OrderExpression[] orderExpressions) {
    result.accept(rows -> Arrays.parallelSort(rows, (o1, o2) -> compare(session, orderExpressions, o1, o2)));
    return result;
  }

  public static int compare(Session session, Order.OrderExpression[] orderExpressions, ValueAccessor o1, ValueAccessor o2) {

    int c = 0;
    for (Order.OrderExpression expr :orderExpressions) {
      Value v1 = expr.getExpression().exec(o1);
      Value v2 = expr.getExpression().exec(o2);
      boolean isAsc = expr.getOrderType();
      c = isAsc ? v1.compareTo(session, v2) : v2.compareTo(session, v1);
      if (c != 0) {
        break;
      }
    }
    return c;
  }
}
