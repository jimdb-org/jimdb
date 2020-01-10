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
package io.jimdb.sql.optimizer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import io.jimdb.core.Session;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.ExpressionType;
import io.jimdb.core.expression.ExpressionUtil;
import io.jimdb.core.expression.FuncExpr;
import io.jimdb.core.expression.Schema;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.expression.ValueExpr;
import io.jimdb.core.expression.functions.Func;
import io.jimdb.core.expression.functions.FuncDialect;
import io.jimdb.core.expression.functions.FuncType;
import io.jimdb.core.expression.functions.builtin.logic.AndFunc;
import io.jimdb.core.expression.functions.builtin.logic.OrFunc;
import io.jimdb.pb.Basepb;
import io.jimdb.core.types.Types;
import io.jimdb.core.types.ValueType;
import io.jimdb.core.values.LongValue;
import io.jimdb.core.values.Value;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import javafx.util.Pair;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

/**
 * ConstantPropagator is to propagate constant in conditions for sql.
 */
@SuppressFBWarnings({ "CLI_CONSTANT_LIST_INDEX", "FCCD_FIND_CLASS_CIRCULAR_DEPENDENCY",
        "NAB_NEEDLESS_BOOLEAN_CONSTANT_CONVERSION" })
public class ConstantPropagator {

  private static final int MAX_PROPAGATION_ROUND = 50;

  private Map<Long, Integer> col2IdxMap = new HashMap<>();

  // if eqList[i] != null, col_i = eqValExprMap[i]
  private Map<Integer, ValueExpr> eqValExprMap;

  //columns contains all the columns in the given expressions
  private List<ColumnExpr> columns;
  private Session session;
  private List<Expression> originalExprs;
  private List<Expression> resultExprs;

  public ConstantPropagator(List<Expression> expressions, Session session) {
    this.session = session;
    this.originalExprs = expressions;
  }

  public List<Expression> propagateConstants() {
    // extract all CNF items and columns from expressions
    columns = new ArrayList<>();
    resultExprs = new ArrayList<>();
    List<ColumnExpr> columnList = new ArrayList<>();
    for (Expression expression : originalExprs) {
      resultExprs.addAll(ExpressionUtil.splitCNFItems(expression));
      ExpressionUtil.extractColumns(columnList, expression, null);
    }

    // remove duplicate columns
    for (ColumnExpr columnExpr : columnList) {
      if (!col2IdxMap.containsKey(columnExpr.getUid())) {
        col2IdxMap.put(columnExpr.getUid(), col2IdxMap.size());
        columns.add(columnExpr);
      }
    }
    eqValExprMap = new HashMap<>();

    // process column = constant
    propagateColEqConst();

    // process columnX = columnY
    propagateColEqCol();

    // process DNF
    propagateConstDNF(session, resultExprs);

    return resultExprs;
  }

  private Pair<ColumnExpr, ValueExpr> extractColEqConst(Expression expression) {
    if (expression.getExprType() == ExpressionType.FUNC) {
      FuncExpr func = (FuncExpr) expression;
      if (func.getFuncType() == FuncType.Equality) {
        Expression[] args = func.getArgs();

        if (args[0] == null || args[0].getExprType() == ExpressionType.COLUMN) {
          if (args[1] == null || args[1].getExprType() == ExpressionType.CONST) {
            return new Pair((ColumnExpr) args[0], (ValueExpr) args[1]);
          }
        }
        if (args[1] == null || args[1].getExprType() == ExpressionType.COLUMN) {
          if (args[0] == null || args[0].getExprType() == ExpressionType.CONST) {
            return new Pair((ColumnExpr) args[1], (ValueExpr) args[0]);
          }
        }
      }
    }
    return new Pair(null, null);
  }

  private Tuple2<Boolean, Boolean> updateEqExprList(ColumnExpr columnExpr, ValueExpr valueExpr) {
    if (valueExpr.isNull()) {
      return Tuples.of(false, true);
    }

    int idx = col2IdxMap.get(columnExpr.getUid());
    ValueExpr originalValExpr = eqValExprMap.get(idx);
    if (originalValExpr != null) {
      boolean isEqual = valueExpr.equals(originalValExpr);
      return Tuples.of(false, !isEqual);
    }

    eqValExprMap.put(idx, valueExpr);
    return Tuples.of(true, false);
  }

  private Map<Integer, ValueExpr> getColEqConsts(HashSet<Integer> processedExprIdxSet) {
    Map<Integer, ValueExpr> colId2ConstMap = new HashMap<>();
    for (int i = 0; i < resultExprs.size(); i++) {
      if (processedExprIdxSet.contains(i)) {
        continue;
      }
      Expression expr = resultExprs.get(i);
      Pair<ColumnExpr, ValueExpr> cv = extractColEqConst(expr);
      ColumnExpr columnExpr = cv.getKey();
      ValueExpr valueExpr = cv.getValue();
      if (columnExpr == null) {
        if (expr.getExprType() == ExpressionType.CONST && valueExpr != null) {
          Value value = valueExpr.exec(ValueAccessor.EMPTY);
          if (value.getType() == ValueType.LONG) {
            if (((LongValue) value).getValue() == 0) {
              resultExprs.clear();
              resultExprs.add(ValueExpr.ZERO);
              return null;
            }
          }
        }
        continue;
      }

      processedExprIdxSet.add(i);
      Tuple2<Boolean, Boolean> valExprUpdate = updateEqExprList(columnExpr, valueExpr);
      if (valExprUpdate.getT2()) {
        resultExprs.clear();
        resultExprs.add(ValueExpr.ZERO);
        return null;
      }
      if (valExprUpdate.getT1()) {
        colId2ConstMap.put(i, valueExpr);
      }
    }

    return colId2ConstMap;
  }

  // process the situations like columnX=10
  private void propagateColEqConst() {
    HashSet<Integer> processedExprIdxSet = new HashSet<>();
    for (int i = 0; i < MAX_PROPAGATION_ROUND; i++) {
      Map<Integer, ValueExpr> colConstMap = getColEqConsts(processedExprIdxSet);
      if (colConstMap.isEmpty()) {
        return;
      }
      List<ColumnExpr> columnExprs = new ArrayList<>();
      List<Expression> valueExprs = new ArrayList<>();
      colConstMap.forEach((id, con) -> {
        columnExprs.add(columns.get(id));
        valueExprs.add(con);
      });

      Expression[] exprs = new Expression[valueExprs.size()];
      valueExprs.toArray(exprs);
      for (int j = 0; j < resultExprs.size(); j++) {
        Expression expr = resultExprs.get(j);
        if (!processedExprIdxSet.contains(j)) {
          Schema schema = new Schema(columnExprs);
          Expression newExpr = ExpressionUtil.substituteColumn(session, expr, schema, exprs);
          resultExprs.set(j, newExpr);
        }
      }
    }
  }

  /**
   * Used as return result for replaceColInExpr() method.
   */
  static class ExprReplaceResult {
    public boolean isReplaced;
    public boolean isExprUncertain;
    public Expression resultExpr;

    ExprReplaceResult(boolean isReplaced, boolean isExprUncertain, Expression resultExpr) {
      this.isReplaced = isReplaced;
      this.isExprUncertain = isExprUncertain;
      this.resultExpr = resultExpr;
    }
  }

  private ExprReplaceResult replaceColInExpr(
          ColumnExpr srcColumn, ColumnExpr tgtColumn, Expression expression) {
    // the expression should be a Func
    if (expression.getExprType() != ExpressionType.FUNC) {
      return new ExprReplaceResult(false, false, expression);
    }

    // the func should be foldable
    FuncExpr funcExpr = (FuncExpr) expression;
    FuncDialect dialect = funcExpr.getFuncType().getDialect();
    if (!dialect.isFoldable() || funcExpr.getFuncType() == FuncType.ISNULL) {
      return new ExprReplaceResult(false, true, expression);
    }

    // check each expression arg
    boolean isReplaced = false;
    Expression[] args = funcExpr.getArgs();
    Expression[] tempArgs = Arrays.copyOf(args, 0);
    for (int i = 0; i < args.length; i++) {
      ExpressionType argType = args[i].getExprType();
      if (argType == ExpressionType.COLUMN
              && srcColumn.getUid().longValue() == ((ColumnExpr) args[i]).getUid().longValue()) {
        isReplaced = true;
      } else {
        ExprReplaceResult subResult = replaceColInExpr(srcColumn, tgtColumn, args[i]);
        if (subResult.isExprUncertain) {
          return new ExprReplaceResult(false, true, expression);
        } else if (subResult.isReplaced) {
          isReplaced = true;
          tempArgs[i] = subResult.resultExpr;
        }
      }

      // return new func expression if replacement occurs
      if (isReplaced) {
        Func func = funcExpr.getFunc();
        func.setArgs(tempArgs);
        FuncExpr newFuncExpr = new FuncExpr(funcExpr.getFuncType(), func, funcExpr.getResultType());
        return new ExprReplaceResult(true, false, newFuncExpr);
      }
    }

    return new ExprReplaceResult(false, false, expression);
  }

  // process the situations like columnX=columnY
  private void propagateColEqCol() {
    HashSet<Integer> processedExprIdxSet = new HashSet<>();

    // unionSet contains all the column subsets like: a=b=c, d=e, x=y=z
    UnionSet unionSet = new UnionSet(columns.size());
    for (int i = 0; i < resultExprs.size(); i++) {
      Expression expr = resultExprs.get(i);
      if (expr.getExprType() == ExpressionType.FUNC) {
        FuncExpr func = (FuncExpr) expr;
        if (func.getFuncType() == FuncType.Equality) {
          Expression[] args = func.getArgs();
          if (args[0].getExprType() == ExpressionType.COLUMN
                  && args[1].getExprType() == ExpressionType.COLUMN) {
            ColumnExpr columnA = (ColumnExpr) args[0];
            ColumnExpr columnB = (ColumnExpr) args[1];
            int columnAIdx = col2IdxMap.get(columnA.getUid());
            int columnBIdx = col2IdxMap.get(columnB.getUid());
            unionSet.mergeSet(columnAIdx, columnBIdx);
            processedExprIdxSet.add(i);
          }
        }
      }
    }

    for (int i = 0; i < columns.size(); i++) {
      ColumnExpr columnA = columns.get(i);
      for (int j = i + 1; j < columns.size(); j++) {
        ColumnExpr columnB = columns.get(j);
        if (unionSet.findSetRoot(i) == unionSet.findSetRoot(j)) {
          for (int k = 0; k < resultExprs.size(); k++) {
            if (!processedExprIdxSet.contains(k)) {
              Expression expr = resultExprs.get(k);
              ExprReplaceResult replaceResult = replaceColInExpr(columnA, columnB, expr);
              if (replaceResult.isReplaced) {
                resultExprs.add(replaceResult.resultExpr);
              }
              replaceResult = replaceColInExpr(columnB, columnA, expr);
              if (replaceResult.isReplaced) {
                resultExprs.add(replaceResult.resultExpr);
              }
            }
          }
        }
      }
    }
  }

  public static Expression composeCNFCondition(Session session, List<Expression> expressions) {
    return composeExpressions(session, expressions, FuncType.BooleanAnd);
  }

  public static Expression composeDNFCondition(Session session, List<Expression> expressions) {
    return composeExpressions(session, expressions, FuncType.BooleanOr);
  }

  private static Expression composeExpressions(Session session, List<Expression> expressions, FuncType andOrType) {
    int len = expressions.size();
    if (len == 0) {
      return null;
    } else if (len == 1) {
      return expressions.get(0);
    } else {
      int m = len / 2;
      Expression[] newArgs = new Expression[2];
      newArgs[0] = composeExpressions(session, expressions.subList(0, m), andOrType);
      newArgs[1] = composeExpressions(session, expressions.subList(m, len), andOrType);

      Func func = new OrFunc.OrFuncBuilder(FuncType.BooleanOr.name()).build(session, newArgs);
      if (andOrType == FuncType.BooleanAnd) {
        func = new AndFunc.AndFuncBuilder(FuncType.BooleanAnd.name()).build(session, newArgs);
      }
      return new FuncExpr(andOrType, func, Types.buildSQLType(Basepb.DataType.BigInt));
    }
  }

  private void propagateConstDNF(Session session, List<Expression> expressions) {
    for (int i = 0; i < expressions.size(); i++) {
      Expression expression = expressions.get(i);
      if (expression.getExprType() == ExpressionType.FUNC) {
        FuncExpr funcExpr = (FuncExpr) expression;
        if (funcExpr.getFuncType() == FuncType.BooleanOr) {
          List<Expression> dnfExprs = ExpressionUtil.splitDNFItems(funcExpr);
          for (int j = 0; j < dnfExprs.size(); j++) {
            List<Expression> tempList = new ArrayList<>();
            tempList.add(dnfExprs.get(j));
            ConstantPropagator constProp = new ConstantPropagator(tempList, session);
            List<Expression> expressions1 = constProp.propagateConstants();
            Expression newAndExpr = composeExpressions(session, expressions1, FuncType.BooleanAnd);
            dnfExprs.set(j, newAndExpr);
          }
          Expression newOrExpr = composeExpressions(session, dnfExprs, FuncType.BooleanOr);
          expressions.set(i, newOrExpr);
        }
      }
    }
  }
}
