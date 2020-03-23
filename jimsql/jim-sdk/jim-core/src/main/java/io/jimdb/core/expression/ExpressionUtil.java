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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import io.jimdb.core.Session;
import io.jimdb.core.expression.functions.Func;
import io.jimdb.core.expression.functions.FuncType;
import io.jimdb.core.expression.functions.builtin.ValuesFunc;
import io.jimdb.core.model.meta.Column;
import io.jimdb.core.model.meta.Index;
import io.jimdb.core.model.meta.Table;
import io.jimdb.core.types.Types;
import io.jimdb.core.types.ValueType;
import io.jimdb.core.values.LongValue;
import io.jimdb.core.values.NullValue;
import io.jimdb.core.values.Value;
import io.jimdb.core.values.ValueConvertor;
import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.pb.Basepb;
import io.jimdb.pb.Metapb.SQLType;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

/**
 * @version V1.0
 */
@SuppressFBWarnings({ "FCCD_FIND_CLASS_CIRCULAR_DEPENDENCY", "FII_USE_FUNCTION_IDENTITY", "LII_LIST_INDEXED_ITERATING", "PSC_SUBOPTIMAL_COLLECTION_SIZING" })
public final class ExpressionUtil {
  private ExpressionUtil() {
  }

  public static List<Expression> splitNFItems(Expression expr, FuncType funcType) {
    List<Expression> result = new ArrayList<>();
    if (expr.getExprType() == ExpressionType.FUNC) {
      FuncExpr funcExpr = (FuncExpr) expr;
      if (funcExpr.getFuncType() == funcType) {
        for (Expression exp : funcExpr.getArgs()) {
          result.addAll(splitNFItems(exp, funcType));
        }
        return result;
      }
    }

    result.add(expr);
    return result;
  }

  public static List<Expression> splitCNFItems(Expression expression) {
    return splitNFItems(expression, FuncType.BooleanAnd);
  }

  public static List<Expression> splitDNFItems(Expression expression) {
    return splitNFItems(expression, FuncType.BooleanOr);
  }

  public static Schema buildSchema(Session session, String dbName, Table table) {
    List<ColumnExpr> columns = buildColumnExpr(session, dbName, table.getName(), table.getReadableColumns());
    Schema schema = new Schema(columns);

    for (Index index : table.getReadableIndices()) {
      if (!index.isUnique()) {
        continue;
      }

      boolean flag = true;
      KeyColumn key = new KeyColumn(index.getColumns().length);
      for (Column idxCol : index.getColumns()) {
        int i = -1;
        boolean found = false;
        for (Column tblCol : table.getReadableColumns()) {
          i++;
          if (idxCol.getId().equals(tblCol.getId())) {
            if (tblCol.getType().getNotNull()) {
              key.addColumnExpr(columns.get(i));
              found = true;
            }
            break;
          }
        }

        if (!found) {
          flag = false;
          break;
        }
      }

      if (flag) {
        schema.addKeyColumn(key);
      }
    }

    return schema;
  }

  public static List<ColumnExpr> buildColumnExpr(Session session, String dbName, String tblName, Column[] columns) {
    List<ColumnExpr> colExprs = new ArrayList<>(columns.length);
    for (Column column : columns) {
      ColumnExpr col = buildColumnExpr(session, dbName, tblName, column);
      colExprs.add(col);
    }
    return colExprs;
  }

  public static ColumnExpr buildColumnExpr(Session session, String dbName, String tblName, Column column) {
    ColumnExpr col = new ColumnExpr(session.allocColumnID(), column);
    col.setOriTable(tblName);
    col.setAliasTable(tblName);
    col.setCatalog(dbName);
    return col;
  }

  public static FuncExpr buildValuesFunc(Session session, int offset, SQLType type) {
    final Func func = ValuesFunc.build(session, offset, type);
    return new FuncExpr(FuncType.VALUES, func, type);
  }

  public static Tuple2<Expression, Boolean> foldConst(Expression expr) {
    switch (expr.getExprType()) {
      case FUNC:
        final FuncExpr funcExpr = (FuncExpr) expr;
        if (!funcExpr.getFuncType().getDialect().isFoldable()) {
          return Tuples.of(expr, Boolean.FALSE);
        }

        final Session session = funcExpr.getSession();
        final Expression[] args = funcExpr.getArgs();
        final boolean[] consts = new boolean[args.length];
        boolean isNull = false;
        boolean isAllConst = true;
        boolean isLazyConst = false;
        for (int i = 0; i < args.length; i++) {
          final Tuple2<Expression, Boolean> foldArg = foldConst(args[i]);
          final Expression foldArgExpr = foldArg.getT1();
          args[i] = foldArgExpr;
          boolean isConst = foldArgExpr.getExprType() == ExpressionType.CONST;
          consts[i] = isConst;
          isAllConst = isAllConst && isConst;
          isNull = isNull || (isConst && ((ValueExpr) foldArgExpr).isNull());
          isLazyConst = isLazyConst || foldArg.getT2();
        }

        if (!isAllConst) {
          if (!isNull || !session.getStmtContext().isNullReject() || funcExpr.getFuncType() == FuncType.LessThanOrEqualOrGreaterThan) {
            return Tuples.of(expr, Boolean.valueOf(isLazyConst));
          }

          final Expression[] constArgs = new Expression[args.length];
          for (int i = 0; i < args.length; i++) {
            if (consts[i]) {
              constArgs[i] = args[i];
            } else {
              constArgs[i] = ValueExpr.ONE;
            }
          }

          try {
            final Expression dummyFunc = FuncExpr.build(session, funcExpr.getFuncType(), funcExpr.resultType, false,
                    constArgs);
            final Value value = dummyFunc.exec(ValueAccessor.EMPTY);

            if (value == null || value.isNull()) {
              return isLazyConst ? Tuples.of(new ValueExpr(value, funcExpr.resultType, funcExpr), Boolean.TRUE)
                      : Tuples.of(new ValueExpr(NullValue.getInstance(), funcExpr.resultType), Boolean.FALSE);
            }
            if (!ValueConvertor.convertToBool(session, value)) {
              return isLazyConst ? Tuples.of(new ValueExpr(value, funcExpr.resultType, funcExpr), Boolean.TRUE)
                      : Tuples.of(new ValueExpr(NullValue.getInstance(), funcExpr.resultType), Boolean.FALSE);
            }

            return Tuples.of(expr, Boolean.valueOf(isLazyConst));
          } catch (Exception ex) {
            return Tuples.of(expr, Boolean.valueOf(isLazyConst));
          }
        }

        try {
          final Value value = funcExpr.exec(ValueAccessor.EMPTY);
          return isLazyConst ? Tuples.of(new ValueExpr(value, funcExpr.resultType, funcExpr), Boolean.TRUE)
                  : Tuples.of(new ValueExpr(value == null ? NullValue.getInstance() : value, funcExpr.resultType),
                  Boolean.FALSE);
        } catch (Exception ex) {
          return Tuples.of(expr, Boolean.valueOf(isLazyConst));
        }

      case CONST:
        final ValueExpr constExpr = (ValueExpr) expr;
        if (constExpr.getLazyExpr() != null) {
          try {
            final Value value = constExpr.getLazyExpr().exec(ValueAccessor.EMPTY);
            return Tuples.of(new ValueExpr(value, constExpr.resultType, constExpr.getLazyExpr()), Boolean.TRUE);
          } catch (Exception ex) {
            return Tuples.of(expr, Boolean.TRUE);
          }
        }
        break;
    }
    return Tuples.of(expr, Boolean.FALSE);
  }

  public static Tuple2<ValueExpr, Boolean> rewriteComparedConstant(Session session, ValueExpr valueExpr,
                                                                   SQLType retType, FuncType funcType) {
    try {
      Value val = valueExpr.exec(ValueAccessor.EMPTY);
      Value intVal = ValueConvertor.convertType(session, val, retType);
      int comp = intVal.compareTo(session, valueExpr.getValue());
      if (comp == 0) {
        return Tuples.of(new ValueExpr(intVal, retType, valueExpr.getLazyExpr()), Boolean.FALSE);
      }

      switch (funcType) {
        case Equality:
        case LessThanOrEqualOrGreaterThan:
          switch (Types.sqlToValueType(valueExpr.getResultType())) {
            case DOUBLE:
            case DECIMAL:
              return Tuples.of(valueExpr, Boolean.TRUE);
            case STRING:
              Value dVal = ValueConvertor.convertType(session, val, ValueType.DOUBLE, null);
              comp = dVal.compareTo(session, intVal);
              return comp == 0 ? Tuples.of(new ValueExpr(intVal, retType, valueExpr.getLazyExpr()), Boolean.FALSE)
                      : Tuples.of(valueExpr, Boolean.TRUE);
          }
        case GreaterThan:
        case LessThanOrEqual:
          Expression floorValue = FuncExpr.build(session, FuncType.Floor, Types.buildSQLType(Basepb.DataType.BigInt), true,
                  valueExpr);
          return Tuples.of((ValueExpr) floorValue, Boolean.FALSE);
        case GreaterThanOrEqual:
        case LessThan:
          Expression ceilValue = FuncExpr.build(session, FuncType.Ceil, Types.buildSQLType(Basepb.DataType.BigInt), true,
                  valueExpr);
          return Tuples.of((ValueExpr) ceilValue, Boolean.FALSE);
      }
    } catch (DBException dbEx) {
      if (dbEx.getCode() == ErrorCode.ER_SYSTEM_VALUE_OVER_FLOW_UP) {
        return Tuples.of(new ValueExpr(LongValue.getInstance(Long.MAX_VALUE), retType, valueExpr), Boolean.TRUE);
      }
      if (dbEx.getCode() == ErrorCode.ER_SYSTEM_VALUE_OVER_FLOW_DOWN) {
        return Tuples.of(new ValueExpr(LongValue.getInstance(Long.MIN_VALUE), retType, valueExpr), Boolean.TRUE);
      }
      return Tuples.of(valueExpr, Boolean.FALSE);
    } catch (Exception ex) {
      return Tuples.of(valueExpr, Boolean.FALSE);
    }

    return Tuples.of(valueExpr, Boolean.FALSE);
  }

  public static Tuple2<Boolean, Boolean> execBoolean(Session session, ValueAccessor row, Expression... cnfExprs) {
    if (cnfExprs == null) {
      return Tuples.of(Boolean.FALSE, Boolean.FALSE);
    }

    try {
      Value data = null;
      Boolean hasNull = Boolean.FALSE;
      for (Expression expression : cnfExprs) {
        data = expression.exec(row);
        if (data == null || data.isNull()) {
          if (!isEQCondFromIn(expression)) {
            return Tuples.of(Boolean.FALSE, Boolean.FALSE);
          }

          hasNull = Boolean.TRUE;
          continue;
        }

        boolean flag = ValueConvertor.convertToBool(session, data);
        if (!flag) {
          return Tuples.of(Boolean.FALSE, Boolean.FALSE);
        }
      }

      if (hasNull) {
        return Tuples.of(Boolean.FALSE, Boolean.TRUE);
      }
      return Tuples.of(Boolean.TRUE, Boolean.FALSE);
    } catch (Exception ex) {
      return Tuples.of(Boolean.FALSE, Boolean.FALSE);
    }
  }

  public static boolean isEQCondFromIn(Expression expression) {
    if (expression == null || expression.getExprType() != ExpressionType.FUNC) {
      return false;
    }

    final FuncExpr funcExpr = (FuncExpr) expression;
    if (funcExpr.getFuncType() != FuncType.Equality) {
      return false;
    }

    List<ColumnExpr> exprlist = new ArrayList<>(8);
    extractColumns(exprlist, Arrays.asList(funcExpr.getArgs()), ColumnExpr::isInSubQuery);
    return exprlist.size() > 0;
  }

  public static Set<Long> extractColumnIds(List<Expression> expressions) {
    Set<Long> result = Sets.newHashSet();
    if (expressions == null || expressions.isEmpty()) {
      return result;
    }

    for (Expression expression : expressions) {
      switch (expression.getExprType()) {
        case COLUMN:
          final ColumnExpr colExpr = (ColumnExpr) expression;
          result.add(colExpr.getUid());
          break;

        case FUNC:
          final FuncExpr funcExpr = (FuncExpr) expression;
          // TODO ger rid of this conversion
          result.addAll(extractColumnIds(Arrays.asList(funcExpr.getArgs())));
          break;

        default:
          break;
      }
    }

    return result;
  }

  public static List<ColumnExpr> extractColumns(Expression expression) {
    List<ColumnExpr> result = Lists.newArrayListWithCapacity(8);
    extractColumns(result, expression, null);
    return result;
  }

  // TODO Refactor all these "extractColumns" functions. Putting outExprs into the function args is really a bad practice.
  public static void extractColumns(List<ColumnExpr> outExprs, List<Expression> exprs,
                                    Predicate<ColumnExpr> predicate) {
    if (exprs == null) {
      return;
    }

    for (Expression expr : exprs) {
      extractColumns(outExprs, expr, predicate);
    }
  }

  public static void extractColumns(List<ColumnExpr> outExprs, Expression expression, Predicate<ColumnExpr> predicate) {
    switch (expression.getExprType()) {
      case COLUMN:
        final ColumnExpr colExpr = (ColumnExpr) expression;
        if (predicate == null || colExpr.isInSubQuery()) {
          outExprs.add(colExpr);
        }
        break;

      case FUNC:
        final FuncExpr funcExpr = (FuncExpr) expression;
        for (Expression arg : funcExpr.getArgs()) {
          extractColumns(outExprs, arg, predicate);
        }
    }
  }

  public static void extractColumnIds(List<Integer> outExprIds, List<Expression> exprs,
                                    Predicate<ColumnExpr> predicate) {
    if (exprs == null) {
      return;
    }

    for (Expression expr : exprs) {
      extractColumnIds(outExprIds, expr, predicate);
    }
  }

  public static void extractColumnIds(List<Integer> outExprIds, Expression expression, Predicate<ColumnExpr> predicate) {
    switch (expression.getExprType()) {
      case COLUMN:
        final ColumnExpr colExpr = (ColumnExpr) expression;
        if (predicate == null || colExpr.isInSubQuery()) {
          outExprIds.add(colExpr.getId());
        }
        break;

      case FUNC:
        final FuncExpr funcExpr = (FuncExpr) expression;
        for (Expression arg : funcExpr.getArgs()) {
          extractColumnIds(outExprIds, arg, predicate);
        }
    }
  }



  public static Expression substituteColumn(Session session, Expression expression,
                                            Schema schema, Expression[] newExpressions) {
    switch (expression.getExprType()) {
      case COLUMN:
        ColumnExpr columnExpr = (ColumnExpr) expression;
        int index = schema.getColumnIndex(columnExpr);
        if (index == -1) {
          return expression;
        }

        // TODO indicate whether this column is the inner operand of column equal condition converted from `[not] in
        //  (subq)`

        Expression newExpression = newExpressions[index];
        if (columnExpr.isInSubQuery()) {
          newExpression = setColumnSubQuery(newExpression);
        }
        return newExpression;

      case FUNC:
        FuncExpr funcExpr = (FuncExpr) expression;
        if (funcExpr.getFuncType() == FuncType.Cast) {
          FuncExpr newFunc = funcExpr.clone();
          newFunc.getArgs()[0] = substituteColumn(session, newFunc.getArgs()[0], schema, newExpressions);
          return newFunc;
        }
        Expression[] args = funcExpr.getArgs();
        List<Expression> newArgs = new ArrayList<>(args.length);
        Arrays.stream(args).forEach(arg -> newArgs.add(substituteColumn(session, arg, schema, newExpressions)));

        return FuncExpr.build(session, funcExpr.getFuncType(), funcExpr.getResultType(),
                true, newArgs.toArray(new Expression[newArgs.size()]));
    }
    return expression;
  }

  private static Expression setColumnSubQuery(Expression expression) {
    switch (expression.getExprType()) {
      case COLUMN:
        ColumnExpr columnExpr = (ColumnExpr) expression;
        return columnExpr.clone().setInSubQuery(true);

      case FUNC:
        FuncExpr funcExpr = (FuncExpr) expression;
        Expression[] args = funcExpr.getArgs();
        for (int i = 0; i < args.length; i++) {
          args[i] = setColumnSubQuery(args[i]);
        }
        break;

      default:
        break;
    }

    return expression;
  }

  // TODO is this correct? or should we use id/uid to check?
  public static ColumnExpr idxCol2ColExpr(Column column, List<ColumnExpr> columnExprs) {
    if (column == null || columnExprs == null) {
      return null;
    }
    ColumnExpr result = null;
    for (ColumnExpr columnExpr : columnExprs) {
      if (null != column.getName() && column.getName().equals(columnExpr.getAliasCol())) {
        result = columnExpr;
        break;
      }
    }

    return result;
  }

  public static ColumnExpr columnToColumnExpr(Column column, List<ColumnExpr> columnExprs) {
    Map<Integer, ColumnExpr> map = columnExprs.stream().collect(Collectors.toMap(ColumnExpr::getId, columnExpr -> columnExpr));
    return map.get(column.getId());
  }

  // If the index has three Columns that the 1st and 3rd Column have the corresponding ColumnExpr,
  // only the 1st corresponding ColumnExpr will be returned.
  public static List<ColumnExpr> indexToColumnExprs(Index index, List<ColumnExpr> columnExprs) {
    int size = columnExprs.size();
    Map<Integer, ColumnExpr> map = new HashMap<>(size);
    for (int i = 0; i < size; i++) {
      map.put(columnExprs.get(i).getId(), columnExprs.get(i));
    }

    Integer key;
    List<ColumnExpr> result = new ArrayList<>(size);
    for (Column column : index.getColumns()) {
      key = column.getId();
      if (!map.containsKey(key)) {
        return result;
      }

      result.add(map.get(key));
    }

    return result;
  }

  public static boolean containsExpr(List<Expression> exprs, Expression expr) {
    for (Expression expression : exprs) {
      if (expr != null && expr.equals(expression)) {
        return true;
      }
    }

    return false;
  }

  public static boolean containsExpr(Expression[] exprs, Expression expr) {
    for (Expression expression : exprs) {
      if (expr != null && expr.equals(expression)) {
        return true;
      }
    }

    return false;
  }

  public static Optional<Column> getPkCol(Column[] columns) {
    return Arrays.stream(columns).filter(Column::isPrimary).findFirst();
  }
}
