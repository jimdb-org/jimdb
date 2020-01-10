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
package io.jimdb.sql.analyzer;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import io.jimdb.core.SQLAnalyzer;
import io.jimdb.core.Session;
import io.jimdb.core.context.PrepareContext;
import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.exception.JimException;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.ExpressionUtil;
import io.jimdb.core.expression.FuncExpr;
import io.jimdb.core.expression.Schema;
import io.jimdb.core.expression.ValueExpr;
import io.jimdb.core.expression.functions.FuncType;
import io.jimdb.pb.Basepb.DataType;
import io.jimdb.pb.Metapb.SQLType;
import io.jimdb.sql.operator.RelOperator;
import io.jimdb.core.types.Types;
import io.jimdb.core.values.LongValue;
import io.jimdb.core.values.StringValue;
import io.jimdb.core.values.ValueConvertor;

import com.alibaba.druid.sql.ast.SQLArgument;
import com.alibaba.druid.sql.ast.SQLArrayDataType;
import com.alibaba.druid.sql.ast.SQLCommentHint;
import com.alibaba.druid.sql.ast.SQLDataType;
import com.alibaba.druid.sql.ast.SQLDeclareItem;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLKeep;
import com.alibaba.druid.sql.ast.SQLLimit;
import com.alibaba.druid.sql.ast.SQLMapDataType;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.SQLOver;
import com.alibaba.druid.sql.ast.SQLParameter;
import com.alibaba.druid.sql.ast.SQLPartition;
import com.alibaba.druid.sql.ast.SQLPartitionByHash;
import com.alibaba.druid.sql.ast.SQLPartitionByList;
import com.alibaba.druid.sql.ast.SQLPartitionByRange;
import com.alibaba.druid.sql.ast.SQLPartitionValue;
import com.alibaba.druid.sql.ast.SQLRecordDataType;
import com.alibaba.druid.sql.ast.SQLStructDataType;
import com.alibaba.druid.sql.ast.SQLSubPartition;
import com.alibaba.druid.sql.ast.SQLSubPartitionByHash;
import com.alibaba.druid.sql.ast.SQLSubPartitionByList;
import com.alibaba.druid.sql.ast.SQLWindow;
import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.druid.sql.ast.expr.SQLAllColumnExpr;
import com.alibaba.druid.sql.ast.expr.SQLAllExpr;
import com.alibaba.druid.sql.ast.expr.SQLAnyExpr;
import com.alibaba.druid.sql.ast.expr.SQLArrayExpr;
import com.alibaba.druid.sql.ast.expr.SQLBetweenExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExprGroup;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLBooleanExpr;
import com.alibaba.druid.sql.ast.expr.SQLCaseExpr;
import com.alibaba.druid.sql.ast.expr.SQLCaseStatement;
import com.alibaba.druid.sql.ast.expr.SQLCastExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLContainsExpr;
import com.alibaba.druid.sql.ast.expr.SQLCurrentOfCursorExpr;
import com.alibaba.druid.sql.ast.expr.SQLDateExpr;
import com.alibaba.druid.sql.ast.expr.SQLDefaultExpr;
import com.alibaba.druid.sql.ast.expr.SQLExistsExpr;
import com.alibaba.druid.sql.ast.expr.SQLFlashbackExpr;
import com.alibaba.druid.sql.ast.expr.SQLGroupingSetExpr;
import com.alibaba.druid.sql.ast.expr.SQLHexExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLInListExpr;
import com.alibaba.druid.sql.ast.expr.SQLInSubQueryExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntervalExpr;
import com.alibaba.druid.sql.ast.expr.SQLListExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.expr.SQLNCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLNotExpr;
import com.alibaba.druid.sql.ast.expr.SQLNullExpr;
import com.alibaba.druid.sql.ast.expr.SQLNumberExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.druid.sql.ast.expr.SQLRealExpr;
import com.alibaba.druid.sql.ast.expr.SQLSequenceExpr;
import com.alibaba.druid.sql.ast.expr.SQLSomeExpr;
import com.alibaba.druid.sql.ast.expr.SQLTimestampExpr;
import com.alibaba.druid.sql.ast.expr.SQLUnaryExpr;
import com.alibaba.druid.sql.ast.expr.SQLValuableExpr;
import com.alibaba.druid.sql.ast.expr.SQLValuesExpr;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.druid.sql.ast.statement.*;

import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

/**
 * @version V1.0
 */
public abstract class ExpressionAnalyzer implements SQLAnalyzer {
//  private static final Map<String, DataType> DATA_TYPE_MAP;

  protected boolean isScalar;
  protected boolean isUserScalar;
  protected boolean isExprAnalyzer;
  protected int forbidFoldCounter;
  protected SQLObject expr;
  protected RelOperator currentOp;

  // only for insert-on-duplicate.
  protected Schema insertSchema;
  protected Schema schema;
  protected Session session;
  protected ClauseType clause = ClauseType.NULL;
  protected Map<SQLAggregateExpr, Integer> aggMapper;
  protected Function<SQLObject, SQLObject> preFunc;
  protected Deque<Expression> stack;

//  static {
//    DATA_TYPE_MAP = new HashMap<>();
//    DATA_TYPE_MAP.put(INT, DataType.Int);
//    DATA_TYPE_MAP.put(BIGINT, DataType.BigInt);
//    DATA_TYPE_MAP.put(SMALLINT, DataType.SmallInt);
//    DATA_TYPE_MAP.put(TINYINT, DataType.TinyInt);
//    DATA_TYPE_MAP.put(BOOLEAN, DataType.TinyInt);
//    DATA_TYPE_MAP.put(REAL, DataType.Double);
//    DATA_TYPE_MAP.put(DECIMAL, DataType.Decimal);
//    DATA_TYPE_MAP.put(NUMBER, DataType.Decimal);
//    DATA_TYPE_MAP.put(VARCHAR, DataType.Varchar);
//    DATA_TYPE_MAP.put(TEXT, DataType.Varchar);
//    DATA_TYPE_MAP.put(DATE, DataType.Date);
//    DATA_TYPE_MAP.put(TIMESTAMP, DataType.TimeStamp);
//    DATA_TYPE_MAP.put(CHAR, DataType.Char);
//    DATA_TYPE_MAP.put(NCHAR, DataType.NChar);
//    DATA_TYPE_MAP.put(BYTEA, DataType.Binary);
//  }

  /**
   *
   */
  public enum ClauseType {
    NULL(""),
    FIELDLIST("field list"),
    HAVING("having clause"),
    ON("on clause"),
    ORDER("order clause"),
    WHERE("where clause"),
    GROUP("group clause"),
    SHOW("show statement"),
    WINDOW("window statement");

    private final String value;

    ClauseType(final String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }

  }

  public ExpressionAnalyzer() {
    this.isScalar = true;
    this.isUserScalar = false;
    this.stack = new ArrayDeque<>(8);
  }

  public void setOperator(RelOperator currentOp) {
    this.currentOp = currentOp;
    if (currentOp != null) {
      this.schema = currentOp.getSchema();
    }
  }

  public void setExprAnalyzer(boolean exprAnalyzer) {
    isExprAnalyzer = exprAnalyzer;
  }

  @Override
  public void setSession(Session session) {
    this.session = session;
  }

  public void setInsertTable(Schema table) {
    this.insertSchema = table;
  }

  public void setSchema(Schema schema) {
    this.schema = schema;
  }

  public void setSQLExpr(SQLObject expr) {
    this.expr = expr;
  }

  public void setClause(ClauseType clause) {
    this.clause = clause;
  }

  public ClauseType getClause() {
    return clause;
  }

  public void setScalar(boolean isScalar) {
    this.isScalar = isScalar;
    this.isUserScalar = true;
  }

  public void setAggMapper(Map<SQLAggregateExpr, Integer> aggMapper) {
    this.aggMapper = aggMapper;
  }

  public void setPreFunc(Function<SQLObject, SQLObject> preFunc) {
    this.preFunc = preFunc;
  }

  @Override
  public void reset() {
    this.isScalar = true;
    this.isUserScalar = false;
    this.isExprAnalyzer = false;
    this.forbidFoldCounter = 0;
    this.currentOp = null;
    this.expr = null;
    this.schema = null;
    this.insertSchema = null;
    this.session = null;
    this.aggMapper = null;
    this.preFunc = null;
    this.clause = ClauseType.NULL;
    this.stack.clear();
  }

  public Tuple2<Optional<Expression>, RelOperator> doAnalyze() throws JimException {
    expr.accept(this);
    if (!isScalar && stack.isEmpty()) {
      return Tuples.of(Optional.ofNullable(null), currentOp);
    }

    if (stack.size() != 1) {
      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_EXPR_REWRITE_STACK_INVALID, Integer.toString(stack
              .size()));
    }
    return Tuples.of(Optional.of(stack.pollFirst()), currentOp);
  }

  @Override
  public boolean visit(final SQLAggregateExpr e) {
    Integer index = null;
    if (aggMapper != null) {
      index = aggMapper.get(e);
    }
    if (index == null) {
      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_INVALID_GROUP_FUNC_USE);
    }

    stack.addFirst(schema.getColumn(index));
    if (!isUserScalar) {
      isScalar = false;
    }
    return false;
  }

  @Override
  public boolean visit(SQLBinaryOpExpr e) {
    if (isCompareSubquery(e)) {
      if (!isUserScalar) {
        isScalar = false;
      }
      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_NOT_SUPPORTED_YET, "Compare Subquery");
    }
    return true;
  }

  @Override
  public boolean visit(SQLPropertyExpr x) {
    return false;
  }

  @Override
  public boolean visit(SQLInSubQueryExpr e) {
    if (!isUserScalar) {
      isScalar = false;
    }
    throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_NOT_SUPPORTED_YET, "In Subquery");
  }

  @Override
  public boolean visit(SQLInListExpr e) {
    if (e.getExpr() instanceof SQLQueryExpr) {
      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_NOT_SUPPORTED_YET, "In Subquery");
    }
    e.getTargetList().forEach(expr -> {
      if (expr instanceof SQLQueryExpr) {
        throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_NOT_SUPPORTED_YET, "In Subquery");
      }
    });

    return true;
  }

  @Override
  public boolean visit(SQLExistsExpr e) {
    if (!isUserScalar) {
      isScalar = false;
    }
    throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_NOT_SUPPORTED_YET, "Exists Subquery");
  }

  @Override
  public boolean visit(SQLQueryExpr e) {
    if (!isUserScalar) {
      isScalar = false;
    }
    throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_NOT_SUPPORTED_YET, "Subquery");
  }

  @Override
  public boolean visit(SQLMethodInvokeExpr e) {
    FuncType funcType = FuncType.forName(e.getMethodName().toUpperCase());
    if (funcType.getDialect().isForbidChildFold()) {
      this.forbidFoldCounter++;
    }

    if (funcType != FuncType.VALUES) {
      return true;
    }

    Schema schema = this.schema;
    if (this.insertSchema != null) {
      schema = this.insertSchema;
    }

    final SQLName colName = (SQLName) e.getParameters().get(0);
    final ColumnExpr column = schema.getColumn(colName);
    if (column == null) {
      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_BAD_FIELD_ERROR, colName.getSimpleName(), "field list");
    }

    stack.addFirst(ExpressionUtil.buildValuesFunc(session, column.getOffset(), column.getResultType()));
    return false;
  }

  @Override
  public void postVisit(final SQLObject node) {
    if (!this.isExprAnalyzer) {
      return;
    }

    SQLObject visitNode = node;
    if (preFunc != null) {
      visitNode = preFunc.apply(visitNode);
    }
    final Class clazz = visitNode.getClass();

    if (clazz == SQLAllColumnExpr.class) {
      stack.addFirst(ValueExpr.ONE);
      return;
    }
    if (clazz == SQLIdentifierExpr.class) {
      this.rewriteColumnExpr((SQLIdentifierExpr) visitNode);
      return;
    }
    if (clazz == SQLPropertyExpr.class) {
      if (((SQLPropertyExpr) visitNode).getOwner() instanceof SQLVariantRefExpr) {
        this.rewriteVariable((SQLPropertyExpr) visitNode);
      } else {
        this.rewriteColumnExpr((SQLPropertyExpr) visitNode);
      }
      return;
    }

    if (visitNode instanceof SQLValuableExpr) {
      SQLObject parent = node.getParent();
      if (parent != null && (parent instanceof SQLSelectOrderByItem || parent instanceof SQLSelectGroupByClause)) {
        LongValue value = ValueConvertor.convertToLong(session, AnalyzerUtil.resolveValueExpr(visitNode, null), null);
        long pos = value == null ? -1 : value.getValue();
        if (pos > 0 && pos <= schema.size()) {
          stack.addFirst(schema.getColumn((int) pos - 1));
        } else {
          throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_BAD_FIELD_ERROR, String.valueOf(pos),
                  this.clause.getValue());
        }
      } else {
        ValueExpr valueExpr = new ValueExpr();
        AnalyzerUtil.resolveValueExpr(visitNode, valueExpr);
        stack.addFirst(valueExpr);
      }
      return;
    }

    if (clazz == SQLUnaryExpr.class) {
      this.rewriteUnaryOpExpr((SQLUnaryExpr) visitNode);
      return;
    }

    if (clazz == SQLBinaryOpExpr.class) {
      final SQLBinaryOpExpr opExpr = (SQLBinaryOpExpr) visitNode;
      if (!isCompareSubquery(opExpr)) {
        this.rewriteBinaryOpExpr(opExpr);
      }
      return;
    }
    if (clazz == SQLBetweenExpr.class) {
      this.rewriteBetweenExpr((SQLBetweenExpr) visitNode);
      return;
    }

    if (clazz == SQLAggregateExpr.class || clazz == SQLQueryExpr.class || clazz == SQLExistsExpr.class) {
      return;
    }
    if (clazz == SQLMethodInvokeExpr.class) {
      FuncType funcType = FuncType.forName(((SQLMethodInvokeExpr) visitNode).getMethodName().toUpperCase());
      if (funcType.getDialect().isForbidChildFold()) {
        this.forbidFoldCounter--;
      }
      if (funcType == FuncType.DATABASE) {
        ValueExpr valueExpr = new ValueExpr(StringValue.getInstance(this.session.getVarContext().getDefaultCatalog()), Types.buildSQLType(DataType.Varchar));
        this.stack.addFirst(valueExpr);
        return;
      }
      if (funcType == FuncType.VALUES) {
        return;
      }
    }

    if (clazz == SQLCastExpr.class) {
      Expression castExpr = stack.pollFirst();
      SQLDataType sqlType = ((SQLCastExpr) visitNode).getDataType();
      stack.addFirst(this.buildFuncExpr(FuncType.Cast, Types.buildSQLType(sqlType, null), castExpr));
      return;
    }

    if (clazz == SQLVariantRefExpr.class) {
      this.rewriteVariable((SQLVariantRefExpr) visitNode);
      return;
    }

    throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_NOT_SUPPORTED_YET, String.format("sql expr type %s",
            clazz.getName()));
  }

  private void rewriteUnaryOpExpr(final SQLUnaryExpr opExpr) {
    final Expression funcExpr;
    Expression arg = stack.pollFirst();
    switch (opExpr.getOperator()) {
      case Plus:
      case NOT:
      default:
        funcExpr = this.buildFuncExpr(FuncType.forName(opExpr.getOperator().name()), Types.UNDEFINE_TYPE, arg);
    }
    stack.addFirst(funcExpr);
  }

  private void rewriteBinaryOpExpr(final SQLBinaryOpExpr opExpr) {
    final Expression funcExpr;

    switch (opExpr.getOperator()) {
      case Equality:
      case NotEqual:
      case LessThanOrGreater:
      case LessThanOrEqualOrGreaterThan:
      case GreaterThan:
      case GreaterThanOrEqual:
      case LessThan:
      case LessThanOrEqual:
        Expression arg2 = stack.pollFirst();
        Expression arg1 = stack.pollFirst();
        funcExpr = this.buildBinaryOpFunc(arg1, arg2, opExpr.getOperator());
        break;

      default:
        arg2 = stack.pollFirst();
        arg1 = stack.pollFirst();
        funcExpr = this.buildFuncExpr(FuncType.forName(opExpr.getOperator().name()), Types.UNDEFINE_TYPE, arg1, arg2);
    }

    stack.addFirst(funcExpr);
  }

  private void rewriteBetweenExpr(SQLBetweenExpr between) {
    final Expression endExpr = stack.pollFirst();
    final Expression beginExpr = stack.pollFirst();
    final Expression testExpr = stack.pollFirst();

    final SQLType st = Types.buildSQLType(DataType.TinyInt);
    final Expression left = this.buildFuncExpr(FuncType.GreaterThanOrEqual, st, testExpr, beginExpr);
    final Expression right = this.buildFuncExpr(FuncType.LessThanOrEqual, st, testExpr, endExpr);

    Expression funcExpr = this.buildFuncExpr(FuncType.BooleanAnd, st, left, right);
    if (between.isNot()) {
      funcExpr = this.buildFuncExpr(FuncType.Not, st, funcExpr);
    }
    stack.addFirst(funcExpr);
  }

  private void rewriteColumnExpr(SQLName name) {
    ColumnExpr column = schema.getColumn(name);
    if (column != null) {
      stack.addFirst(column);
      return;
    }

    throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_BAD_FIELD_ERROR, name.getSimpleName(),
            this.clause.getValue());
  }

  private void rewriteVariable(SQLExpr expr) {

    if (expr instanceof SQLVariantRefExpr) {
      PrepareContext prepareContext = session.getPrepareContext();
      SQLVariantRefExpr varExp = (SQLVariantRefExpr) expr;
      if ("?".equals(varExp.getName())) {
        ValueExpr constVal = new ValueExpr(StringValue.getInstance(varExp.getName()), Types.buildSQLType(DataType.Varchar));

        SQLType sqlType = Types.buildSQLType(DataType.Int);
        ValueExpr valueExpr = new ValueExpr(LongValue.getInstance(varExp.getIndex()), sqlType);
        if (prepareContext.getPrepareTypes() == null) {
          stack.addFirst(constVal);
          return;
        }
        Expression expression = this.buildFuncExpr(FuncType.GetParam, prepareContext.getPrepareTypes().get(varExp
                .getIndex()), valueExpr);
        constVal.setLazyExpr(expression);
        expression.setResultType(prepareContext.getPrepareTypes().get(varExp.getIndex()));
        stack.addFirst(constVal);
        return;
      }
    }

    Variables.Variable variable = Variables.resolveVariable(expr);
    if (!variable.isSystem()) {
      ValueExpr valueExpr = new ValueExpr(StringValue.getInstance(variable.getName()), Types.buildSQLType(DataType.Varchar));
      Expression varFunc = buildFuncExpr(FuncType.GetVariable, Types.buildSQLType(DataType.Varchar), valueExpr);
      stack.addFirst(varFunc);
      return;
    }

    String val;
    if (variable.isGlobal()) {
      val = session.getVarContext().getGlobalVariable(variable.getName());
    } else {
      val = session.getVarContext().getSessionVariable(variable.getName());
    }

    ValueExpr valueExpr = new ValueExpr(StringValue.getInstance(val), Types.buildSQLType(DataType.Varchar));
    stack.addFirst(valueExpr);
  }

  /**
   * Check whether the expression is for "expr comparison_operator (select ...)".
   *
   * @param e
   * @return
   * @see //dev.mysql.com/doc/refman/5.7/en/comparisons-using-subqueries.html
   * @see //dev.mysql.com/doc/refman/5.7/en/any-in-some-subqueries.html
   * @see //dev.mysql.com/doc/refman/5.7/en/LogicalSelectionall-subqueries.html
   */
  private boolean isCompareSubquery(SQLBinaryOpExpr e) {
    final SQLExpr left = e.getLeft();
    final SQLExpr right = e.getRight();

    return left instanceof SQLQueryExpr || right instanceof SQLQueryExpr
            || left instanceof SQLAllExpr || right instanceof SQLAllExpr
            || left instanceof SQLAnyExpr || right instanceof SQLAnyExpr
            || left instanceof SQLSomeExpr || right instanceof SQLSomeExpr;
  }

  private Expression buildBinaryOpFunc(Expression arg1, Expression arg2, SQLBinaryOperator operator) {
    return this.buildFuncExpr(FuncType.forName(operator.name()), Types.buildSQLType(DataType.TinyInt), arg1, arg2);
  }

  private Expression buildFuncExpr(FuncType funcType, SQLType type, Expression... args) {
    return FuncExpr.build(session, funcType, type, forbidFoldCounter <= 0, args);
  }

  @Override
  public void endVisit(SQLBetweenExpr x) {
  }

  @Override
  public void endVisit(SQLBinaryOpExpr x) {
  }

  @Override
  public void endVisit(SQLCaseExpr x) {
  }

  @Override
  public void endVisit(SQLCaseExpr.Item x) {
  }

  @Override
  public void endVisit(SQLCaseStatement x) {
  }

  @Override
  public void endVisit(SQLCaseStatement.Item x) {
  }

  @Override
  public void endVisit(SQLCharExpr x) {
  }

  @Override
  public void endVisit(SQLIdentifierExpr x) {
  }

  @Override
  public void endVisit(SQLInListExpr x) {
  }

  @Override
  public void endVisit(SQLIntegerExpr x) {
  }

  @Override
  public void endVisit(SQLExistsExpr x) {
  }

  @Override
  public void endVisit(SQLNCharExpr x) {
  }

  @Override
  public void endVisit(SQLNotExpr x) {
  }

  @Override
  public void endVisit(SQLNullExpr x) {
  }

  @Override
  public void endVisit(SQLNumberExpr x) {
  }

  @Override
  public void endVisit(SQLPropertyExpr x) {
  }

  @Override
  public void endVisit(SQLSelectGroupByClause x) {
  }

  @Override
  public void endVisit(SQLSelectItem x) {
  }

  @Override
  public void endVisit(SQLSelectStatement selectStatement) {
  }

  @Override
  public void preVisit(SQLObject x) {
  }

  @Override
  public boolean visit(SQLAllColumnExpr x) {
    return true;
  }

  @Override
  public boolean visit(SQLBetweenExpr x) {
    return true;
  }

  @Override
  public boolean visit(SQLCaseExpr x) {
    return true;
  }

  @Override
  public boolean visit(SQLCaseExpr.Item x) {
    return true;
  }

  @Override
  public boolean visit(SQLCaseStatement x) {
    return true;
  }

  @Override
  public boolean visit(SQLCaseStatement.Item x) {
    return true;
  }

  @Override
  public boolean visit(SQLCastExpr x) {
    return true;
  }

  @Override
  public boolean visit(SQLCharExpr x) {
    return true;
  }

  @Override
  public boolean visit(SQLIdentifierExpr x) {
    return true;
  }

  @Override
  public boolean visit(SQLIntegerExpr x) {
    return true;
  }

  @Override
  public boolean visit(SQLNCharExpr x) {
    return true;
  }

  @Override
  public boolean visit(SQLNotExpr x) {
    return true;
  }

  @Override
  public boolean visit(SQLNullExpr x) {
    return true;
  }

  @Override
  public boolean visit(SQLNumberExpr x) {
    return true;
  }

  @Override
  public boolean visit(SQLSelectGroupByClause x) {
    return true;
  }

  @Override
  public boolean visit(SQLSelectItem x) {
    return true;
  }

  @Override
  public void endVisit(SQLCastExpr x) {

  }

  @Override
  public boolean visit(SQLSelectStatement astNode) {
    return true;
  }

  @Override
  public void endVisit(SQLAggregateExpr astNode) {

  }

  @Override
  public boolean visit(SQLVariantRefExpr x) {
    return true;
  }

  @Override
  public void endVisit(SQLVariantRefExpr x) {
  }

  @Override
  public void endVisit(SQLQueryExpr x) {
  }

  @Override
  public boolean visit(SQLUnaryExpr x) {
    return true;
  }

  @Override
  public void endVisit(SQLUnaryExpr x) {
  }

  @Override
  public boolean visit(SQLHexExpr x) {
    return true;
  }

  @Override
  public void endVisit(SQLHexExpr x) {
  }

  @Override
  public boolean visit(SQLSelect x) {
    return true;
  }

  @Override
  public void endVisit(SQLSelect select) {
  }

  @Override
  public boolean visit(SQLSelectQueryBlock x) {
    return true;
  }

  @Override
  public void endVisit(SQLSelectQueryBlock x) {
  }

  @Override
  public boolean visit(SQLExprTableSource x) {
    return true;
  }

  @Override
  public void endVisit(SQLExprTableSource x) {
  }

  @Override
  public boolean visit(SQLOrderBy x) {
    return true;
  }

  @Override
  public void endVisit(SQLOrderBy x) {
  }

  @Override
  public boolean visit(SQLSelectOrderByItem x) {
    return true;
  }

  @Override
  public void endVisit(SQLSelectOrderByItem x) {
  }

  @Override
  public boolean visit(SQLDropTableStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLDropTableStatement x) {
  }

  @Override
  public boolean visit(SQLCreateTableStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLCreateTableStatement x) {
  }

  @Override
  public boolean visit(SQLColumnDefinition x) {
    return true;
  }

  @Override
  public void endVisit(SQLColumnDefinition x) {
  }

  @Override
  public boolean visit(SQLColumnDefinition.Identity x) {
    return true;
  }

  @Override
  public void endVisit(SQLColumnDefinition.Identity x) {
  }

  @Override
  public boolean visit(SQLDataType x) {
    return true;
  }

  @Override
  public void endVisit(SQLDataType x) {
  }

  @Override
  public boolean visit(SQLCharacterDataType x) {
    return true;
  }

  @Override
  public void endVisit(SQLCharacterDataType x) {
  }

  @Override
  public boolean visit(SQLDeleteStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLDeleteStatement x) {
  }

  @Override
  public boolean visit(SQLCurrentOfCursorExpr x) {
    return true;
  }

  @Override
  public void endVisit(SQLCurrentOfCursorExpr x) {
  }

  @Override
  public boolean visit(SQLInsertStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLInsertStatement x) {
  }

  @Override
  public boolean visit(SQLInsertStatement.ValuesClause x) {
    return true;
  }

  @Override
  public void endVisit(SQLInsertStatement.ValuesClause x) {
  }

  @Override
  public boolean visit(SQLUpdateSetItem x) {
    return true;
  }

  @Override
  public void endVisit(SQLUpdateSetItem x) {
  }

  @Override
  public boolean visit(SQLUpdateStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLUpdateStatement x) {
  }

  @Override
  public boolean visit(SQLCreateViewStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLCreateViewStatement x) {
  }

  @Override
  public boolean visit(SQLCreateViewStatement.Column x) {
    return true;
  }

  @Override
  public void endVisit(SQLCreateViewStatement.Column x) {
  }

  @Override
  public boolean visit(SQLNotNullConstraint x) {
    return true;
  }

  @Override
  public void endVisit(SQLNotNullConstraint x) {
  }

  @Override
  public void endVisit(SQLMethodInvokeExpr x) {
  }

  @Override
  public void endVisit(SQLUnionQuery x) {
  }

  @Override
  public boolean visit(SQLUnionQuery x) {
    return true;
  }

  @Override
  public void endVisit(SQLSetStatement x) {
  }

  @Override
  public boolean visit(SQLSetStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLAssignItem x) {
  }

  @Override
  public boolean visit(SQLAssignItem x) {
    return true;
  }

  @Override
  public void endVisit(SQLCallStatement x) {

  }

  @Override
  public boolean visit(SQLCallStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLJoinTableSource x) {

  }

  @Override
  public boolean visit(SQLJoinTableSource x) {
    return true;
  }

  @Override
  public void endVisit(SQLSomeExpr x) {

  }

  @Override
  public boolean visit(SQLSomeExpr x) {
    return true;
  }

  @Override
  public void endVisit(SQLAnyExpr x) {

  }

  @Override
  public boolean visit(SQLAnyExpr x) {
    return true;
  }

  @Override
  public void endVisit(SQLAllExpr x) {

  }

  @Override
  public boolean visit(SQLAllExpr x) {
    return true;
  }

  @Override
  public void endVisit(SQLInSubQueryExpr x) {

  }

  @Override
  public void endVisit(SQLListExpr x) {

  }

  @Override
  public boolean visit(SQLListExpr x) {
    return true;
  }

  @Override
  public void endVisit(SQLSubqueryTableSource x) {

  }

  @Override
  public boolean visit(SQLSubqueryTableSource x) {
    return true;
  }

  @Override
  public void endVisit(SQLTruncateStatement x) {

  }

  @Override
  public boolean visit(SQLTruncateStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLDefaultExpr x) {

  }

  @Override
  public boolean visit(SQLDefaultExpr x) {
    return true;
  }

  @Override
  public void endVisit(SQLCommentStatement x) {

  }

  @Override
  public boolean visit(SQLCommentStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLUseStatement x) {

  }

  @Override
  public boolean visit(SQLUseStatement x) {
    return true;
  }

  @Override
  public boolean visit(SQLAlterTableAddColumn x) {
    return true;
  }

  @Override
  public void endVisit(SQLAlterTableAddColumn x) {

  }

  @Override
  public boolean visit(SQLAlterTableDropColumnItem x) {
    return true;
  }

  @Override
  public void endVisit(SQLAlterTableDropColumnItem x) {

  }

  @Override
  public boolean visit(SQLAlterTableDropIndex x) {
    return true;
  }

  @Override
  public void endVisit(SQLAlterTableDropIndex x) {

  }

  @Override
  public boolean visit(SQLDropIndexStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLDropIndexStatement x) {

  }

  @Override
  public boolean visit(SQLDropViewStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLDropViewStatement x) {

  }

  @Override
  public boolean visit(SQLSavePointStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLSavePointStatement x) {

  }

  @Override
  public boolean visit(SQLRollbackStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLRollbackStatement x) {

  }

  @Override
  public boolean visit(SQLReleaseSavePointStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLReleaseSavePointStatement x) {

  }

  @Override
  public void endVisit(SQLCommentHint x) {

  }

  @Override
  public boolean visit(SQLCommentHint x) {
    return true;
  }

  @Override
  public void endVisit(SQLCreateDatabaseStatement x) {

  }

  @Override
  public boolean visit(SQLCreateDatabaseStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLOver x) {

  }

  @Override
  public boolean visit(SQLOver x) {
    return true;
  }

  @Override
  public void endVisit(SQLKeep x) {

  }

  @Override
  public boolean visit(SQLKeep x) {
    return true;
  }

  @Override
  public void endVisit(SQLColumnPrimaryKey x) {

  }

  @Override
  public boolean visit(SQLColumnPrimaryKey x) {
    return true;
  }

  @Override
  public boolean visit(SQLColumnUniqueKey x) {
    return true;
  }

  @Override
  public void endVisit(SQLColumnUniqueKey x) {

  }

  @Override
  public void endVisit(SQLWithSubqueryClause x) {

  }

  @Override
  public boolean visit(SQLWithSubqueryClause x) {
    return true;
  }

  @Override
  public void endVisit(SQLWithSubqueryClause.Entry x) {

  }

  @Override
  public boolean visit(SQLWithSubqueryClause.Entry x) {
    return true;
  }

  @Override
  public void endVisit(SQLAlterTableAlterColumn x) {

  }

  @Override
  public boolean visit(SQLAlterTableAlterColumn x) {
    return true;
  }

  @Override
  public boolean visit(SQLCheck x) {
    return true;
  }

  @Override
  public void endVisit(SQLCheck x) {

  }

  @Override
  public boolean visit(SQLAlterTableDropForeignKey x) {
    return true;
  }

  @Override
  public void endVisit(SQLAlterTableDropForeignKey x) {

  }

  @Override
  public boolean visit(SQLAlterTableDropPrimaryKey x) {
    return true;
  }

  @Override
  public void endVisit(SQLAlterTableDropPrimaryKey x) {

  }

  @Override
  public boolean visit(SQLAlterTableDisableKeys x) {
    return true;
  }

  @Override
  public void endVisit(SQLAlterTableDisableKeys x) {

  }

  @Override
  public boolean visit(SQLAlterTableEnableKeys x) {
    return true;
  }

  @Override
  public void endVisit(SQLAlterTableEnableKeys x) {

  }

  @Override
  public boolean visit(SQLAlterTableStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLAlterTableStatement x) {

  }

  @Override
  public boolean visit(SQLAlterTableDisableConstraint x) {
    return true;
  }

  @Override
  public void endVisit(SQLAlterTableDisableConstraint x) {

  }

  @Override
  public boolean visit(SQLAlterTableEnableConstraint x) {
    return true;
  }

  @Override
  public void endVisit(SQLAlterTableEnableConstraint x) {

  }

  @Override
  public boolean visit(SQLColumnCheck x) {
    return true;
  }

  @Override
  public void endVisit(SQLColumnCheck x) {

  }

  @Override
  public boolean visit(SQLExprHint x) {
    return true;
  }

  @Override
  public void endVisit(SQLExprHint x) {

  }

  @Override
  public boolean visit(SQLAlterTableDropConstraint x) {
    return true;
  }

  @Override
  public void endVisit(SQLAlterTableDropConstraint x) {

  }

  @Override
  public boolean visit(SQLUnique x) {
    return true;
  }

  @Override
  public void endVisit(SQLUnique x) {

  }

  @Override
  public boolean visit(SQLPrimaryKeyImpl x) {
    return true;
  }

  @Override
  public void endVisit(SQLPrimaryKeyImpl x) {

  }

  @Override
  public boolean visit(SQLCreateIndexStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLCreateIndexStatement x) {

  }

  @Override
  public boolean visit(SQLAlterTableRenameColumn x) {
    return true;
  }

  @Override
  public void endVisit(SQLAlterTableRenameColumn x) {

  }

  @Override
  public boolean visit(SQLColumnReference x) {
    return true;
  }

  @Override
  public void endVisit(SQLColumnReference x) {

  }

  @Override
  public boolean visit(SQLForeignKeyImpl x) {
    return true;
  }

  @Override
  public void endVisit(SQLForeignKeyImpl x) {

  }

  @Override
  public boolean visit(SQLDropSequenceStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLDropSequenceStatement x) {

  }

  @Override
  public boolean visit(SQLDropTriggerStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLDropTriggerStatement x) {

  }

  @Override
  public void endVisit(SQLDropUserStatement x) {

  }

  @Override
  public boolean visit(SQLDropUserStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLExplainStatement x) {

  }

  @Override
  public boolean visit(SQLExplainStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLGrantStatement x) {

  }

  @Override
  public boolean visit(SQLGrantStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLDropDatabaseStatement x) {

  }

  @Override
  public boolean visit(SQLDropDatabaseStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLAlterTableAddIndex x) {

  }

  @Override
  public boolean visit(SQLAlterTableAddIndex x) {
    return true;
  }

  @Override
  public void endVisit(SQLAlterTableAddConstraint x) {

  }

  @Override
  public boolean visit(SQLAlterTableAddConstraint x) {
    return true;
  }

  @Override
  public void endVisit(SQLCreateTriggerStatement x) {

  }

  @Override
  public boolean visit(SQLCreateTriggerStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLDropFunctionStatement x) {

  }

  @Override
  public boolean visit(SQLDropFunctionStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLDropTableSpaceStatement x) {

  }

  @Override
  public boolean visit(SQLDropTableSpaceStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLDropProcedureStatement x) {

  }

  @Override
  public boolean visit(SQLDropProcedureStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLBooleanExpr x) {

  }

  @Override
  public boolean visit(SQLBooleanExpr x) {
    return true;
  }

  @Override
  public void endVisit(SQLUnionQueryTableSource x) {

  }

  @Override
  public boolean visit(SQLUnionQueryTableSource x) {
    return true;
  }

  @Override
  public void endVisit(SQLTimestampExpr x) {

  }

  @Override
  public boolean visit(SQLTimestampExpr x) {
    return true;
  }

  @Override
  public void endVisit(SQLRevokeStatement x) {

  }

  @Override
  public boolean visit(SQLRevokeStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLBinaryExpr x) {

  }

  @Override
  public boolean visit(SQLBinaryExpr x) {
    return true;
  }

  @Override
  public void endVisit(SQLAlterTableRename x) {

  }

  @Override
  public boolean visit(SQLAlterTableRename x) {
    return true;
  }

  @Override
  public void endVisit(SQLAlterViewRenameStatement x) {

  }

  @Override
  public boolean visit(SQLAlterViewRenameStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLShowTablesStatement x) {

  }

  @Override
  public boolean visit(SQLShowTablesStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLAlterTableAddPartition x) {

  }

  @Override
  public boolean visit(SQLAlterTableAddPartition x) {
    return true;
  }

  @Override
  public void endVisit(SQLAlterTableDropPartition x) {

  }

  @Override
  public boolean visit(SQLAlterTableDropPartition x) {
    return true;
  }

  @Override
  public void endVisit(SQLAlterTableRenamePartition x) {

  }

  @Override
  public boolean visit(SQLAlterTableRenamePartition x) {
    return true;
  }

  @Override
  public void endVisit(SQLAlterTableSetComment x) {

  }

  @Override
  public boolean visit(SQLAlterTableSetComment x) {
    return true;
  }

  @Override
  public void endVisit(SQLAlterTableSetLifecycle x) {

  }

  @Override
  public boolean visit(SQLAlterTableSetLifecycle x) {
    return true;
  }

  @Override
  public void endVisit(SQLAlterTableEnableLifecycle x) {

  }

  @Override
  public boolean visit(SQLAlterTableEnableLifecycle x) {
    return true;
  }

  @Override
  public void endVisit(SQLAlterTableDisableLifecycle x) {

  }

  @Override
  public boolean visit(SQLAlterTableDisableLifecycle x) {
    return true;
  }

  @Override
  public void endVisit(SQLAlterTableTouch x) {

  }

  @Override
  public boolean visit(SQLAlterTableTouch x) {
    return true;
  }

  @Override
  public void endVisit(SQLArrayExpr x) {

  }

  @Override
  public boolean visit(SQLArrayExpr x) {
    return true;
  }

  @Override
  public void endVisit(SQLOpenStatement x) {

  }

  @Override
  public boolean visit(SQLOpenStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLFetchStatement x) {

  }

  @Override
  public boolean visit(SQLFetchStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLCloseStatement x) {

  }

  @Override
  public boolean visit(SQLCloseStatement x) {
    return true;
  }

  @Override
  public boolean visit(SQLGroupingSetExpr x) {
    return true;
  }

  @Override
  public void endVisit(SQLGroupingSetExpr x) {

  }

  @Override
  public boolean visit(SQLIfStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLIfStatement x) {

  }

  @Override
  public boolean visit(SQLIfStatement.ElseIf x) {
    return true;
  }

  @Override
  public void endVisit(SQLIfStatement.ElseIf x) {

  }

  @Override
  public boolean visit(SQLIfStatement.Else x) {
    return true;
  }

  @Override
  public void endVisit(SQLIfStatement.Else x) {

  }

  @Override
  public boolean visit(SQLLoopStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLLoopStatement x) {

  }

  @Override
  public boolean visit(SQLParameter x) {
    return true;
  }

  @Override
  public void endVisit(SQLParameter x) {

  }

  @Override
  public boolean visit(SQLCreateProcedureStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLCreateProcedureStatement x) {

  }

  @Override
  public boolean visit(SQLCreateFunctionStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLCreateFunctionStatement x) {

  }

  @Override
  public boolean visit(SQLBlockStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLBlockStatement x) {

  }

  @Override
  public boolean visit(SQLAlterTableDropKey x) {
    return true;
  }

  @Override
  public void endVisit(SQLAlterTableDropKey x) {

  }

  @Override
  public boolean visit(SQLDeclareItem x) {
    return true;
  }

  @Override
  public void endVisit(SQLDeclareItem x) {

  }

  @Override
  public boolean visit(SQLPartitionValue x) {
    return true;
  }

  @Override
  public void endVisit(SQLPartitionValue x) {

  }

  @Override
  public boolean visit(SQLPartition x) {
    return true;
  }

  @Override
  public void endVisit(SQLPartition x) {

  }

  @Override
  public boolean visit(SQLPartitionByRange x) {
    return true;
  }

  @Override
  public void endVisit(SQLPartitionByRange x) {

  }

  @Override
  public boolean visit(SQLPartitionByHash x) {
    return true;
  }

  @Override
  public void endVisit(SQLPartitionByHash x) {

  }

  @Override
  public boolean visit(SQLPartitionByList x) {
    return true;
  }

  @Override
  public void endVisit(SQLPartitionByList x) {

  }

  @Override
  public boolean visit(SQLSubPartition x) {
    return true;
  }

  @Override
  public void endVisit(SQLSubPartition x) {

  }

  @Override
  public boolean visit(SQLSubPartitionByHash x) {
    return true;
  }

  @Override
  public void endVisit(SQLSubPartitionByHash x) {

  }

  @Override
  public boolean visit(SQLSubPartitionByList x) {
    return true;
  }

  @Override
  public void endVisit(SQLSubPartitionByList x) {

  }

  @Override
  public boolean visit(SQLAlterDatabaseStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLAlterDatabaseStatement x) {

  }

  @Override
  public boolean visit(SQLAlterTableConvertCharSet x) {
    return true;
  }

  @Override
  public void endVisit(SQLAlterTableConvertCharSet x) {

  }

  @Override
  public boolean visit(SQLAlterTableReOrganizePartition x) {
    return true;
  }

  @Override
  public void endVisit(SQLAlterTableReOrganizePartition x) {

  }

  @Override
  public boolean visit(SQLAlterTableCoalescePartition x) {
    return true;
  }

  @Override
  public void endVisit(SQLAlterTableCoalescePartition x) {

  }

  @Override
  public boolean visit(SQLAlterTableTruncatePartition x) {
    return true;
  }

  @Override
  public void endVisit(SQLAlterTableTruncatePartition x) {

  }

  @Override
  public boolean visit(SQLAlterTableDiscardPartition x) {
    return true;
  }

  @Override
  public void endVisit(SQLAlterTableDiscardPartition x) {

  }

  @Override
  public boolean visit(SQLAlterTableImportPartition x) {
    return true;
  }

  @Override
  public void endVisit(SQLAlterTableImportPartition x) {

  }

  @Override
  public boolean visit(SQLAlterTableAnalyzePartition x) {
    return true;
  }

  @Override
  public void endVisit(SQLAlterTableAnalyzePartition x) {

  }

  @Override
  public boolean visit(SQLAlterTableCheckPartition x) {
    return true;
  }

  @Override
  public void endVisit(SQLAlterTableCheckPartition x) {

  }

  @Override
  public boolean visit(SQLAlterTableOptimizePartition x) {
    return true;
  }

  @Override
  public void endVisit(SQLAlterTableOptimizePartition x) {

  }

  @Override
  public boolean visit(SQLAlterTableRebuildPartition x) {
    return true;
  }

  @Override
  public void endVisit(SQLAlterTableRebuildPartition x) {

  }

  @Override
  public boolean visit(SQLAlterTableRepairPartition x) {
    return true;
  }

  @Override
  public void endVisit(SQLAlterTableRepairPartition x) {

  }

  @Override
  public boolean visit(SQLSequenceExpr x) {
    return true;
  }

  @Override
  public void endVisit(SQLSequenceExpr x) {

  }

  @Override
  public boolean visit(SQLMergeStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLMergeStatement x) {

  }

  @Override
  public boolean visit(SQLMergeStatement.MergeUpdateClause x) {
    return true;
  }

  @Override
  public void endVisit(SQLMergeStatement.MergeUpdateClause x) {

  }

  @Override
  public boolean visit(SQLMergeStatement.MergeInsertClause x) {
    return true;
  }

  @Override
  public void endVisit(SQLMergeStatement.MergeInsertClause x) {

  }

  @Override
  public boolean visit(SQLErrorLoggingClause x) {
    return true;
  }

  @Override
  public void endVisit(SQLErrorLoggingClause x) {

  }

  @Override
  public boolean visit(SQLNullConstraint x) {
    return true;
  }

  @Override
  public void endVisit(SQLNullConstraint x) {

  }

  @Override
  public boolean visit(SQLCreateSequenceStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLCreateSequenceStatement x) {

  }

  @Override
  public boolean visit(SQLDateExpr x) {
    return true;
  }

  @Override
  public void endVisit(SQLDateExpr x) {

  }

  @Override
  public boolean visit(SQLLimit x) {
    return true;
  }

  @Override
  public void endVisit(SQLLimit x) {

  }

  @Override
  public void endVisit(SQLStartTransactionStatement x) {

  }

  @Override
  public boolean visit(SQLStartTransactionStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLDescribeStatement x) {

  }

  @Override
  public boolean visit(SQLDescribeStatement x) {
    return true;
  }

  @Override
  public boolean visit(SQLWhileStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLWhileStatement x) {

  }

  @Override
  public boolean visit(SQLDeclareStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLDeclareStatement x) {

  }

  @Override
  public boolean visit(SQLReturnStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLReturnStatement x) {

  }

  @Override
  public boolean visit(SQLArgument x) {
    return true;
  }

  @Override
  public void endVisit(SQLArgument x) {

  }

  @Override
  public boolean visit(SQLCommitStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLCommitStatement x) {

  }

  @Override
  public boolean visit(SQLFlashbackExpr x) {
    return true;
  }

  @Override
  public void endVisit(SQLFlashbackExpr x) {

  }

  @Override
  public boolean visit(SQLCreateMaterializedViewStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLCreateMaterializedViewStatement x) {

  }

  @Override
  public boolean visit(SQLBinaryOpExprGroup x) {
    return true;
  }

  @Override
  public void endVisit(SQLBinaryOpExprGroup x) {

  }

  @Override
  public boolean visit(SQLScriptCommitStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLScriptCommitStatement x) {

  }

  @Override
  public boolean visit(SQLReplaceStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLReplaceStatement x) {

  }

  @Override
  public boolean visit(SQLCreateUserStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLCreateUserStatement x) {

  }

  @Override
  public boolean visit(SQLAlterFunctionStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLAlterFunctionStatement x) {

  }

  @Override
  public boolean visit(SQLAlterTypeStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLAlterTypeStatement x) {

  }

  @Override
  public boolean visit(SQLIntervalExpr x) {
    return true;
  }

  @Override
  public void endVisit(SQLIntervalExpr x) {

  }

  @Override
  public boolean visit(SQLLateralViewTableSource x) {
    return true;
  }

  @Override
  public void endVisit(SQLLateralViewTableSource x) {

  }

  @Override
  public boolean visit(SQLShowErrorsStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLShowErrorsStatement x) {

  }

  @Override
  public boolean visit(SQLAlterCharacter x) {
    return true;
  }

  @Override
  public void endVisit(SQLAlterCharacter x) {

  }

  @Override
  public boolean visit(SQLExprStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLExprStatement x) {

  }

  @Override
  public boolean visit(SQLAlterProcedureStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLAlterProcedureStatement x) {

  }

  @Override
  public boolean visit(SQLAlterViewStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLAlterViewStatement x) {

  }

  @Override
  public boolean visit(SQLDropEventStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLDropEventStatement x) {

  }

  @Override
  public boolean visit(SQLDropLogFileGroupStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLDropLogFileGroupStatement x) {

  }

  @Override
  public boolean visit(SQLDropServerStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLDropServerStatement x) {

  }

  @Override
  public boolean visit(SQLDropSynonymStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLDropSynonymStatement x) {

  }

  @Override
  public boolean visit(SQLRecordDataType x) {
    return true;
  }

  @Override
  public void endVisit(SQLRecordDataType x) {

  }

  @Override
  public boolean visit(SQLDropTypeStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLDropTypeStatement x) {

  }

  @Override
  public boolean visit(SQLExternalRecordFormat x) {
    return true;
  }

  @Override
  public void endVisit(SQLExternalRecordFormat x) {

  }

  @Override
  public boolean visit(SQLArrayDataType x) {
    return true;
  }

  @Override
  public void endVisit(SQLArrayDataType x) {

  }

  @Override
  public boolean visit(SQLMapDataType x) {
    return true;
  }

  @Override
  public void endVisit(SQLMapDataType x) {

  }

  @Override
  public boolean visit(SQLStructDataType x) {
    return true;
  }

  @Override
  public void endVisit(SQLStructDataType x) {

  }

  @Override
  public boolean visit(SQLStructDataType.Field x) {
    return true;
  }

  @Override
  public void endVisit(SQLStructDataType.Field x) {

  }

  @Override
  public boolean visit(SQLDropMaterializedViewStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLDropMaterializedViewStatement x) {

  }

  @Override
  public boolean visit(SQLAlterTableRenameIndex x) {
    return true;
  }

  @Override
  public void endVisit(SQLAlterTableRenameIndex x) {

  }

  @Override
  public boolean visit(SQLAlterSequenceStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLAlterSequenceStatement x) {

  }

  @Override
  public boolean visit(SQLAlterTableExchangePartition x) {
    return true;
  }

  @Override
  public void endVisit(SQLAlterTableExchangePartition x) {

  }

  @Override
  public boolean visit(SQLValuesExpr x) {
    return true;
  }

  @Override
  public void endVisit(SQLValuesExpr x) {

  }

  @Override
  public boolean visit(SQLValuesTableSource x) {
    return true;
  }

  @Override
  public void endVisit(SQLValuesTableSource x) {

  }

  @Override
  public boolean visit(SQLContainsExpr x) {
    return true;
  }

  @Override
  public void endVisit(SQLContainsExpr x) {

  }

  @Override
  public boolean visit(SQLRealExpr x) {
    return true;
  }

  @Override
  public void endVisit(SQLRealExpr x) {

  }

  @Override
  public boolean visit(SQLWindow x) {
    return true;
  }

  @Override
  public void endVisit(SQLWindow x) {

  }

  @Override
  public boolean visit(SQLDumpStatement x) {
    return true;
  }

  @Override
  public void endVisit(SQLDumpStatement x) {

  }

  @Override
  public void endVisit(SQLAllColumnExpr x) {

  }
}
