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
package io.jimdb.sql.operator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import io.jimdb.core.Session;
import io.jimdb.core.context.PrepareContext;
import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.exception.JimException;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.model.prepare.JimStatement;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.model.result.impl.PrepareResult;
import io.jimdb.sql.Planner;
import io.jimdb.sql.analyzer.CacheableAnalyzer;
import io.jimdb.sql.analyzer.PrepareAnalyzer;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.druid.sql.ast.statement.SQLDDLStatement;

import reactor.core.publisher.Flux;

/**
 * @version V1.0
 */
public final class Prepare extends Operator {
  private int id;
  private String name;
  private String sqlText;
  private SQLStatement statement;
  private Planner planner;

  // MAX_PREPARED_STMT_COUNT is the name for 'max_prepared_stmt_count' system variable.
  public static final String MAX_PREPARED_STMT_COUNT = "max_prepared_stmt_count";

  @Override
  public Flux<ExecResult> execute(Session session) throws JimException {
    PrepareContext context = session.getPrepareContext();
    ConcurrentHashMap<Integer, JimStatement> jimstmts = context.getPreparedStmts();

    PrepareResult prepareResult = new PrepareResult();

    if (this.id != 0 && null != jimstmts.get(id)) {
      prepareResult.setStmtId(this.id);
      return Flux.just(prepareResult);
    }

    List<SQLStatement> statements = this.planner.parse(sqlText);

    if (statements.size() != 1) {
      throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_NOT_SUPPORTED_YET, "Can not prepare multiple statements");
    }

    SQLStatement statement = statements.get(0);

    if (statement instanceof SQLDDLStatement) {
      throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_NOT_SUPPORTED_YET, "Can not prepare DDL statements with parameters");
    }

    List<SQLVariantRefExpr> variantRefExprs = new ArrayList<>();
    PrepareAnalyzer prepareAnalyzer = new PrepareAnalyzer();
    prepareAnalyzer.setVariantRefExprs(variantRefExprs);
    statement.accept(prepareAnalyzer);


    if (variantRefExprs.size() > (1 << 16 - 1)) {
      throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_NOT_SUPPORTED_YET, "Too many prepare parameters");
    }

    long maxStmtCount = Long.parseLong(session.getVarContext().getGlobalVariable(MAX_PREPARED_STMT_COUNT));
    if (jimstmts.size() > maxStmtCount) {
      throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_MAX_PREPARED_STMT_COUNT_REACHED, ErrorCode.ER_MAX_PREPARED_STMT_COUNT_REACHED.getMessage());
    }

    Operator operator = this.planner.analyze1(session, statement);
    context.setUseCache(CacheableAnalyzer.checkCacheable(statement));

    List<ColumnExpr> columns = operator.getSchema().getColumns();
    prepareResult.setColumnExprs(columns.toArray(new ColumnExpr[0]));
    prepareResult.setColumnsNum(columns.size());
    prepareResult.setParametersNum(variantRefExprs.size());
    prepareResult.setParams(variantRefExprs);

    if (this.id == 0) {
      this.id = context.getNextPreparedStmtID();
    }
    prepareResult.setStmtId(this.id);

    JimStatement stmtCache = new JimStatement();
    stmtCache.setStmtId(this.id);
    stmtCache.setSql(sqlText);
    stmtCache.setSqlStatement(statement);
    stmtCache.setParametersNum(variantRefExprs.size());

    jimstmts.put(this.id, stmtCache);
    return Flux.just(prepareResult);
  }

  @Override
  public OperatorType getOperatorType() {
    return OperatorType.SIMPLE;
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getSqlText() {
    return sqlText;
  }

  public void setSqlText(String sqlText) {
    this.sqlText = sqlText;
  }

  public SQLStatement getStatement() {
    return statement;
  }

  public void setStatement(SQLStatement statement) {
    this.statement = statement;
  }

  public Planner getPlanner() {
    return planner;
  }

  public void setPlanner(Planner planner) {
    this.planner = planner;
  }
}
