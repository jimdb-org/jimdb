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
import java.util.List;

import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.exception.BaseException;
import io.jimdb.core.Session;
import io.jimdb.core.context.PreparedContext;
import io.jimdb.core.context.PreparedStatement;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.model.result.impl.PrepareResult;
import io.jimdb.sql.planner.Planner;
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
  // MAX_PREPARED_STMT_COUNT is the name for 'max_prepared_stmt_count' system variable.
  private static final String MAX_PREPARED_STMT_COUNT = "max_prepared_stmt_count";
  private static final int MAX_PREPARED_PARAM_COUNT = 1 << 16 - 1;

  private final String sql;
  private final Planner planner;

  public Prepare(Planner planner, String sql) {
    this.planner = planner;
    this.sql = sql;
  }

  @Override
  public Flux<ExecResult> execute(Session session) throws BaseException {
    List<SQLStatement> statements = planner.parse(sql);
    if (statements.size() != 1) {
      throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_NOT_SUPPORTED_YET, "prepare multiple statements");
    }

    SQLStatement statement = statements.get(0);
    if (statement instanceof SQLDDLStatement) {
      throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_NOT_SUPPORTED_YET, "prepare DDL statements");
    }

    List<SQLVariantRefExpr> variantExprs = new ArrayList<>();
    PrepareAnalyzer prepareAnalyzer = new PrepareAnalyzer();
    prepareAnalyzer.setVariantRefExprs(variantExprs);
    statement.accept(prepareAnalyzer);
    if (variantExprs.size() > MAX_PREPARED_PARAM_COUNT) {
      throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_PS_MANY_PARAM);
    }

    long maxStmtCount = Long.parseLong(session.getVarContext().getGlobalVariable(MAX_PREPARED_STMT_COUNT));
    PreparedContext preparedContext = session.getPreparedContext();
    if (preparedContext.getStatementCount() > maxStmtCount) {
      throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_MAX_PREPARED_STMT_COUNT_REACHED);
    }

    PrepareResult prepareResult = new PrepareResult();

    Operator operator = planner.analyze(session, statement);
    int stmtID = preparedContext.nextID();
    List<ColumnExpr> columns = operator.getSchema().getColumns();
    prepareResult.setStmtId(stmtID);
    prepareResult.setColumnExprs(columns.toArray(new ColumnExpr[0]));
    prepareResult.setColumnsNum(columns.size());
    prepareResult.setParametersNum(variantExprs.size());
    prepareResult.setParams(variantExprs);

    boolean cachePlan = CacheableAnalyzer.checkCacheable(statement);
    PreparedStatement stmtCache = new PreparedStatement(session.getPreparedContext(), sql, stmtID,
            cachePlan, variantExprs.size(), statement);
    preparedContext.setStatement(stmtID, stmtCache);
    return Flux.just(prepareResult);
  }

  @Override
  public OperatorType getOperatorType() {
    return OperatorType.SIMPLE;
  }

  @Override
  public String getName() {
    return "prepare";
  }
}
