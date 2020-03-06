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
package io.jimdb.sql.planner;

import java.util.List;
import java.util.function.Supplier;

import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.exception.BaseException;
import io.jimdb.core.SQLAnalyzer;
import io.jimdb.core.Session;
import io.jimdb.core.context.StatementContext;
import io.jimdb.core.model.privilege.PrivilegeInfo;
import io.jimdb.core.model.privilege.UserInfo;
import io.jimdb.core.plugin.PluginFactory;
import io.jimdb.core.plugin.PrivilegeEngine;
import io.jimdb.core.plugin.SQLEngine;
import io.jimdb.sql.analyzer.StatementAnalyzer;
import io.jimdb.sql.analyzer.mysql.MySQLAnalyzer;
import io.jimdb.sql.operator.Delete;
import io.jimdb.sql.operator.DualTable;
import io.jimdb.sql.operator.Explain;
import io.jimdb.sql.operator.Operator;
import io.jimdb.sql.operator.RelOperator;
import io.jimdb.sql.operator.Update;
import io.jimdb.sql.operator.show.Show;
import io.jimdb.sql.optimizer.Optimizer;
import io.jimdb.sql.optimizer.statistics.TableStatsManager;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLParserFeature;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.sql.parser.Token;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * SQL planner that parses, analyzes, and optimize the given SQL statement.
 */
@SuppressFBWarnings
public final class Planner {
  private static final SQLParserFeature[] SQL_FEATURES = new SQLParserFeature[]{
          SQLParserFeature.UseInsertColumnsCache,
          SQLParserFeature.SkipComments };

  private final SQLEngine.DBType dbType;
  private final Supplier<SQLAnalyzer> analyzer;

  private PrivilegeEngine engine;

  public Planner(final SQLEngine.DBType type) {
    switch (type) {
      case MYSQL:
        this.dbType = type;
        this.analyzer = MySQLAnalyzer::new;
        break;

      default:
        throw DBException.get(ErrorModule.PLANNER, ErrorCode.ER_NOT_SUPPORTED_YET, "DBType(" + type.getName() + ")");
    }
    this.engine = PluginFactory.getPrivilegeEngine();
  }

  public void shutdown() {
    TableStatsManager.shutdown();
  }

  public List<SQLStatement> parse(final String sql) throws BaseException {
    SQLStatementParser parser = null;
    switch (dbType) {
      case MYSQL:
        parser = new MySqlStatementParser(sql, SQL_FEATURES);
        break;

      default:
        parser = new MySqlStatementParser(sql, SQL_FEATURES);
        break;
    }

    List<SQLStatement> stmtList;
    try {
      stmtList = parser.parseStatementList();
    } catch (Exception e) {
      throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_SYSTEM_PARSE_ERROR, e, e.getMessage());
    }

    if (parser.getLexer().token() != Token.EOF) {
      throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_SYSTEM_PARSE_ERROR, "syntax error");
    }
    return stmtList;
  }

  /**
   * Perform analysis on the parsed SQL statement
   *
   * @param session current query session
   * @param stmt    parsed SQL statement
   * @return analyzed & generated plan tree
   */
  public Operator analyze(Session session, SQLStatement stmt) {
    return analyze(session, stmt, false);
  }

  public Operator analyzeAndOptimize(Session session, SQLStatement stmt) {
    return analyze(session, stmt, true);
  }

  private Operator analyze(Session session, SQLStatement stmt, boolean isOptimize) {
    final StatementContext stmtCtx = session.getStmtContext();
    stmtCtx.reset();

    try {
      final StatementAnalyzer analyzer = (StatementAnalyzer) stmtCtx.retainAnalyzer(this.analyzer);
      analyzer.setSession(session);
      stmt.accept(analyzer);
      Operator op = analyzer.getResultOperator();
      if (op == null) {
        throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_NOT_SUPPORTED_YET, stmt.toLowerCaseString());
      }

      if (isOptimize) {
        if (op instanceof Explain) {
          Explain explain = (Explain) op;
          op = optimize(session, explain.getChild(), analyzer.getOptimizationFlag());
          explain.setChild(op);
          return explain;
        }

        return optimize(session, op, analyzer.getOptimizationFlag());
      }
      return op;
    } finally {
      stmtCtx.releaseAnalyzer();
    }
  }

  /**
   * Perform optimizations (CBO & RBO) on the given plan tree
   *
   * @param session current query session
   * @param planOp  the given plan tree
   * @return optimized plan tree
   */
  private Operator optimize(Session session, Operator planOp, int optimizationFlag) {
    checkPrivilege(session);

    Operator.OperatorType type = planOp.getOperatorType();
    if (type == Operator.OperatorType.SIMPLE || type == Operator.OperatorType.INSERT
            || type == Operator.OperatorType.DDL || type == Operator.OperatorType.ANALYZE) {
      return planOp;
    }

    if (type == Operator.OperatorType.DELETE) {
      Delete delete = (Delete) planOp;
      RelOperator selectOp = delete.getSelect();
      if (selectOp.getOperatorType() == Operator.OperatorType.LOGIC) {
        RelOperator optimizedOperator = Optimizer.optimize(session, selectOp, optimizationFlag);
        delete.setSelect(optimizedOperator);
      }
      return delete;
    }

    if (type == Operator.OperatorType.UPDATE) {
      Update update = (Update) planOp;
      RelOperator selectOp = update.getSelect();
      if (selectOp.getOperatorType() == Operator.OperatorType.LOGIC) {
        RelOperator optimizedOperator = Optimizer.optimize(session, selectOp, optimizationFlag);
        update.setSelect(optimizedOperator);
      }

      update.resolveOffset();
      return update;
    }

    if (type == Operator.OperatorType.SHOW) {
      Show show = (Show) planOp;
      RelOperator projection = show.getProjectOperator();
      //RelOperator optimizeOperator = Optimizer.optimize(session, projection, this.optimizationFlag, tableStatsMap);
      RelOperator relOperator = substituteDualTablePlaceHolder(projection, show.getShowOperator());
      show.setShowOperator(relOperator);
      return show;
    }

    return Optimizer.optimize(session, (RelOperator) planOp, optimizationFlag);
  }

  @SuppressFBWarnings("CFS_CONFUSING_FUNCTION_SEMANTICS")
  private RelOperator substituteDualTablePlaceHolder(RelOperator src, RelOperator dst) {
    if (src instanceof DualTable) {
      DualTable dualTable = (DualTable) src;
      if (dualTable.isPlaceHolder()) {
        return dst;
      }
    }

    for (int i = 0; src.getChildren() != null && i < src.getChildren().length; i++) {
      RelOperator relOperator = substituteDualTablePlaceHolder(src.getChildren()[i], dst);
      src.setChild(i, relOperator);
    }
    return src;
  }

  public void checkPrivilege(Session session) {
    UserInfo userInfo = session.getUserInfo();
    if (userInfo == null) {
      return;
    }

    List<PrivilegeInfo> privilegeInfos = session.getStmtContext().getPrivilegeInfos();
    for (PrivilegeInfo privilegeInfo : privilegeInfos) {
      if (!engine.verify(userInfo, privilegeInfo)) {
        BaseException replyErr = null;
        switch (privilegeInfo.getType()) {
          case CREATE_PRIV:
          case INSERT_PRIV:
          case SELECT_PRIV:
          case DROP_PRIV:
          case DELETE_PRIV:
          case ALTER_PRIV:
          case INDEX_PRIV:
          case UPDATE_PRIV:
          case CREATE_VIEW_PRIV:
            if (privilegeInfo.getTable() != null) {
              replyErr = DBException.get(ErrorModule.PRIVILEGE, ErrorCode.ER_TABLEACCESS_DENIED_ERROR, privilegeInfo
                      .getType().toString(), userInfo.getUser(), userInfo.getHost(), privilegeInfo.getTable());
            } else if (privilegeInfo.getCatalog() != null) {
              replyErr = DBException.get(ErrorModule.PRIVILEGE, ErrorCode.ER_DBACCESS_DENIED_ERROR, privilegeInfo
                      .getType().toString(), userInfo.getUser(), userInfo.getHost(), privilegeInfo
                      .getCatalog());
            }
            break;
          case SUPER_PRIV:
          case TRIGGER_PRIV:
          case CREATE_USER_PRIV:
          case PROCESS_PRIV:
          case GRANT_PRIV:
          case CREATE_ROLE_PRIV:
          case SHOW_DB_PRIV:
          case EXECUTE_PRIV:
          case DROP_ROLE_PRIV:
          case SHOW_VIEW_PRIV:
          case REFERENCES_PRIV:
            replyErr = DBException.get(ErrorModule.PRIVILEGE, ErrorCode.ER_KEYRING_ACCESS_DENIED_ERROR, privilegeInfo
                    .getType().toString());
        }
        throw replyErr;
      }
    }
  }
}
