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
package io.jimdb.sql;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.ThreadSafe;

import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.exception.JimException;
import io.jimdb.common.utils.os.SystemClock;
import io.jimdb.core.Session;
import io.jimdb.core.config.JimConfig;
import io.jimdb.core.context.PrepareContext;
import io.jimdb.core.model.prepare.JimStatement;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.model.result.ResultType;
import io.jimdb.core.plugin.PluginFactory;
import io.jimdb.core.plugin.SQLEngine;
import io.jimdb.core.plugin.SQLExecutor;
import io.jimdb.core.plugin.store.Engine;
import io.jimdb.core.plugin.store.Transaction;
import io.jimdb.sql.ddl.DDLExecutor;
import io.jimdb.sql.operator.DualTable;
import io.jimdb.sql.operator.Operator;
import io.jimdb.sql.operator.Prepare;
import io.jimdb.sql.optimizer.physical.RangeRebuildVisitor;
import io.jimdb.sql.optimizer.statistics.TableStatsManager;

import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.google.common.base.Preconditions;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SignalType;

/**
 * Asynchronous Handler base on calcite.
 *
 * @version V1.0
 */
@ThreadSafe
public final class JimSQLExecutor implements SQLExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(JimSQLExecutor.class);

  private Engine storeEngine;
  private Planner planner;

  @Override
  @SuppressFBWarnings({ "HES_EXECUTOR_NEVER_SHUTDOWN", "HES_EXECUTOR_OVERWRITTEN_WITHOUT_SHUTDOWN" })
  public void init(JimConfig config) {
    this.storeEngine = PluginFactory.getStoreEngine();
    Preconditions.checkNotNull(this.storeEngine, "StoreEngine cant be null.");

    DDLExecutor.init(config);
    TableStatsManager.init(config);
    this.planner = new Planner(SQLEngine.DBType.MYSQL);
  }

  @Override
  public void close() {
    this.planner.shutdown();
    DDLExecutor.close();
  }

  @Override
  public void executeQuery(Session s, String sql) {
    JimException replyErr = null;
    try {
      s.initTxnContext();
      List<SQLStatement> stmts = this.planner.parse(sql);
      Flux<ExecResult> execFlux;
      final ExecutorSubscriber subscriber;

      if (stmts.size() > 1) {
        execFlux = Flux.just(stmts.toArray(new SQLStatement[0])).flatMap(e -> {
          Operator optimizedOp = this.planner.analyzeAndOptimize(s, stmts.get(0));
          if (!optimizedOp.getOperatorType().isWritable()) {
            return Flux.error(DBException.get(ErrorModule.EXECUTOR, ErrorCode.ER_NOT_SUPPORTED_YET, "Batch execute of non-dml"));
          }
          return optimizedOp.execute(s);
        });
        subscriber = new BatchExecResultSubscriber(s, sql, stmts.size());
      } else {
        Operator optimizedOp = this.planner.analyzeAndOptimize(s, stmts.get(0));
        Operator.OperatorType opType = optimizedOp.getOperatorType();
        if (opType == Operator.OperatorType.DDL) {
          Transaction txn = s.getTxn();
          if (txn != null && txn.isPending()) {
            txn.commit().blockFirst();
          }
        }
        if (!opType.isWritable()) {
          s.getTxnContext().metaRelease();
        }

        execFlux = optimizedOp.execute(s);
        subscriber = new ExecResultSubscriber(s, sql, optimizedOp);
        // trace log -- default : log is closed
        PlanLogTracer.sqlPhysicalPlanLOG(stmts.get(0), optimizedOp);
      }

      Instant timeout;
      if ((timeout = s.getStmtContext().getTimeout()) != null) {
        Instant now = SystemClock.currentTimeStamp();
        if (now.isAfter(timeout)) {
          throw DBException.get(ErrorModule.EXECUTOR, ErrorCode.ER_QUERY_TIMEOUT);
        }
        execFlux = execFlux.timeout(Duration.between(now, timeout));
      }

      execFlux.subscribe(subscriber);
    } catch (JimException e) {
      replyErr = e;
    } catch (Exception e) {
      replyErr = DBException.get(ErrorModule.EXECUTOR, ErrorCode.ER_UNKNOWN_ERROR, e);
    }

    if (replyErr != null) {
      replyError(s, sql, replyErr);
    }
  }

  @Override
  public void executePrepare(Session s, String sql) {
    s.initTxnContext();
    Prepare prepare = new Prepare();
    prepare.setPlanner(this.planner);
    prepare.setSqlText(sql);

    Flux<ExecResult> execute = prepare.execute(s);
    execute.subscribe(r -> s.write(r, true));
  }

  @Override
  public void execute(Session session, int stmtId) {
    JimException replyErr = null;
    PrepareContext prepareContext = session.getPrepareContext();
    JimStatement jimStmt = prepareContext.getPreparedStmts().get(stmtId);
    SQLStatement stmt = jimStmt.getSqlStatement();
    try {
      session.getStmtContext().setBinaryProtocol(true);
      PreparePlanCacheKey planCacheKey = new PreparePlanCacheKey(session.getVarContext().getDefaultCatalog(), session.getConnID(),
              stmtId, 0);
      Object plan = prepareContext.getCache().getIfPresent(planCacheKey);

      Operator operator;
      if (plan != null) {
        operator = (Operator) plan;
        operator.acceptVisitor(new RangeRebuildVisitor(session));
      } else {
        operator = this.planner.analyzeAndOptimize(session, stmt);

        if (!(operator instanceof DualTable)) {
          prepareContext.getCache().put(planCacheKey, operator);
        }
      }
      Flux<ExecResult> resultFlux = operator.execute(session);

      resultFlux.subscribe(new ExecResultSubscriber(session, stmt.toLowerCaseString(), operator));
    } catch (JimException e) {
      replyErr = e;
    } catch (Exception e) {
      replyErr = DBException.get(ErrorModule.EXECUTOR, ErrorCode.ER_UNKNOWN_ERROR, e);
    }

    if (replyErr != null) {
      replyError(session, stmt.toLowerCaseString(), replyErr);
    }
  }

  private static void replyError(Session s, String sql, JimException e) {
    LOG.error(String.format("Session replyError to %s, message: %s", sql, e), e);

    try {
      s.writeError(e);
    } catch (Exception ex) {
      LOG.error(String.format("Session writeError reply to %s error.", sql), ex);
    }
  }

  private static void replyBatchResult(Session s, List<ExecResult> results) {
    s.getVarContext().setStatus(SQLEngine.ServerStatus.MORERESULTSEXISTS, true);
    int i = 0;
    boolean isEof = false;
    for (ExecResult result : results) {
      if (++i == results.size()) {
        isEof = true;
        s.getVarContext().setStatus(SQLEngine.ServerStatus.MORERESULTSEXISTS, false);
      }

      s.write(result, isEof);
    }
  }

  /**
   * Flux subscriber used to process the execution results
   */
  private final class ExecResultSubscriber extends ExecutorSubscriber {
    ExecResultSubscriber(final Session session, final String sql, final Operator op) {
      super(session, sql, op);
    }

    @Override
    protected void doOnNext(final ExecResult result) {
      isEOF = result.isEof();
      if (result.getType() == ResultType.DML && session.getVarContext().isAutocommit() && session.getTxn().isPending()) {
        session.getTxn().commit()
                .subscribe(new CommitSubscriber(session, sql, result));
      } else {
        session.write(result, isEOF);
      }
    }
  }

  /**
   * @version V1.0
   */
  final class BatchExecResultSubscriber extends ExecutorSubscriber {
    private final int size;
    private final List<ExecResult> results;

    BatchExecResultSubscriber(Session session, String sql, int size) {
      super(session, sql, null);
      this.size = size;
      this.results = new ArrayList<>(size);
    }

    @Override
    protected void doOnNext(final ExecResult result) {
      results.add(result);
      isEOF = results.size() == size;
      if (isEOF) {
        if (session.getVarContext().isAutocommit() && session.getTxn().isPending()) {
          session.getTxn().commit()
                  .subscribe(new CommitSubscriber(session, sql, results));
        } else {
          replyBatchResult(session, results);
        }
      }
    }
  }

  /**
   * @version V1.0
   */
  private final class CommitSubscriber extends ExecutorSubscriber {
    private final ExecResult result;
    private final List<ExecResult> batchResults;

    private CommitSubscriber(final Session session, final String sql, final ExecResult result) {
      super(session, sql, null);
      this.isEOF = true;
      this.result = result;
      this.batchResults = null;
    }

    private CommitSubscriber(final Session session, final String sql, final List<ExecResult> batchResults) {
      super(session, sql, null);
      this.isEOF = true;
      this.result = null;
      this.batchResults = batchResults;
    }

    @Override
    protected void doOnNext(final ExecResult commit) {
      if (batchResults == null) {
        session.write(result, isEOF);
        return;
      }

      replyBatchResult(session, batchResults);
    }
  }

  /**
   * @version V1.0
   */
  abstract class ExecutorSubscriber extends BaseSubscriber<ExecResult> {
    protected final String sql;
    protected final Session session;
    protected final Operator op;
    protected volatile boolean isEOF = false;

    ExecutorSubscriber(final Session session, final String sql, final Operator op) {
      this.session = session;
      this.sql = sql;
      this.op = op;
    }

    @Override
    protected void hookOnSubscribe(Subscription subscription) {
      request(1);
    }

    abstract void doOnNext(ExecResult result);

    @Override
    protected void hookOnNext(final ExecResult result) {
      try {
        this.doOnNext(result);

        if (isEOF) {
          dispose();
        } else {
          request(1);
        }
      } catch (JimException ex) {
        replyError(session, sql, ex);
        dispose();
      } catch (Exception ex) {
        replyError(session, sql, DBException.get(ErrorModule.EXECUTOR, ErrorCode.ER_UNKNOWN_ERROR, ex));
        dispose();
      }
    }

    @Override
    protected void hookOnError(final Throwable throwable) {
      synchronized (session) {
        if (session.getTxn().isPending()) {
          try {
            session.getTxn().rollback().subscribe(r -> {
              if (LOG.isInfoEnabled()) {
                LOG.info("transaction rollback successful when request error");
              }
            }, e -> LOG.error("transaction rollback failed when request error", e));
          } catch (Exception ex) {
            LOG.error("transaction rollback call method error", ex);
          }
        }
      }

      if (throwable instanceof JimException) {
        replyError(this.session, sql, (JimException) throwable);
      } else {
        if (throwable instanceof TimeoutException) {
          replyError(this.session, sql, DBException.get(ErrorModule.EXECUTOR, ErrorCode.ER_QUERY_TIMEOUT, throwable));
        } else {
          replyError(this.session, sql, DBException.get(ErrorModule.EXECUTOR, ErrorCode.ER_UNKNOWN_ERROR, throwable));
        }
      }
    }

    @Override
    protected void hookOnCancel() {
      if (isEOF) {
        return;
      }
      replyError(this.session, sql, DBException.get(ErrorModule.EXECUTOR, ErrorCode.ER_SYSTEM_CANCELLED));
    }

    @Override
    protected void hookFinally(SignalType type) {
      if (this.op != null) {
        this.op.close();
      }
    }
  }

  /**
   * @version V1.0
   */
  private static final class PreparePlanCacheKey {
    public String database;
    public long connId;
    public long stmtID;
    public long schemaVersion;
    public int sqlMode;
    public int timeZone;

    private PreparePlanCacheKey(String database, long connId, long stmtID, long schemaVersion) {
      this.database = database;
      this.connId = connId;
      this.stmtID = stmtID;
      this.schemaVersion = schemaVersion;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result += prime * database.hashCode();
      result += prime * connId;
      result += prime * stmtID;
      result += prime * schemaVersion;
      result += prime * sqlMode;
      return result + prime * timeZone;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof PreparePlanCacheKey)) {
        return false;
      }
      PreparePlanCacheKey other = (PreparePlanCacheKey) obj;

      if (!other.database.equals(this.database)) {
        return false;
      }

      if (other.connId != this.connId) {
        return false;
      }

      if (other.stmtID != this.stmtID) {
        return false;
      }

      if (other.schemaVersion != this.schemaVersion) {
        return false;
      }

      if (other.sqlMode != this.sqlMode) {
        return false;
      }

      if (other.timeZone != this.timeZone) {
        return false;
      }

      return true;
    }
  }
}
