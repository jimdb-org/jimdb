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
package io.jimdb.core;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import io.jimdb.common.exception.BaseException;
import io.jimdb.common.utils.generator.ConnectionIdGenerator;
import io.jimdb.common.utils.lang.Resettable;
import io.jimdb.common.utils.os.SystemClock;
import io.jimdb.core.context.PreparedContext;
import io.jimdb.core.context.StatementContext;
import io.jimdb.core.context.TransactionContext;
import io.jimdb.core.context.VariableContext;
import io.jimdb.core.model.privilege.UserInfo;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.plugin.SQLEngine;
import io.jimdb.core.plugin.store.Engine;
import io.jimdb.core.plugin.store.Transaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The session context for a specific client connection.
 * The implementation of this interface must be guaranteed to be thread-safe.
 *
 * @version V1.0
 * @ThreadSafe
 */
public class Session implements Resettable {
  private static final Logger LOG = LoggerFactory.getLogger(Session.class);

  private static final AtomicLong SEQ_GEN = new AtomicLong();

  @sun.misc.Contended
  protected volatile byte seqID;
  @sun.misc.Contended
  protected volatile long lastTime;
  protected volatile long columnID;
  protected volatile Transaction txn;
  protected volatile UserInfo userInfo;

  protected final int connID;
  protected final Long sessionID;

  protected final Engine storeEngine;
  protected final VariableContext varContext;
  protected final StatementContext stmtContext;
  protected final TransactionContext txnContext;
  protected final PreparedContext preparedContext;
  protected final Map<String, Object> context;
  protected final AtomicBoolean closed = new AtomicBoolean(false);

  public Session(final SQLEngine sqlEngine, final Engine storeEngine) {
    this.storeEngine = storeEngine;
    this.connID = ConnectionIdGenerator.next();
    this.sessionID = SEQ_GEN.incrementAndGet();
    this.lastTime = SystemClock.currentTimeMillis();
    this.context = new ConcurrentHashMap<>(8);
    this.varContext = new VariableContext(sqlEngine, null);
    this.stmtContext = new StatementContext();
    this.txnContext = new TransactionContext();
    this.preparedContext = new PreparedContext();
  }

  public final long allocColumnID() {
    return ++columnID;
  }

  public final int getConnID() {
    return connID;
  }

  public final Long getSessionID() {
    return sessionID;
  }

  public final byte getSeqID() {
    return seqID;
  }

  public final void resetSeqID() {
    this.seqID = 0;
  }

  public final byte incrementAndGetSeqID() {
    return seqID++;
  }

  public final long getLastTime() {
    return lastTime;
  }

  public final void updateLastTime() {
    this.lastTime = SystemClock.currentTimeMillis();
  }

  public final VariableContext getVarContext() {
    return varContext;
  }

  public final StatementContext getStmtContext() {
    return stmtContext;
  }

  public TransactionContext getTxnContext() {
    return txnContext;
  }

  public PreparedContext getPreparedContext() {
    return preparedContext;
  }

  public final void putContext(String key, Object val) {
    this.context.put(key, val);
  }

  public final Object getContext(String key) {
    return this.context.get(key);
  }

  public final void removeContext(String key) {
    this.context.remove(key);
  }

  public Engine getStoreEngine() {
    return storeEngine;
  }

  /**
   * Return the current transaction. If the transaction is null, then create a new transaction.
   *
   * @return
   */
  public final Transaction getTxn() {
    if (this.txn == null) {
      return this.createTxn();
    }

    if (!this.varContext.isAutocommit()) {
      this.varContext.setStatus(SQLEngine.ServerStatus.INTRANS, true);
    }
    return this.txn;
  }

  /**
   * Create a new transaction. If old transaction is valid, then commit it first.
   *
   * @return
   */
  public final Transaction createTxn() {
    if (this.txn == null) {
      this.txn = this.storeEngine.beginTxn(this);
    } else if (this.txn.isPending()) {
      this.txn.commit().blockFirst();
    }

    return this.txn;
  }

  public void initTxnContext() {
    txnContext.init();
  }

  @Override
  public void reset() {
    resetSeqID();
    if (txn == null || !txn.isPending()) {
      txnContext.reset();
    }
    this.varContext.reset();
    this.preparedContext.reset();
    this.stmtContext.close();
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      synchronized (this) {
        if (txn != null && txn.isPending()) {
          txn.rollback().subscribe(r -> {
            if (LOG.isInfoEnabled()) {
              LOG.info("transaction rollback successful when session close");
            }
          }, e -> LOG.error("transaction rollback failed when session close", e));
        }
      }

      txnContext.close();
      varContext.close();
      stmtContext.close();
      txnContext.close();
      preparedContext.close();
      context.clear();
    }
  }

  /**
   * Write the ExecResult to this <code>Session</code> associated connection.
   *
   * @param rs
   * @throws Exception
   */
  public void write(ExecResult rs, boolean isEof) {
    throw new UnsupportedOperationException("unsupported write result");
  }

  /**
   * Write Error to this <code>Session</code> associated connection.
   *
   * @param ex
   * @throws Exception
   */
  public void writeError(BaseException ex) {
    throw new UnsupportedOperationException("unsupported write error");
  }

  /**
   * Get the remote address
   *
   * @return ip:port
   * @throws Exception
   */
  public String getRemoteAddress() {
    throw new UnsupportedOperationException("get remote address");
  }

  public UserInfo getUserInfo() {
    return userInfo;
  }

  public void setUserInfo(UserInfo userInfo) {
    this.userInfo = userInfo;
  }
}
