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
package io.jimdb.engine;

import java.time.Instant;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import io.jimdb.common.utils.os.SystemClock;
import io.jimdb.common.utils.retry.RetryPolicy;
import io.jimdb.core.Session;
import io.jimdb.core.model.meta.Table;
import io.jimdb.meta.Router;
import io.jimdb.meta.route.RoutingPolicy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @version V1.0
 */
public class StoreCtx {
  private static final Logger LOGGER = LoggerFactory.getLogger(StoreCtx.class);

  private static final AtomicLong ID = new AtomicLong(0L);
  private long cxtId;

  private Table table;
  private RoutingPolicy routingPolicy;
  private final Instant timeout;

  private TimeZone timeZone = TimeZone.getDefault();

  private final Router router;

  private RetryPolicy retryPolicy;
  private AtomicInteger retry = new AtomicInteger(0);

  private StoreCtx(final Table table, final Instant timeout, Router router) {
    this.cxtId = ID.incrementAndGet();
    this.table = table;
    this.router = router;
    this.routingPolicy = router.getOrCreatePolicy(table.getCatalog().getId(), table.getId());
    this.timeout = timeout;
    this.retryPolicy = new RetryPolicy.Builder().retryDelay(20).useExponentialBackOff(true).backOffMultiplier(1.35)
            .build();
  }

  public static StoreCtx buildCtx(Session session, Table table, Router router) {
    Instant timeout = null;
    if (session != null) {
      timeout = session.getStmtContext().getTimeout();
    }

    if (timeout == null) {
      return new StoreCtx(table, SystemClock.currentTimeStamp().plusSeconds(20), router);
    }
    return new StoreCtx(table, timeout, router);
  }

  @Deprecated
  public static StoreCtx buildCtx(final Table table, final Instant timeout, Router rpcManager) {
    if (timeout == null) {
      return new StoreCtx(table, SystemClock.currentTimeStamp().plusSeconds(20), rpcManager);
    }
    return new StoreCtx(table, timeout, rpcManager);
  }

  public Table getTable() {
    return table;
  }

  public RoutingPolicy getRoutingPolicy() {
    return routingPolicy;
  }

  public Instant getTimeout() {
    return timeout;
  }

  public boolean isTimeout() {
    return SystemClock.currentTimeStamp().isAfter(this.timeout);
  }

  public boolean canRetryWithDelay() {
    long delay = retryDelay();
    if (delay < 0) {
      return false;
    }
    try {
      Thread.sleep(delay);
    } catch (InterruptedException e) {
      LOGGER.warn("store ctx retry sleep failure.");
    }
    return true;
  }

  public long retryDelay() {
    this.retry.addAndGet(1);
    return retryPolicy.getDelay(this.timeout, retry.get());
  }

  public Router getRouter() {
    return router;
  }

  public void setTable(Table table) {
    this.table = table;
  }

  public long getCxtId() {
    return cxtId;
  }

  public TimeZone getTimeZone() {
    return timeZone;
  }
}
