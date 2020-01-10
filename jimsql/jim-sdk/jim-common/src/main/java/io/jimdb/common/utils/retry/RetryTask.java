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
package io.jimdb.common.utils.retry;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import io.jimdb.common.utils.os.SystemClock;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * @version V1.0
 */
public final class RetryTask implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(RetryTask.class);

  private static final AtomicLong COUNTER = new AtomicLong();

  @sun.misc.Contended
  private volatile long retrys = 1;

  private final long startTime;
  private final String name;
  private final ScheduledExecutorService retryExecutor;
  private final RetryCallback callback;
  private final RetryPolicy policy;

  public RetryTask(final String name, final RetryPolicy policy, final ScheduledExecutorService retryExecutor, final RetryCallback callback) {
    Preconditions.checkArgument(callback != null, "callback must not be null");
    Preconditions.checkArgument(policy != null, "policy must not be null");
    Preconditions.checkArgument(retryExecutor != null, "retryExecutor must not be null");
    this.name = StringUtils.isBlank(name) ? "RetryTask-" + COUNTER.incrementAndGet() : name;
    this.callback = callback;
    this.policy = policy;
    this.retryExecutor = retryExecutor;
    this.startTime = SystemClock.currentTimeMillis();
  }

  @Override
  public void run() {
    if (LOG.isInfoEnabled()) {
      LOG.info("RetryTask {} start at {}ms and retrying {} call", name, startTime, retrys);
    }

    boolean ok = false;
    try {
      ok = callback.execute();
    } catch (Exception ex) {
      if (LOG.isInfoEnabled()) {
        LOG.info(String.format("RetryTask %s start at %dms and retry %d execute call error", name, startTime, retrys), ex);
      }
    }

    if (ok) {
      if (LOG.isInfoEnabled()) {
        LOG.info("RetryTask {} start at {}ms and retry {} execute call successful return", name, startTime, retrys);
      }

      callback.onTerminate(true);
      return;
    }

    int retry = (int) retrys++;
    retry = retry < 0 ? Integer.MAX_VALUE : retry;
    long delay = policy.getDelay(startTime, retry);
    if (delay < 0) {
      if (LOG.isInfoEnabled()) {
        LOG.info("RetryTask {} start at {}ms and retry {} execute call exceed return", name, startTime, retrys);
      }

      callback.onTerminate(false);
      return;
    }

    retryExecutor.schedule(this, delay, TimeUnit.MILLISECONDS);
  }
}
