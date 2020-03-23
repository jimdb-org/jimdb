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

import java.io.Closeable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import io.jimdb.common.utils.lang.NamedThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Distributed request processing involving multiple ShardSender requests
 */
@SuppressFBWarnings()
public final class AsyncTaskManager implements Dispatcher, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(AsyncTaskManager.class);

  private static final int ASYNC_QUEUE_NUM = 1000000;
  private static final int ASYNC_THREAD_NUM = 4;

  private final BlockingQueue<Runnable> asyncQueue;
  private final ExecutorService asyncExecutorPool;
  //private final ShardSender shardSender;
  private volatile boolean isRunning = true;

  public AsyncTaskManager() {
    //this.shardSender = new ShardSender(config, routeManager);
    this.asyncQueue = new LinkedBlockingQueue<>(ASYNC_QUEUE_NUM);
    this.asyncExecutorPool = new ThreadPoolExecutor(ASYNC_THREAD_NUM, ASYNC_THREAD_NUM, 50L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>(), new NamedThreadFactory("DistSender-Asyncer", true));
  }

  @Override
  public void start() {
    for (int i = 0; i < ASYNC_THREAD_NUM; i++) {
      this.asyncExecutorPool.execute(() -> {
        while (isRunning) {
          try {
            if (LOG.isInfoEnabled()) {
              LOG.info("take task from queue");
            }
            Runnable task = asyncQueue.take();
            task.run();
          } catch (InterruptedException e1) {
            LOG.warn("take task from queue interrupted");
          } catch (Throwable e2) {
            LOG.error("take task from queue err", e2);
          }
        }
      });
    }
  }

  @Override
  public void enqueue(Runnable runnable) {
    try {
      this.asyncQueue.put(runnable);
    } catch (InterruptedException e) {
      LOG.warn("async queue interrupted");
    }
  }

  @Override
  public void close() {
    this.isRunning = false;
    //this.shardSender.close();
    this.asyncQueue.clear();
    this.asyncExecutorPool.shutdown();
  }
}
