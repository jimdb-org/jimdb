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
package reactor.core.scheduler;

import java.io.Closeable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;

/**
 * @version V1.0
 */
public final class ExecutorSchedulerFactory implements Schedulers.Factory, Closeable {
  private final JimExecutorScheduler scheduler;

  public ExecutorSchedulerFactory(final ExecutorService pool, final int schedulerParallel) {
    this.scheduler = new JimExecutorScheduler(pool, schedulerParallel);
  }

  @Override
  public Scheduler newElastic(int ttlSeconds, ThreadFactory threadFactory) {
    return this.scheduler;
  }

  @Override
  public Scheduler newParallel(int parallelism, ThreadFactory threadFactory) {
    return this.scheduler;
  }

  @Override
  public void close() {
    scheduler.close();
  }
}
