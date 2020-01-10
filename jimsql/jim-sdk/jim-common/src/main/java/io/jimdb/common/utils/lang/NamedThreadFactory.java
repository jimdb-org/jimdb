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
package io.jimdb.common.utils.lang;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @version V1.0
 */
public final class NamedThreadFactory implements ThreadFactory {
  private final String name;
  private final boolean daemon;
  private final AtomicLong counter = new AtomicLong(0);

  public NamedThreadFactory(final String name, final boolean daemon) {
    if (name == null || name.isEmpty()) {
      throw new IllegalArgumentException("name must be not empty");
    }
    this.name = name;
    this.daemon = daemon;
  }

  @Override
  public Thread newThread(Runnable r) {
    Thread thread = new Thread(r);
    thread.setName(name + " - " + counter.incrementAndGet());
    if (daemon) {
      thread.setDaemon(daemon);
    }
    return thread;
  }
}
