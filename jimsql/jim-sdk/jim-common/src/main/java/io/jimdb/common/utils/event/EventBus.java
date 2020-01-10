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
package io.jimdb.common.utils.event;

import java.io.Closeable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
public final class EventBus implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(EventBus.class);

  private static final AtomicLong COUNTER = new AtomicLong();

  private final String name;
  private final CopyOnWriteArrayList<EventListener> listeners;
  private final ExecutorService eventExecutor;

  public EventBus(String name, ExecutorService eventExecutor) {
    Preconditions.checkArgument(eventExecutor != null, "eventExecutor must not be null");
    this.name = StringUtils.isBlank(name) ? "EventBus-" + COUNTER.incrementAndGet() : name;
    this.eventExecutor = eventExecutor;
    this.listeners = new CopyOnWriteArrayList<>();
    if (LOG.isInfoEnabled()) {
      LOG.info("EventBus {} started", this.name);
    }
  }

  @SuppressFBWarnings("LO_TOSTRING_PARAMETER")
  public void publish(final Event e) {
    if (e == null) {
      return;
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("EventBus publish event {}", e.toString());
    }

    for (EventListener listener : listeners) {
      if (listener.isSupport(e)) {
        eventExecutor.execute(() -> listener.handleEvent(e));
      }
    }
  }

  public boolean addListener(final EventListener listener) {
    if (listener != null) {
      if (LOG.isInfoEnabled()) {
        LOG.info("EventBus add listener {}", listener.getName());
      }
      return listeners.addIfAbsent(listener);
    }
    return false;
  }

  public boolean removeListener(final EventListener listener) {
    if (listener != null) {
      if (LOG.isInfoEnabled()) {
        LOG.info("EventBus remove listener {}", listener.getName());
      }
      return listeners.remove(listener);
    }
    return false;
  }

  @Override
  public void close() {
    listeners.clear();
    if (LOG.isInfoEnabled()) {
      LOG.info("EventBus {} stopped", name);
    }
  }
}
