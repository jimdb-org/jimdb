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
package io.jimdb.rpc.client;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;

import io.jimdb.common.utils.event.Event;
import io.jimdb.common.utils.event.EventListener;
import io.jimdb.common.utils.retry.RetryCallback;
import io.jimdb.common.utils.retry.RetryPolicy;
import io.jimdb.common.utils.retry.RetryTask;

import com.google.common.base.Preconditions;

/**
 * @version V1.0
 */
public final class ConnectEventListener implements EventListener {
  private final ConcurrentHashMap<String, Boolean> reconnects;

  private final ScheduledExecutorService retryExecutor;

  private final RetryPolicy retryPolicy;

  public ConnectEventListener(ScheduledExecutorService retryExecutor) {
    Preconditions.checkArgument(retryExecutor != null, "retryExecutor must not be null");
    this.retryExecutor = retryExecutor;
    this.reconnects = new ConcurrentHashMap<>();

    RetryPolicy.Builder retryBuilder = new RetryPolicy.Builder();
    retryBuilder.retryDelay(200)
            .maxRetryDelay(3000)
            .maxRetry(1000)
            .expireTime(0)
            .useExponentialBackOff(true)
            .backOffMultiplier(1.2);
    this.retryPolicy = retryBuilder.build();
  }

  @Override
  public String getName() {
    return "ConnectEventListener";
  }

  @Override
  public boolean isSupport(Event e) {
    return e instanceof ConnectEvent && ((ConnectEvent) e).getEventType() != ConnectEvent.EventType.HEARTBEAT;
  }

  @Override
  public void handleEvent(Event e) {
    final ConnectEvent connEvent = (ConnectEvent) e;
    final Connection conn = connEvent.getConn();
    if (conn == null) {
      return;
    }

    final ConnectEvent.EventType type = connEvent.getEventType();
    boolean reconnect = false;
    if (type == ConnectEvent.EventType.CLOSE || type == ConnectEvent.EventType.RECONNECT || type == ConnectEvent.EventType.EXCEPTION) {
      reconnect = reconnects.putIfAbsent(conn.getID(), Boolean.TRUE) == null;
    }

    if (reconnect) {
      conn.reconnect = true;
      retryExecutor.execute(new RetryTask("Reconnect-" + conn.getID(), retryPolicy, retryExecutor, new RetryCallback() {
        @Override
        public boolean execute() {
          return conn.reConnect();
        }

        @Override
        public void onTerminate(boolean success) {
          reconnects.remove(conn.getID());
          conn.reconnect = false;
        }
      }));

      conn.logEvent(ConnectEvent.EventType.RECONNECT);
    }
  }
}
