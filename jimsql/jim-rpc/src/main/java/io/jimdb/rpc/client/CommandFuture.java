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

import java.util.concurrent.atomic.AtomicBoolean;

import io.jimdb.rpc.client.command.Command;
import io.jimdb.rpc.client.command.CommandCallback;
import io.jimdb.common.utils.os.SystemClock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @version V1.0
 */
public final class CommandFuture {
  private static final Logger LOG = LoggerFactory.getLogger(CommandFuture.class);

  // Event time
  final long startTime;
  // Request timeout
  final long timeout;

  final Command request;

  final CommandCallback callback;

  private final AtomicBoolean onceCallback = new AtomicBoolean(false);

  CommandFuture(final Command request, final CommandCallback callback, final long startTime, final long timeout) {
    this.request = request;
    this.callback = callback;
    this.timeout = timeout;
    this.startTime = startTime;
  }

  public boolean isTimeout() {
    return SystemClock.currentTimeMillis() - startTime >= timeout + 1000;
  }


  /**
   * Execute request callback
   */
  void complete(final Command response, final Throwable error) {
    if (callback == null || !onceCallback.compareAndSet(false, true)) {
      return;
    }

    long reqID = request.getReqID();
    try {
      if (error != null) {
        callback.onFailed(request, error);
      } else if (response != null) {
        callback.onSuccess(request, response);
      } else {
        LOG.error("request callback bug: {} success and error are not assigned", reqID);
      }
    } catch (Exception ex) {
      LOG.error("request callback error " + reqID, ex);
    }
  }
}
