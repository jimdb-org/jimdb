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
package io.jimdb.rpc.client.heartbeat;

import io.jimdb.rpc.client.ConnectEvent;
import io.jimdb.rpc.client.Connection;
import io.jimdb.rpc.client.command.Command;
import io.jimdb.rpc.client.command.CommandCallback;
import io.jimdb.common.utils.event.Event;
import io.jimdb.common.utils.event.EventListener;
import io.netty.channel.Channel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * @version V1.0
 */
public final class HeartbeatListener implements EventListener {
  private static final Logger LOG = LoggerFactory.getLogger(HeartbeatListener.class);

  private final HeartbeatCommand heartbeat;

  public HeartbeatListener(final HeartbeatCommand heartbeat) {
    Preconditions.checkArgument(heartbeat != null, "heartbeatCommand must not be null");
    this.heartbeat = heartbeat;
  }

  @Override
  public String getName() {
    return "HeartbeatListener";
  }

  @Override
  public boolean isSupport(Event e) {
    return e instanceof ConnectEvent && ((ConnectEvent) e).getEventType() == ConnectEvent.EventType.HEARTBEAT;
  }

  @Override
  public void handleEvent(Event e) {
    final ConnectEvent connEvent = (ConnectEvent) e;
    final Connection conn = connEvent.getConn();
    if (conn == null) {
      return;
    }

    Channel channel = conn.getChannel();
    try {
      conn.request(heartbeat.build(), new CommandCallback<Command>() {
        @Override
        protected boolean onSuccess0(Command request, Command response) {
          if (!heartbeat.verify(request, response)) {
            LOG.error("Connection {} heartbeat response verify failed", conn.getID());
            if (channel != null && channel.isActive()) {
              channel.close();
            }
          }
          return false;
        }

        @Override
        protected boolean onFailed0(Command request, Throwable cause) {
          LOG.error(String.format("Connection %s heartbeat response error", conn.getID()), cause);
          if (channel != null && channel.isActive()) {
            channel.close();
          }
          return false;
        }
      });
    } catch (Exception ex) {
      LOG.error(String.format("Connection %s send heartbeat error", conn.getID()), ex);
      if (channel != null && channel.isActive()) {
        channel.close();
      }
    }
  }
}
