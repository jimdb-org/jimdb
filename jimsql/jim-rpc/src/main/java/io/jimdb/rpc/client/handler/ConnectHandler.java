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
package io.jimdb.rpc.client.handler;

import io.jimdb.rpc.client.ConnectEvent;
import io.jimdb.rpc.client.Connection;
import io.jimdb.common.utils.event.EventBus;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * @version V1.0
 */
@ChannelHandler.Sharable
public final class ConnectHandler extends ChannelDuplexHandler {
  private static final Logger LOG = LoggerFactory.getLogger(ConnectHandler.class);

  private final EventBus eventBus;

  public ConnectHandler(EventBus eventBus) {
    Preconditions.checkArgument(eventBus != null, "eventBus must not be null");
    this.eventBus = eventBus;
  }

  @Override
  public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
    super.channelInactive(ctx);

    Connection conn = ctx.channel().attr(Connection.ATTR_KEY_CONN).get();
    if (conn != null) {
      eventBus.publish(new ConnectEvent(conn, ConnectEvent.EventType.CLOSE));
    }
  }

  @Override
  public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
    Channel channel = ctx.channel();
    Connection conn = channel.attr(Connection.ATTR_KEY_CONN).get();
    if (conn != null) {
      eventBus.publish(new ConnectEvent(conn, ConnectEvent.EventType.EXCEPTION));
      LOG.error(String.format("connection %s channel exception ", conn.getID()), cause);
    }

    if (channel.isActive()) {
      try {
        channel.close().await();
      } catch (InterruptedException ignored) {
      }
    }
  }
}
