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
package io.jimdb.rpc.client.handler;

import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import io.jimdb.rpc.client.Connection;
import io.jimdb.rpc.client.command.Command;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * @version V1.0
 */
@ChannelHandler.Sharable
public final class CommandHandler extends ChannelInboundHandlerAdapter {
  private static final Logger LOG = LoggerFactory.getLogger(CommandHandler.class);

  private final Supplier<? extends Command> commandSupplier;
  private final ExecutorService workExecutor;

  public CommandHandler(final Supplier<? extends Command> commandSupplier, final ExecutorService workExecutor) {
    Preconditions.checkArgument(commandSupplier != null, "commandSupplier must be not null");
    this.commandSupplier = commandSupplier;
    this.workExecutor = workExecutor;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    final ByteBuf cmd = (ByteBuf) msg;
    final Channel channel = ctx.channel();
    final Connection conn = channel.attr(Connection.ATTR_KEY_CONN).get();

    if (conn == null) {
      cmd.release();
      LOG.error("the connect associated with the channel was not found");
    } else {
      if (workExecutor == null) {
        handleCommand(conn, channel, cmd);
      } else {
        workExecutor.execute(() -> handleCommand(conn, channel, cmd));
      }
    }
  }

  protected void handleCommand(final Connection conn, final Channel channel, final ByteBuf buf) {
    Command resp = null;
    try {
      resp = commandSupplier.get();
      resp.decode(buf);
    } catch (Exception ex) {
      LOG.error("command response decode error", ex);
      if (channel.isActive()) {
        channel.close();
      }
    } finally {
      buf.release();
    }

    if (resp == null) {
      return;
    }

    conn.response(resp.getReqID(), resp);
  }
}
