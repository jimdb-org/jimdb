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
package io.jimdb.sql.server.netty;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import io.jimdb.core.Session;
import io.jimdb.common.config.NettyServerConfig;
import io.jimdb.core.plugin.SQLEngine;
import io.jimdb.core.plugin.store.Engine;
import io.jimdb.common.utils.lang.NamedThreadFactory;
import io.jimdb.common.utils.lang.NetUtil;
import io.jimdb.common.utils.os.SystemClock;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.AttributeKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings("HES_EXECUTOR_NEVER_SHUTDOWN")
@ChannelHandler.Sharable
public final class SessionHandler extends ChannelDuplexHandler implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(SessionHandler.class);

  static final AttributeKey<Session> NETTY_CHANNEL_SESSION = AttributeKey.valueOf("jim.netty.session");

  private final int maxIdle;
  private final SQLEngine sqlEngine;
  private final Engine storeEngine;
  private final ExecutorService workExecutor;
  private final Map<Long, Session> sessionMap;
  private final ChannelFutureListener writeListener;
  private final ScheduledExecutorService idleExecutor;

  public SessionHandler(final NettyServerConfig config, final SQLEngine sqlEngine, final Engine storeEngine) {
    Preconditions.checkArgument(config != null, "NettyServerConfig must not be null");
    Preconditions.checkArgument(sqlEngine != null, "SQLEngine must not be null");
    Preconditions.checkArgument(storeEngine != null, "StoreEngine must not be null");

    this.sqlEngine = sqlEngine;
    this.storeEngine = storeEngine;
    this.maxIdle = config.getMaxIdle();
    this.workExecutor = config.getWorkExecutor();
    this.sessionMap = new ConcurrentHashMap<>();
    this.writeListener = new ChannelFutureListener() {
      @Override
      public void operationComplete(ChannelFuture future) throws Exception {
        Session session = future.channel().attr(NETTY_CHANNEL_SESSION).get();
        if (session != null) {
          session.updateLastTime();
        }
      }
    };

    this.idleExecutor = new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("Rpc-Server-IdleEvictor", true));
    this.idleExecutor.scheduleWithFixedDelay(new IdleTask(), this.maxIdle, this.maxIdle, TimeUnit.MILLISECONDS);
  }

  @Override
  public void channelActive(final ChannelHandlerContext context) {
    final Session session = new NettySession(context.channel(), this.sqlEngine, this.storeEngine);
    context.channel().attr(NETTY_CHANNEL_SESSION).set(session);
    this.sessionMap.put(session.getSessionID(), session);
    this.sqlEngine.handShake(session);
  }

  @Override
  public void channelRead(ChannelHandlerContext context, Object message) {
    ByteBuf in = (ByteBuf) message;
    if (LOG.isDebugEnabled()) {
      LOG.debug("jim server read from client {} : \n {}", context.channel().id().asShortText(), ByteBufUtil.prettyHexDump(in));
    }

    final Session session = context.channel().attr(NETTY_CHANNEL_SESSION).get();
    session.updateLastTime();

    if (workExecutor == null) {
      sqlEngine.handleCommand(session, in);
    } else {
      workExecutor.execute(() -> {
        try {
          sqlEngine.handleCommand(session, in);
        } catch (Exception ex) {
          LOG.error("sqlEngine handleCommand error", ex);
        }
      });
    }
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
    ctx.write(msg, promise.unvoid()).addListener(writeListener);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    final Session session = ctx.channel().attr(NETTY_CHANNEL_SESSION).get();
    if (session != null) {
      session.close();
      sessionMap.remove(session.getSessionID());
    }

    super.channelInactive(ctx);
  }

  @Override
  public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
    final Channel channel = ctx.channel();
    final Session session = channel.attr(NETTY_CHANNEL_SESSION).get();
    if (session != null) {
      session.close();
      sessionMap.remove(session.getSessionID());
    }

    if (channel.isActive()) {
      try {
        channel.close().await();
      } catch (InterruptedException ignored) {
      }
    }

    LOG.error(String.format("server connection channel %s exception", NetUtil.toIP(channel.remoteAddress())), cause);
  }

  @Override
  public void close() {
    idleExecutor.shutdown();
  }

  /**
   * Session idle check task.
   */
  private final class IdleTask implements Runnable {
    @Override
    public void run() {
      sessionMap.forEach((sessID, session) -> {
        if (SystemClock.currentTimeMillis() - session.getLastTime() > maxIdle) {
          session.close();
          sessionMap.remove(sessID);
        }
      });
    }
  }
}
