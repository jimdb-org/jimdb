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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import io.jimdb.common.config.NettyClientConfig;
import io.jimdb.common.exception.ConnectException;
import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.exception.JimException;
import io.jimdb.rpc.client.command.Command;
import io.jimdb.rpc.client.command.CommandCallback;
import io.jimdb.common.utils.event.EventBus;
import io.jimdb.common.utils.os.SystemClock;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.util.AttributeKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings("EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS")
public final class Connection {
  private static final Logger LOG = LoggerFactory.getLogger(Connection.class);

  public static final AttributeKey<Connection> ATTR_KEY_CONN = AttributeKey.valueOf("RPC_CONN");

  @sun.misc.Contended
  private volatile long lastTime;
  @sun.misc.Contended
  volatile boolean reconnect = false;
  private volatile boolean closed = false;
  private volatile Channel channel = null;

  private final int port;
  private final String id;
  private final String ip;
  private final NettyClientConfig config;
  private final Bootstrap bootstrap;
  private final EventBus eventBus;
  private final ConcurrentHashMap<Long, CommandFuture> pendings;
  private final AtomicLong seq = new AtomicLong(0);
  private final Lock lock = new ReentrantLock();

  @SuppressFBWarnings("STT_TOSTRING_STORED_IN_FIELD")
  Connection(final NettyClientConfig config, final String address, final int num, final Bootstrap bootstrap, final EventBus eventBus) {
    this.closed = false;
    this.config = config;
    this.id = address + "-" + num;
    this.bootstrap = bootstrap;
    this.eventBus = eventBus;
    this.lastTime = SystemClock.currentTimeMillis();
    this.pendings = new ConcurrentHashMap<>(512, 0.75f);

    String[] parts = address.split("[._:]");
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < parts.length - 1; i++) {
      if (i > 0) {
        builder.append('.');
      }
      builder.append(parts[i]);
    }
    this.ip = builder.toString();
    this.port = Integer.parseInt(parts[parts.length - 1]);
  }

  public <T extends Command> void request(final T command, final CommandCallback<T> callback) throws JimException {
    if (command == null) {
      throw DBException.get(ErrorModule.NETRPC, ErrorCode.ER_RPC_REQUEST_INVALID);
    }

    long startTime = SystemClock.currentTimeMillis();
    int timeout = command.getTimeout();
    if (timeout <= 0) {
      timeout = config.getSendTimeout();
    }

    this.incrLastTime();
    if (!reconnect && (channel == null || !channel.isActive())) {
      init();
    }

    final Channel curChannel = channel;
    if (curChannel == null || !curChannel.isActive()) {
      throw ConnectException.get(ErrorCode.ER_RPC_CONNECT_INACTIVE, id);
    }

    Long reqID = command.getReqID();
    if (reqID == null || reqID <= 0) {
      reqID = seq.addAndGet(1);
      command.setReqID(reqID);
    }
    CommandFuture cmdFuture = new CommandFuture(command, callback, startTime, timeout);

    ByteBuf buf = null;
    try {
      buf = curChannel.alloc().buffer(command.size());
      command.encode(buf);
      if (callback != null) {
        pendings.put(reqID, cmdFuture);
      }

      curChannel.writeAndFlush(buf).addListener(new ResponseListener<T>(reqID, this));
    } catch (Exception ex) {
      pendings.remove(reqID);
      if (buf != null) {
        buf.release();
      }

      if (ex instanceof JimException) {
        throw ex;
      }
      throw DBException.get(ErrorModule.NETRPC, ErrorCode.ER_RPC_REQUEST_ERROR, ex, id, String.valueOf(reqID));
    }
  }

  public <T extends Command> void response(final Long reqID, final T resp) {
    this.incrLastTime();
    CommandFuture cmdFuture = pendings.remove(reqID);
    if (cmdFuture == null) {
      if (LOG.isWarnEnabled()) {
        LOG.warn("connection {} find command {} is timeout", id, reqID);
      }
      return;
    }

    cmdFuture.complete(resp, null);
  }

  public void response(final Long reqID, final Throwable error) {
    this.incrLastTime();
    CommandFuture cmdFuture = pendings.remove(reqID);
    if (cmdFuture == null) {
      if (LOG.isWarnEnabled()) {
        LOG.warn("connection {} find command {} is timeout", id, reqID);
      }
      return;
    }

    cmdFuture.complete(null, error);
  }

  public void logEvent(ConnectEvent.EventType type) {
    if (!LOG.isDebugEnabled()) {
      return;
    }

    switch (type) {
      case CONNECT:
        LOG.debug("connection {} active at {}", id, SystemClock.currentTimeMillis());
        break;
      case CLOSE:
        LOG.debug("connection {} inactive at {}", id, SystemClock.currentTimeMillis());
        break;
      case EXCEPTION:
        LOG.debug("connection {} exception at {}", id, SystemClock.currentTimeMillis());
        break;
      case RECONNECT:
        LOG.debug("connection {} reconnect at {}", id, SystemClock.currentTimeMillis());
        break;
      case HEARTBEAT:
        LOG.debug("connection {} heartbeat at {}", id, SystemClock.currentTimeMillis());
        break;
      default:
        break;
    }
  }

  public String getID() {
    return id;
  }

  public Channel getChannel() {
    return channel;
  }

  boolean evictIdle() {
    if (pendings.isEmpty()) {
      this.close();
      return true;
    }

    return false;
  }

  void evictTimeout() {
    pendings.forEach((reqID, future) -> {
      if (future.isTimeout()) {
        if (LOG.isWarnEnabled()) {
          LOG.warn("connection {} remove timeout command {}, begin at {} and timeout at {}", id, reqID, future.startTime, SystemClock.currentTimeMillis());
        }

        final ExecutorService workExecutor = config.getWorkExecutor();
        if (workExecutor == null) {
          response(reqID, DBException.get(ErrorModule.NETRPC, ErrorCode.ER_RPC_REQUEST_TIMEOUT, id, String.valueOf(reqID)));
        } else {
          workExecutor.execute(() -> response(reqID, DBException.get(ErrorModule.NETRPC, ErrorCode.ER_RPC_REQUEST_TIMEOUT, id, String.valueOf(reqID))));
        }
      }
    });
  }

  void incrLastTime() {
    this.lastTime = SystemClock.currentTimeMillis();
  }

  void init() throws JimException {
    lock.lock();
    try {
      if ((channel != null && channel.isActive()) || reconnect || closed) {
        return;
      }

      connect();
    } catch (Exception ex) {
      eventBus.publish(new ConnectEvent(this, ConnectEvent.EventType.RECONNECT));
      LOG.error("Init connect error ", ex);
    } finally {
      lock.unlock();
    }
  }

  boolean reConnect() {
    lock.lock();
    try {
      if ((channel != null && channel.isActive()) || closed) {
        return true;
      }

      final ExecutorService workExecutor = config.getWorkExecutor();
      pendings.forEach((reqID, future) -> {
        if (workExecutor == null) {
          response(reqID, ConnectException.get(ErrorCode.ER_RPC_CONNECT_INACTIVE, id));
        } else {
          workExecutor.execute(() -> response(reqID, ConnectException.get(ErrorCode.ER_RPC_CONNECT_INACTIVE, id)));
        }
      });

      connect();
      return true;
    } finally {
      lock.unlock();
    }
  }

  void connect() throws JimException {
    try {
      SocketAddress socketAddr = new InetSocketAddress(InetAddress.getByName(ip), port);
      ChannelFuture channelFuture = bootstrap.connect(socketAddr);
      if (!channelFuture.await(config.getConnTimeout() + 1)) {
        throw ConnectException.get(ErrorCode.ER_RPC_CONNECT_TIMEOUT, id);
      }
      Channel channel0 = channelFuture.channel();
      if (channel0 == null || !channel0.isActive()) {
        throw ConnectException.get(ErrorCode.ER_RPC_CONNECT_INACTIVE, id);
      }

      channel0.attr(ATTR_KEY_CONN).set(this);
      this.channel = channel0;
      this.logEvent(ConnectEvent.EventType.CONNECT);
    } catch (UnknownHostException ex) {
      throw ConnectException.get(ErrorCode.ER_RPC_CONNECT_UNKNOWN, ex, id);
    } catch (InterruptedException ex) {
      throw ConnectException.get(ErrorCode.ER_RPC_CONNECT_INTERRUPTED, ex, id, ex.getMessage());
    } finally {
      this.incrLastTime();
    }
  }

  boolean shouldHeartbeat() {
    if (reconnect || closed) {
      return false;
    }

    return SystemClock.currentTimeMillis() - lastTime >= config.getHeartbeatInterval();
  }

  boolean isActive() {
    return channel == null || channel.isActive();
  }

  void close() {
    lock.lock();
    try {
      if (closed) {
        return;
      }

      closed = true;
      if (channel != null && channel.isActive()) {
        channel.close();
      }
      if (LOG.isInfoEnabled()) {
        LOG.info("connection {} closed at {}", id, SystemClock.currentTimeMillis());
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Write response listener.
   *
   * @param <T>
   */
  static final class ResponseListener<T extends Command> implements ChannelFutureListener {
    final Long reqID;
    final Connection conn;

    ResponseListener(final Long reqID, final Connection conn) {
      this.reqID = reqID;
      this.conn = conn;
    }

    @Override
    public void operationComplete(final ChannelFuture future) {
      if (!future.isSuccess()) {
        try {
          conn.response(reqID, ConnectException.get(ErrorCode.ER_RPC_CONNECT_ERROR, future.cause(), conn.id));
        } finally {
          Channel channel = future.channel();
          if (channel.isActive()) {
            channel.close();
          }
        }
      }
    }
  }
}
