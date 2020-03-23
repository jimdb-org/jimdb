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

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import io.jimdb.common.config.NettyClientConfig;
import io.jimdb.common.utils.event.Event;
import io.jimdb.common.utils.event.EventBus;
import io.jimdb.common.utils.event.EventListener;
import io.jimdb.common.utils.lang.NamedThreadFactory;
import io.jimdb.rpc.ChannelPipeline;
import io.jimdb.rpc.client.command.Command;
import io.jimdb.rpc.client.command.CommandCallback;
import io.jimdb.rpc.client.handler.CommandHandler;
import io.jimdb.rpc.client.handler.ConnectHandler;
import io.jimdb.rpc.client.heartbeat.HeartbeatCommand;
import io.jimdb.rpc.client.heartbeat.HeartbeatListener;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelOption;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;

import com.google.common.base.Preconditions;

/**
 * @version V1.0
 */
public final class NettyClient implements Closeable {
  private final NettyClientConfig config;
  private final ConcurrentMap<String, ConnectPool> poolMap;
  private final List<ConnectPool> evictPools;
  private final Bootstrap bootstrap;
  private final ConnectHandler connectHandler;
  private final CommandHandler commandHandler;
  private final EventBus eventBus;
  private final ScheduledExecutorService idleExecutor;
  private final ScheduledExecutorService timeoutExecutor;
  private final ScheduledExecutorService eventExecutor;

  public NettyClient(final NettyClientConfig config, final Supplier<? extends Command> commandSupplier,
                     final Supplier<? extends ByteToMessageDecoder> decoderSupplier, final HeartbeatCommand heartbeat) {
    Preconditions.checkArgument(config != null, "clientConfig must not be null");
    Preconditions.checkArgument(commandSupplier != null, "commandSupplier must not be null");
    Preconditions.checkArgument(decoderSupplier != null, " decoderSupplier must not be null");

    this.eventExecutor = new ScheduledThreadPoolExecutor(Math.min(4, Runtime.getRuntime().availableProcessors() / 2), new NamedThreadFactory("Rpc-Client-EventExecutor", true));
    this.eventBus = new EventBus("Rpc-Client-EventBus", this.eventExecutor);
    this.connectHandler = new ConnectHandler(this.eventBus);
    this.commandHandler = new CommandHandler(commandSupplier, config.getWorkExecutor());
    config.setHandlerSupplier(() -> {
      return new ChannelHandler[]{ decoderSupplier.get(), commandHandler, connectHandler };
    });

    config.validate();
    this.config = config;
    this.bootstrap = buildBootstrap();
    this.poolMap = new ConcurrentHashMap<>();
    this.evictPools = new CopyOnWriteArrayList<>();

    this.idleExecutor = new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("Rpc-Client-IdleEvictor", true));
    int idleTime = Math.min(config.getHeartbeatInterval(), config.getMaxIdle());
    this.idleExecutor.scheduleWithFixedDelay(new IdleTask(), idleTime, idleTime, TimeUnit.MILLISECONDS);

    this.timeoutExecutor = new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("Rpc-Client-TimeoutEvictor", true));
    int timeout = config.getSendTimeout() / 2;
    timeout = timeout < 1000 ? config.getSendTimeout() : timeout;
    this.timeoutExecutor.scheduleWithFixedDelay(new TimeoutTask(), timeout, timeout, TimeUnit.MILLISECONDS);

    this.eventBus.addListener(new ConnectEventListener(this.eventExecutor));
    if (heartbeat != null) {
      this.eventBus.addListener(new HeartbeatListener(heartbeat));
    }
  }

  public boolean addListener(final EventListener listener) {
    return eventBus.addListener(listener);
  }

  public boolean removeListener(final EventListener listener) {
    return eventBus.removeListener(listener);
  }

  public void publish(final Event e) {
    eventBus.publish(e);
  }

  public <T extends Command, C extends CommandCallback<T>> void send(final String addr, final T command, final C callback) {
    try {
      getConn(addr).request(command, callback);
    } catch (Exception ex) {
      if (callback != null) {
        callback.onFailed(command, ex);
      } else {
        throw ex;
      }
    }
  }

  private Connection getConn(String addr) {
    ConnectPool pool = poolMap.get(addr);
    if (pool == null) {
      pool = new ConnectPool(config, addr, bootstrap, eventBus);
      ConnectPool oldPool = poolMap.putIfAbsent(addr, pool);
      if (oldPool != null) {
        pool.close();
        pool = oldPool;
      }
    }

    return pool.getConn();
  }

  private Bootstrap buildBootstrap() {
    Bootstrap bootstrap = new Bootstrap();
    bootstrap.group(config.getIoLoopGroup())
            .channel(config.isEpoll() ? EpollSocketChannel.class : NioSocketChannel.class)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.getConnTimeout())
            .option(ChannelOption.TCP_NODELAY, config.isTcpNoDelay())
            .option(ChannelOption.SO_REUSEADDR, config.isReuseAddress())
            .option(ChannelOption.SO_KEEPALIVE, config.isKeepAlive())
            .option(ChannelOption.SO_LINGER, config.getSoLinger())
            .option(ChannelOption.SO_RCVBUF, config.getSocketBufferSize())
            .option(ChannelOption.SO_SNDBUF, config.getSocketBufferSize())
            .option(ChannelOption.ALLOCATOR, config.getAllocatorFactory().getByteBufAllocator())
            .option(ChannelOption.RCVBUF_ALLOCATOR, config.getAllocatorFactory().getRecvByteBufAllocator())
            .option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(128 * 1024, 1024 * 1024))
            .handler(new ChannelPipeline(config.getAllocatorFactory(), config.getHandlerSupplier()));

    return bootstrap;
  }

  @Override
  public void close() {
    poolMap.forEach((addr, pool) -> {
      pool.close();
    });
    evictPools.forEach(pool -> {
      pool.close();
    });

    idleExecutor.shutdown();
    timeoutExecutor.shutdown();
    eventExecutor.shutdown();
  }

  /**
   * Connection idle task.
   */
  private final class IdleTask implements Runnable {
    @Override
    public void run() {
      poolMap.forEach((addr, pool) -> {
        if (pool.shouldEvict()) {
          poolMap.remove(addr);
          // put evict queue
          evictPools.add(pool);
        } else {
          List<Connection> conns = pool.shouldHeartbeat();
          for (Connection conn : conns) {
            eventBus.publish(new ConnectEvent(conn, ConnectEvent.EventType.HEARTBEAT));
          }
        }
      });

      // real execution evict
      evictPools.forEach(pool -> {
        if (pool.evictIdle()) {
          evictPools.remove(pool);
        }
      });
    }
  }

  /**
   * Request timeout task.
   */
  private final class TimeoutTask implements Runnable {
    @Override
    public void run() {
      poolMap.forEach((addr, pool) -> {
        pool.evictTimeout();
      });
    }
  }
}
