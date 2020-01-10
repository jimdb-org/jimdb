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
package io.jimdb.common.config;

import io.jimdb.common.utils.buffer.BufAllocatorFactory;
import io.jimdb.common.utils.buffer.DefaultBufAllocatorFactory;
import io.jimdb.common.utils.lang.NamedThreadFactory;
import io.netty.channel.EventLoopGroup;

import org.apache.commons.lang3.StringUtils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Server Config.
 */
@SuppressFBWarnings("EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS")
public final class NettyServerConfig extends NettyConfig {
  private static final String NETTY_SERVER_HOST = "netty.server.host";

  private static final String NETTY_SERVER_PORT = "netty.server.port";

  private static final String NETTY_SERVER_BACKLOG = "netty.server.backlog";

  private static final String NETTY_SERVER_SENDTIMEOUT = "netty.server.sendTimeout";

  private static final String NETTY_SERVER_BOSSTHREADS = "netty.server.bossThreads";

  private static final String NETTY_SERVER_IOTHREADS = "netty.server.ioThreads";

  private static final String NETTY_SERVER_MAXIDLE = "netty.server.maxIdle";

  private static final String NETTY_SERVER_REUSEADDRESS = "netty.server.reuseAddress";

  private static final String NETTY_SERVER_SOLINGER = "netty.server.soLinger";

  private static final String NETTY_SERVER_TCPNODELAY = "netty.server.tcpNoDelay";

  private static final String NETTY_SERVER_KEEPALIVE = "netty.server.keepAlive";

  private static final String NETTY_SERVER_SOTIMEOUT = "netty.server.soTimeout";

  private static final String NETTY_SERVER_SOCKETBUFFERSIZE = "netty.server.socketBufferSize";

  private static final String NETTY_SERVER_EPOLL = "netty.server.epoll";

  private static final String NETTY_SERVER_FRAMEMAXSIZE = "netty.server.frameMaxSize";

  private static final String NETTY_SERVER_ALLOCATORFACTORY = "netty.server.allocatorFactory";

  // IP Address
  private String host = "0.0.0.0";
  // Port
  private int port = 0;
  // The maximum queue length of connection requests,
  // the connection is rejected if the connection indication is received when the queue is full.
  private int backlog = 65536;
  // Selector thread pool
  private EventLoopGroup bossLoopGroup;

  public NettyServerConfig(final BaseConfig props) {
    this.setHost(props.getString(NETTY_SERVER_HOST, "0.0.0.0"));
    this.setPort(props.getInt(NETTY_SERVER_PORT, 0));
    this.setBacklog(props.getInt(NETTY_SERVER_BACKLOG, 65536));
    this.setSendTimeout(props.getInt(NETTY_SERVER_SENDTIMEOUT, 5000));
    this.setMaxIdle(props.getInt(NETTY_SERVER_MAXIDLE, 30 * 60 * 1000));
    this.setReuseAddress(props.getBoolean(NETTY_SERVER_REUSEADDRESS, true));
    this.setSoLinger(props.getInt(NETTY_SERVER_SOLINGER, -1));
    this.setTcpNoDelay(props.getBoolean(NETTY_SERVER_TCPNODELAY, true));
    this.setKeepAlive(props.getBoolean(NETTY_SERVER_KEEPALIVE, true));
    this.setSoTimeout(props.getInt(NETTY_SERVER_SOTIMEOUT, 3000));
    this.setSocketBufferSize(props.getInt(NETTY_SERVER_SOCKETBUFFERSIZE, 16 * 1024));
    this.setFrameMaxSize(props.getInt(NETTY_SERVER_FRAMEMAXSIZE, 16 * 1024 * 1024 + 1024));
    this.setEpoll(props.getBoolean(NETTY_SERVER_EPOLL, true));

    int bossThread = props.getInt(NETTY_SERVER_BOSSTHREADS, 1);
    bossThread = bossThread <= 0 ? 1 : bossThread;
    this.setBossLoopGroup(createEventLoopGroup(bossThread, new NamedThreadFactory("Server-BossLoopGroup", false)));

    int ioThread = props.getInt(NETTY_SERVER_IOTHREADS, Runtime.getRuntime().availableProcessors());
    ioThread = ioThread <= 0 ? Runtime.getRuntime().availableProcessors() : ioThread;
    this.setIoLoopGroup(createEventLoopGroup(ioThread, new NamedThreadFactory("Server-IOLoopGroup", false)));

    String allocatorFactory = props.getString(NETTY_SERVER_ALLOCATORFACTORY, null);
    if (StringUtils.isNotBlank(allocatorFactory)) {
      try {
        Class allocatorClass = Class.forName(allocatorFactory);
        this.setAllocatorFactory((BufAllocatorFactory) allocatorClass.newInstance());
      } catch (Exception ex) {
        throw new IllegalArgumentException("load allocatorFactory error", ex);
      }
    } else {
      this.setAllocatorFactory(new DefaultBufAllocatorFactory());
    }
  }

  @Override
  public void validate() {
    super.validate();

    if (this.bossLoopGroup == null) {
      throw new IllegalArgumentException("bossLoopGroup can not be null");
    }
    if (this.port <= 0) {
      throw new IllegalArgumentException("port must be greater than 0");
    }
  }

  public void setPort(int port) {
    if (port > 0 && port <= 65535) {
      this.port = port;
    }
  }

  public void setBacklog(int backlog) {
    if (backlog > 0) {
      this.backlog = backlog;
    }
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public int getPort() {
    return port;
  }

  public int getBacklog() {
    return backlog;
  }

  public EventLoopGroup getBossLoopGroup() {
    return bossLoopGroup;
  }

  public void setBossLoopGroup(EventLoopGroup bossLoopGroup) {
    this.bossLoopGroup = bossLoopGroup;
  }

  @Override
  public void close() {
    super.close();
    if (bossLoopGroup != null) {
      bossLoopGroup.shutdownGracefully();
    }
  }
}
