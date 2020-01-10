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
package io.jimdb.common.config;

import io.jimdb.common.utils.buffer.BufAllocatorFactory;
import io.jimdb.common.utils.buffer.DefaultBufAllocatorFactory;
import io.jimdb.common.utils.lang.NamedThreadFactory;

import org.apache.commons.lang3.StringUtils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Client Config.
 */
@SuppressFBWarnings({ "EI_EXPOSE_REP", "EI_EXPOSE_REP2", "EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS" })
public final class NettyClientConfig extends NettyConfig {
  private static final String NETTY_CLIENT_POOLSIZE = "netty.client.poolSize";

  private static final String NETTY_CLIENT_IOTHREADS = "netty.client.ioThreads";

  private static final String NETTY_CLIENT_CONNTIMEOUT = "netty.client.connTimeout";

  private static final String NETTY_CLIENT_SENDTIMEOUT = "netty.client.sendTimeout";

  private static final String NETTY_CLIENT_MAXIDLE = "netty.client.maxIdle";

  private static final String NETTY_CLIENT_HEARTBEAT = "netty.client.heartbeat";

  private static final String NETTY_CLIENT_SOLINGER = "netty.client.soLinger";

  private static final String NETTY_CLIENT_TCPNODELAY = "netty.client.tcpNoDelay";

  private static final String NETTY_CLIENT_KEEPALIVE = "netty.client.keepAlive";

  private static final String NETTY_CLIENT_SOTIMEOUT = "netty.client.soTimeout";

  private static final String NETTY_CLIENT_SOCKETBUFFERSIZE = "netty.client.socketBufferSize";

  private static final String NETTY_CLIENT_EPOLL = "netty.client.epoll";

  private static final String NETTY_CLIENT_FRAMEMAXSIZE = "netty.client.frameMaxSize";

  private static final String NETTY_CLIENT_ALLOCATORFACTORY = "netty.client.allocatorFactory";

  // Connect pool size
  private int connPoolSize = 1;
  // Connect timeout(ms)
  private int connTimeout = 3000;
  // Heartbeat interval(ms)
  private int heartbeatInterval = 30 * 1000;

  public NettyClientConfig(final BaseConfig props) {
    this.setConnPoolSize(props.getInt(NETTY_CLIENT_POOLSIZE, 1));
    this.setConnTimeout(props.getInt(NETTY_CLIENT_CONNTIMEOUT, 3000));
    this.setSendTimeout(props.getInt(NETTY_CLIENT_SENDTIMEOUT, 5000));
    this.setMaxIdle(props.getInt(NETTY_CLIENT_MAXIDLE, 60 * 60 * 1000));
    this.setHeartbeatInterval(props.getInt(NETTY_CLIENT_HEARTBEAT, 30 * 1000));
    this.setSoLinger(props.getInt(NETTY_CLIENT_SOLINGER, -1));
    this.setTcpNoDelay(props.getBoolean(NETTY_CLIENT_TCPNODELAY, true));
    this.setKeepAlive(props.getBoolean(NETTY_CLIENT_KEEPALIVE, true));
    this.setSoTimeout(props.getInt(NETTY_CLIENT_SOTIMEOUT, 3000));
    this.setSocketBufferSize(props.getInt(NETTY_CLIENT_SOCKETBUFFERSIZE, 16 * 1024));
    this.setFrameMaxSize(props.getInt(NETTY_CLIENT_FRAMEMAXSIZE, 16 * 1024 * 1024 + 1024));
    this.setEpoll(props.getBoolean(NETTY_CLIENT_EPOLL, true));

    int ioThread = props.getInt(NETTY_CLIENT_IOTHREADS, -1);
    if (ioThread >= 0) {
      ioThread = ioThread == 0 ? Runtime.getRuntime().availableProcessors() : ioThread;
      this.setIoLoopGroup(createEventLoopGroup(ioThread, new NamedThreadFactory("Client-IOLoopGroup", false)));
    }

    String allocatorFactory = props.getString(NETTY_CLIENT_ALLOCATORFACTORY, null);
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

  public int getConnPoolSize() {
    return connPoolSize;
  }

  public void setConnPoolSize(int connPoolSize) {
    if (connPoolSize > 0) {
      this.connPoolSize = connPoolSize;
    }
  }

  public int getConnTimeout() {
    return connTimeout;
  }

  public void setConnTimeout(int connTimeout) {
    if (connTimeout > 0) {
      this.connTimeout = connTimeout;
    }
  }

  public int getHeartbeatInterval() {
    return heartbeatInterval;
  }

  public void setHeartbeatInterval(int heartbeatInterval) {
    if (heartbeatInterval > 0) {
      this.heartbeatInterval = heartbeatInterval;
    }
  }
}
