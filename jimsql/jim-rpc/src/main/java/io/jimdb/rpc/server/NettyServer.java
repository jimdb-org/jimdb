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
package io.jimdb.rpc.server;

import io.jimdb.common.config.NettyServerConfig;
import io.jimdb.rpc.ChannelPipeline;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @version V1.0
 */
public final class NettyServer {
  private static final Logger LOG = LoggerFactory.getLogger(NettyServer.class);

  private final NettyServerConfig config;

  public NettyServer(final NettyServerConfig config) {
    config.validate();

    this.config = config;
  }

  public void start() throws Exception {
    ServerBootstrap bootStrap = this.buildBootstrap();
    bootStrap.bind().sync();

    if (LOG.isInfoEnabled()) {
      LOG.info("NettyServer is started and listen on {}:{}", config.getHost(), config.getPort());
    }
  }

  public void stop() {
  }

  private ServerBootstrap buildBootstrap() {
    ServerBootstrap bootStrap = new ServerBootstrap();
    bootStrap.group(config.getBossLoopGroup(), config.getIoLoopGroup())
            .channel(this.config.isEpoll() ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
            .option(ChannelOption.TCP_NODELAY, config.isTcpNoDelay())
            .option(ChannelOption.SO_REUSEADDR, config.isReuseAddress())
            .option(ChannelOption.SO_KEEPALIVE, config.isKeepAlive())
            .option(ChannelOption.SO_LINGER, config.getSoLinger())
            .option(ChannelOption.SO_RCVBUF, config.getSocketBufferSize())
            .option(ChannelOption.SO_SNDBUF, config.getSocketBufferSize())
            .option(ChannelOption.SO_BACKLOG, config.getBacklog())
            .option(ChannelOption.ALLOCATOR, config.getAllocatorFactory().getByteBufAllocator())
            .option(ChannelOption.RCVBUF_ALLOCATOR, config.getAllocatorFactory().getRecvByteBufAllocator())
            .option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(128 * 1024, 1024 * 1024));

    if (StringUtils.isBlank(config.getHost())) {
      bootStrap.localAddress(config.getPort());
    } else {
      bootStrap.localAddress(config.getHost(), config.getPort());
    }
    bootStrap.childHandler(new ChannelPipeline(config.getAllocatorFactory(), config.getHandlerSupplier()));
    return bootStrap;
  }
}
