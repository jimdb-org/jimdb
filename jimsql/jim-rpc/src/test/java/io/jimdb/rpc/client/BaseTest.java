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

import java.util.Properties;

import io.jimdb.common.config.BaseConfig;
import io.jimdb.common.config.NettyClientConfig;
import io.jimdb.rpc.ChannelPipeline;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelOption;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @version 1.0
 */
public class BaseTest {
  private final Logger logger = LoggerFactory.getLogger(BaseTest.class);
  protected static final int POOLSIZE = 4;
  protected static final int HEARTBEATINTERVAL = 1000;
  protected static final int MAXIDLE = 5 * 1000;

  protected static ConnectPool pool;
  static final String IP = "127.0.0.1";
  static final int POST = 6000;
  static final String ADDRESS = IP + ":" + POST;

  @BeforeClass
  public static void init() {
    startServer(IP, POST);
  }

  private static void startServer(String ip, int port) {
    TBaseServer.startNettyServer(ip, port);
  }

  @Before
  public void initPool() {
    NettyClientConfig clientConfig = getNettyClientConfig(HEARTBEATINTERVAL);
    pool = new ConnectPool(clientConfig, ADDRESS, buildBootstrap(clientConfig), null);
  }


  protected static NettyClientConfig getNettyClientConfig(int hb) {
    Properties prop = new Properties();
    prop.setProperty("netty.client.ioThreads", "1");
    prop.setProperty("netty.client.poolSize", String.valueOf(POOLSIZE));
    BaseConfig baseConfig = new BaseConfig(prop) {
    };
    NettyClientConfig config = new NettyClientConfig(baseConfig);
    config.setHandlerSupplier(() -> {
      return new ChannelHandler[]{};
    });
    //modify heartbeat interval
    config.setHeartbeatInterval(hb);
    config.setMaxIdle(MAXIDLE);
    return config;
  }

  private static Bootstrap buildBootstrap(NettyClientConfig config) {
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
            .handler(new ChannelPipeline(config.getAllocatorFactory(), config.getHandlerSupplier()));

    return bootstrap;
  }

  protected void sleep(long interval) {
    try {
      Thread.sleep(interval);
    } catch (InterruptedException e) {
      logger.error(e.getMessage(), e);
    }
  }

}
