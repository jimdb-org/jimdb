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
package io.jimdb.rpc.client;

import java.util.Properties;

import io.jimdb.common.config.BaseConfig;
import io.jimdb.common.config.NettyServerConfig;
import io.jimdb.rpc.server.NettyServer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.LineBasedFrameDecoder;

/**
 * @version V1.0
 */
public class TBaseServer {

  public static void startNettyServer(String ip, int port) {
    NettyServerConfig serverConfig = getNettyServerConfig(ip, port);
    NettyServer server = new NettyServer(serverConfig);
    try {
      server.start();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static NettyServerConfig getNettyServerConfig(String ip, int port) {
    BaseConfig baseConfig = new BaseConfig(new Properties()) {
    };
    NettyServerConfig serverConfig = new NettyServerConfig(baseConfig);
    serverConfig.setHost(ip);
    serverConfig.setPort(port);
    serverConfig.setHandlerSupplier(() -> {
      return new ChannelHandler[]{ new LineBasedFrameDecoder(1000), new ServerHandler() };
    });
    return serverConfig;
  }

  /**
   * mock server handler
   */
  static class ServerHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      TBaseCommand t = new TBaseCommand();
      TCodecUtil.decode((ByteBuf) msg, t);

      if (t.getFuncID() > 0) {

        System.out.println("SERVER get client request:     " + t.getBody() + ",    reqid:" + t.getReqID() + ",    "
                + "funcid:" + t.getFuncID());

        TBaseCommand response = new TBaseCommand(t.getFuncID(), t.getReqID(), "hello client boy...\n");
        ByteBuf buf = ctx.channel().alloc().buffer(response.getBody().getBytes().length);
        response.encode(buf);
        ctx.channel().writeAndFlush(buf);
      } else {
        System.out.println("SERVER get client    reqid:" + t.getReqID() + ",    funcid:" + t.getFuncID());
      }
    }
  }
}
