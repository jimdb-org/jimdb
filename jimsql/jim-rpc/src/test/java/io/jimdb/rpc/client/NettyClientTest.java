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

import io.jimdb.rpc.client.command.Command;
import io.jimdb.rpc.client.command.CommandCallback;
import io.netty.handler.codec.LineBasedFrameDecoder;

import org.junit.Assert;
import org.junit.Test;

/**
 * @version V1.0
 */
public class NettyClientTest extends BaseTest {

  @Test
  public void sendTest() {
    int len = 1000;
    NettyClient nettyClient = new NettyClient(getNettyClientConfig(HEARTBEATINTERVAL * 30), TBaseCommand::new, () -> {
      return new LineBasedFrameDecoder(len);
    }, new THeartbeatCommand());

    for (int i = 0; i < 10; i++) {
      TBaseCommand request = new TBaseCommand((short) 1, (long) i, i + "---i am ct. hello server.\n");
      nettyClient.send(ADDRESS, request, new TCallback());
    }
    sleep(5000);
  }

  /**
   * mock callback
   */
  class TCallback extends CommandCallback {

    @Override
    protected boolean onSuccess0(Command request, Command response) {
      Assert.assertEquals(request.getReqID(), response.getReqID());

      TBaseCommand c = (TBaseCommand) response;
      System.out.println("client get server response :     " + c.getBody() + ",    reqid:" + c.getReqID() + ",    "
              + "funcid:" + c.getFuncID());
      return true;
    }

    @Override
    protected boolean onFailed0(Command request, Throwable cause) {
      return false;
    }
  }
}
