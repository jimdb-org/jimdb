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

import io.jimdb.rpc.client.command.Command;
import io.jimdb.rpc.client.heartbeat.HeartbeatListener;

import org.junit.Assert;
import org.junit.Test;

/**
 * @version V1.0
 */
public class HeartbeatListenerTest extends BaseTest {

  @Test
  public void handleEventTest() {
    Connection conn = pool.getConn();
    TBaseCommand request = new TBaseCommand();
    HeartbeatListener listener = new HeartbeatListener(new THeartbeatCommand(request));
    listener.handleEvent(new ConnectEvent(conn, ConnectEvent.EventType.HEARTBEAT));
    listener.handleEvent(new ConnectEvent(conn, ConnectEvent.EventType.HEARTBEAT));
    Command response = new TBaseCommand();
    conn.response(request.getReqID(), response);
    sleep(1000);
    Assert.assertEquals(false, conn.isActive());
  }
}
