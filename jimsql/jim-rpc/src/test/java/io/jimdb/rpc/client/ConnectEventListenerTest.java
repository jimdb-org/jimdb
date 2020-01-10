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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.junit.Assert;
import org.junit.Test;

/**
 * @version 1.0
 */
public class ConnectEventListenerTest extends BaseTest {

  @Test
  public void handleEventTest() {
    ScheduledExecutorService retryExecutor = Executors.newSingleThreadScheduledExecutor();
    ConnectEventListener listener = new ConnectEventListener(retryExecutor);
    Connection conn = pool.getConn();
    conn.connect();
    Assert.assertEquals(true, conn.isActive());
    conn.getChannel().close();

    sleep(1000);
    Assert.assertEquals(false, conn.isActive());
    ConnectEvent connectEvent = new ConnectEvent(conn, ConnectEvent.EventType.CLOSE);
    listener.handleEvent(connectEvent);
    sleep(1000);
    Assert.assertEquals(true, conn.isActive());
  }
}
