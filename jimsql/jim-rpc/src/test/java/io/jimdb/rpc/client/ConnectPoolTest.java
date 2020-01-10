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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @version 1.0
 */
public class ConnectPoolTest extends BaseTest {
  private final Logger logger = LoggerFactory.getLogger(ConnectPool.class);

  @Test
  public void doConnectAndCloseTest() {
    int counter = 0;
    Set<Connection> connectionSet = new HashSet<>();
    while (counter++ < POOLSIZE) {
      Connection connection = pool.getConn();
      connection.connect();
      Assert.assertEquals(true, connection.isActive());
      connectionSet.add(connection);
    }
    Assert.assertEquals(POOLSIZE, connectionSet.size());
    sleep(1000);
    pool.close();
    sleep(1000);
    for (Connection conn : connectionSet) {
      Assert.assertEquals(false, conn.isActive());
    }
  }

  @Test
  public void checkHeartbeatSizeTest() {
    try {
      Thread.sleep(HEARTBEATINTERVAL + 100);
    } catch (InterruptedException e) {
      logger.error(e.getMessage(), e);
    }

    List<Connection> list = pool.shouldHeartbeat();
    Assert.assertEquals(POOLSIZE, list.size());
  }

  @Test
  public void evictIdleTest() {
    Assert.assertEquals(false, pool.shouldEvict());
    sleep(MAXIDLE + 1);
    Assert.assertEquals(true, pool.shouldEvict());
    Assert.assertEquals(true, pool.evictIdle());
  }
}
