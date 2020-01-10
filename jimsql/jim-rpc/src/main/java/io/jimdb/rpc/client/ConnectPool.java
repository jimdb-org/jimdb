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

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;

import io.jimdb.common.config.NettyClientConfig;
import io.jimdb.common.utils.event.EventBus;
import io.jimdb.common.utils.lang.MathUtil;
import io.jimdb.common.utils.os.SystemClock;
import io.netty.bootstrap.Bootstrap;

/**
 * @version V1.0
 */
final class ConnectPool implements Closeable {
  @sun.misc.Contended
  private volatile long lastTime;

  final String address;
  private final int size;
  private final int mask;
  private final int maxIdle;
  private final Connection[] conns;
  private final LongAdder seq = new LongAdder();

  ConnectPool(final NettyClientConfig config, final String address, final Bootstrap bootstrap, final EventBus eventBus) {
    if (address == null || address.isEmpty()) {
      throw new IllegalArgumentException("address must not be empty");
    }
    String[] parts = address.split("[._:]");
    if (parts.length < 1) {
      throw new IllegalArgumentException("address is invalid");
    }

    int port = Integer.parseInt(parts[parts.length - 1]);
    if (port < 0) {
      throw new IllegalArgumentException("address port is invalid");
    }

    if (config.getConnPoolSize() <= 1) {
      this.size = 1;
    } else {
      this.size = MathUtil.powerTwo(config.getConnPoolSize());
    }
    this.mask = this.size - 1;
    this.address = address;
    this.maxIdle = config.getMaxIdle();
    this.lastTime = SystemClock.currentTimeMillis();
    this.conns = new Connection[this.size];

    for (int i = 0; i < this.size; i++) {
      this.conns[i] = new Connection(config, address, i + 1, bootstrap, eventBus);
    }
  }

  Connection getConn() {
    this.lastTime = SystemClock.currentTimeMillis();

    if (this.size == 1) {
      return conns[0];
    }

    int num = 0;
    Connection conn = null;
    while (num < size) {
      seq.add(1);
      long idx = seq.sum() & mask;
      conn = conns[(int) idx];
      if (conn.isActive()) {
        break;
      }
      num++;
    }

    return conn;
  }

  List<Connection> shouldHeartbeat() {
    final List<Connection> heartbeats = new ArrayList<>();

    for (Connection conn : conns) {
      if (conn.shouldHeartbeat()) {
        heartbeats.add(conn);
      }
    }
    return heartbeats;
  }

  boolean shouldEvict() {
    return SystemClock.currentTimeMillis() - lastTime >= maxIdle;
  }

  boolean evictIdle() {
    for (Connection conn : conns) {
      if (!conn.evictIdle()) {
        return false;
      }
    }

    this.close();
    return true;
  }

  void evictTimeout() {
    for (Connection conn : conns) {
      conn.evictTimeout();
    }
  }

  @Override
  public void close() {
    for (Connection conn : conns) {
      conn.close();
    }
  }
}
