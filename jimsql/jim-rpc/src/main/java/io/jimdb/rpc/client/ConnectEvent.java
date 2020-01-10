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

import io.jimdb.common.utils.event.Event;

/**
 * @version V1.0
 */
public final class ConnectEvent implements Event {
  private final Connection conn;
  private final EventType type;

  public ConnectEvent(final Connection conn, final EventType type) {
    this.conn = conn;
    this.type = type;
  }

  @Override
  public String getName() {
    return "ConnectEvent";
  }

  @Override
  public int getType() {
    return type.value;
  }

  public EventType getEventType() {
    return type;
  }

  public Connection getConn() {
    return conn;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(32);
    sb.append(this.getName())
            .append('-')
            .append(type.name())
            .append('(')
            .append(type.value)
            .append(')');
    return sb.toString();
  }

  /**
   * ConnectEvent type.
   */
  public enum EventType {
    CONNECT(11), CLOSE(12), EXCEPTION(14), RECONNECT(15), HEARTBEAT(16);

    private int value;

    EventType(int value) {
      this.value = value;
    }

    public int getValue() {
      return value;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder(20);
      sb.append(this.name()).append('-').append(this.value);
      return sb.toString();
    }
  }
}
