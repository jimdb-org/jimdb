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
package io.jimdb.engine.client;

import io.jimdb.rpc.client.heartbeat.HeartbeatCommand;

/**
 * @version V1.0
 */
public final class JimHeartbeat implements HeartbeatCommand<JimCommand> {
  private static final JimCommand PING_COMMAND = new JimCommand((short) 0, null);

  @Override
  public JimCommand build() {
    return PING_COMMAND;
  }

  @Override
  public boolean verify(final JimCommand ping, final JimCommand pong) {
    return pong.getFuncId() == 0 && pong.isResponse();
  }
}
