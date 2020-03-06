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
package io.jimdb.sql.server.netty;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;

import io.jimdb.core.Session;
import io.jimdb.common.exception.BaseException;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.model.result.ResultType;
import io.jimdb.core.plugin.SQLEngine;
import io.jimdb.core.plugin.store.Engine;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.Channel;

/**
 * @className: SQLSession
 */
public final class NettySession extends Session {
  private final Channel channel;
  private final SQLEngine sqlEngine;
  private final AtomicBoolean channelClosed = new AtomicBoolean(false);

  public NettySession(final Channel channel, final SQLEngine sqlEngine, final Engine storeEngine) {
    super(sqlEngine, storeEngine);
    this.channel = channel;
    this.sqlEngine = sqlEngine;
  }

  @Override
  public void writeError(BaseException ex) {
    CompositeByteBuf out = channel.alloc().compositeBuffer();
    this.sqlEngine.writeError(this, out, ex);

    this.reset();
    this.channel.writeAndFlush(out);
  }

  @Override
  public void write(ExecResult rs, boolean isEof) {
    try {
      CompositeByteBuf out = channel.alloc().compositeBuffer();
      this.sqlEngine.writeResult(this, out, rs);

      if (isEof) {
        if (rs.getType() != ResultType.HANDSHAKE) {
          this.reset();
        }
        this.channel.writeAndFlush(out);
      } else {
        this.channel.write(out);
      }
    } finally {
      rs.release();
    }
  }

  @Override
  public String getRemoteAddress() {
    InetSocketAddress inetSocketAddress = (InetSocketAddress) this.channel.remoteAddress();
    String ip = inetSocketAddress.getAddress().getHostAddress();
    int port = inetSocketAddress.getPort();
    return ip + ":" + port;
  }

  @Override
  public void close() {
    if (channelClosed.compareAndSet(false, true)) {
      if (channel != null && channel.isActive()) {
        channel.close();
      }
    }

    super.close();
  }
}
