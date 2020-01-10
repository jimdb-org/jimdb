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

import java.util.List;

import io.jimdb.core.Session;
import io.jimdb.core.plugin.SQLEngine;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import com.google.common.base.Preconditions;

/**
 * @version V1.0
 */
public final class ProtocolDecoder extends ByteToMessageDecoder {
  private final SQLEngine sqlEngine;

  public ProtocolDecoder(final SQLEngine sqlEngine) {
    Preconditions.checkArgument(sqlEngine != null, "sqlEngine must not be null");
    this.sqlEngine = sqlEngine;
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
    final Session session = ctx.channel().attr(SessionHandler.NETTY_CHANNEL_SESSION).get();

    while (true) {
      final Object decoded = sqlEngine.frameDecode(session, in);
      if (decoded == null) {
        return;
      }

      out.add(decoded);
    }
  }
}
