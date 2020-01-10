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

import io.netty.buffer.ByteBuf;

/**
 * @version V1.0
 */
public class TCodecUtil {
  public static void encode(ByteBuf buf, TBaseCommand t) {
    buf.writeShort(t.getFuncID());
    buf.writeLong(t.getReqID());
    if (t.getFuncID() > 0) {
      buf.writeBytes(t.getBody().getBytes());
    }
  }

  public static void decode(ByteBuf buf, TBaseCommand command) {
    command.setFuncID(buf.readShort());
    command.setReqID(buf.readLong());
    if (command.getFuncID() > 0) {
      byte[] bytes = new byte[buf.readableBytes()];
      buf.readBytes(bytes);
      command.setBody(new String(bytes));
    }
  }
}
