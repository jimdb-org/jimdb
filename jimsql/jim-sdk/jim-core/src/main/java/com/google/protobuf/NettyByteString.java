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
package com.google.protobuf;

import java.io.IOException;

import io.netty.buffer.ByteBuf;

/**
 * @version V1.0
 */
public final class NettyByteString {
  private NettyByteString() {
  }

  public static ByteString wrap(byte[] bytes) {
    return ByteString.wrap(bytes);
  }

  public static ByteString wrap(ByteBuf buf) {
    return ByteString.wrap(buf.nioBuffer());
  }

  public static ByteString wrap(ByteBuf buf, int offset, int length) {
    return ByteString.wrap(buf.nioBuffer(offset, length));
  }

  public static ByteBuf asByteBuf(ByteString value) {
    WrappedByteBufOutput output = new WrappedByteBufOutput();
    try {
      value.writeTo(output);
    } catch (IOException ex) {
    }
    return output.getBuf();
  }

  public static byte[] asByteArray(ByteString value) {
    WrappedArrayOutput output = new WrappedArrayOutput();
    try {
      value.writeTo(output);
    } catch (IOException ex) {
    }
    return output.getBuf();
  }

  public static void writeValue(ByteBuf buf, ByteString value) {
    try {
      value.writeTo(new NettyOutput(buf));
    } catch (IOException ex) {
    }
  }
}
