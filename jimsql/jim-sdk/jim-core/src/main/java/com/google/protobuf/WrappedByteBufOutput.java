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
package com.google.protobuf;

import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * @version V1.0
 */
final class WrappedByteBufOutput extends ByteOutput {
  private ByteBuf buf;

  WrappedByteBufOutput() {
  }

  ByteBuf getBuf() {
    return buf;
  }

  @Override
  public void write(byte value) {
    throw new UnsupportedOperationException("unsupported method write byte");
  }

  @Override
  public void write(byte[] value, int offset, int length) {
    ByteBuf tmp = Unpooled.wrappedBuffer(value, offset, length);
    buf = buf == null ? tmp : Unpooled.wrappedBuffer(buf, tmp);
  }

  @Override
  public void writeLazy(byte[] value, int offset, int length) {
    this.write(value, offset, length);
  }

  @Override
  public void write(ByteBuffer value) {
    ByteBuf tmp = Unpooled.wrappedBuffer(value);
    buf = buf == null ? tmp : Unpooled.wrappedBuffer(buf, tmp);
  }

  @Override
  public void writeLazy(ByteBuffer value) {
    this.write(value);
  }
}
