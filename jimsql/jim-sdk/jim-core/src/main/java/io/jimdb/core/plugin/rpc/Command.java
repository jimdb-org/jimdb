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
package io.jimdb.core.plugin.rpc;

import java.io.Closeable;

import io.jimdb.common.exception.CodecException;
import io.netty.buffer.ByteBuf;

/**
 * @version V1.0
 */
public interface Command extends Closeable {
  Long getReqID();

  void setReqID(Long reqID);

  int getTimeout();

  CommandType getType();

  ByteBuf encode() throws CodecException;

  void decode(ByteBuf buf) throws CodecException;

  @Override
  default void close() {
  }
}
