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
package io.jimdb.rpc;

import java.util.function.Supplier;

import io.jimdb.common.utils.buffer.BufAllocatorFactory;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings("EI_EXPOSE_REP2")
@ChannelHandler.Sharable
public final class ChannelPipeline extends ChannelInitializer {
  private final BufAllocatorFactory allocatorFactory;
  private final Supplier<ChannelHandler[]> handlerSupplier;

  public ChannelPipeline(final BufAllocatorFactory allocatorFactory, final Supplier<ChannelHandler[]> handlerSupplier) {
    this.allocatorFactory = allocatorFactory;
    this.handlerSupplier = handlerSupplier;
  }

  @Override
  protected void initChannel(final Channel channel) {
    channel.config()
            .setAllocator(allocatorFactory.getBufAllocator())
            .setRecvByteBufAllocator(allocatorFactory.getRecvBufAllocator());

    channel.pipeline().addLast(handlerSupplier.get());
  }
}
