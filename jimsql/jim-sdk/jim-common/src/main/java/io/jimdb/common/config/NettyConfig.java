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
package io.jimdb.common.config;

import java.io.Closeable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.function.Supplier;

import io.jimdb.common.utils.buffer.BufAllocatorFactory;
import io.netty.channel.ChannelHandler;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings({ "EI_EXPOSE_REP", "EI_EXPOSE_REP2" })
public abstract class NettyConfig implements Closeable {
  // Send timeout(ms)
  protected int sendTimeout = 5000;
  // Channel idle timeout(ms)
  protected int maxIdle = 60 * 60 * 1000;
  // Frequency to update the statistics table (ms)
  protected int statsRefreshPeriod  = 30 * 1000;
  // Socket address reuse
  protected boolean reuseAddress = true;
  // When off, wait for the unsent packet (in seconds)
  // -1,0: disable, discard unsent packets;
  // >0, wait until the specified time, discard if not already sent
  protected int soLinger = -1;
  // Enable nagle algorithm, send immediately for true, otherwise get confirmation or buffer full send
  protected boolean tcpNoDelay = true;
  // Keep active connections, regular heartbeat packages
  protected boolean keepAlive = true;
  // Socket read timeout(毫秒)
  protected int soTimeout = 3000;
  // socket buffer size
  protected int socketBufferSize = 16 * 1024;
  // Use EPOLL, only support Linux mode
  protected boolean epoll = false;
  // Support for the minimum kernel version of EPOLL
  protected String epollOsVersion = "2.6.32";
  // Packet max size
  protected int frameMaxSize = 16 * 1024 * 1024 + 1024;
  // IO worker thread pool
  protected EventLoopGroup ioLoopGroup;
  // Work thread pool
  private ExecutorService workExecutor;
  // Channel handlers
  private Supplier<ChannelHandler[]> handlerSupplier;
  // Allocator Factory
  protected BufAllocatorFactory allocatorFactory;

  public NettyConfig() {
  }

  public void validate() {
    if (this.ioLoopGroup == null) {
      throw new IllegalArgumentException("ioLoopGroup must not be null");
    }
    if (this.allocatorFactory == null) {
      throw new IllegalArgumentException("allocatorFactory must not be null");
    }
    if (handlerSupplier == null) {
      throw new IllegalArgumentException("handlerSupplier must not be null");
    }
  }

  public int getMaxIdle() {
    return this.maxIdle;
  }

  public void setMaxIdle(int maxIdle) {
    if (maxIdle > 0) {
      this.maxIdle = maxIdle;
    }
  }

  public int getStatsRefreshPeriod() {
    return this.statsRefreshPeriod;
  }

  public void setStatsRefreshPeriod(int statsRefreshPeriod) {
    this.statsRefreshPeriod = statsRefreshPeriod;
  }

  public void setSoTimeout(int soTimeout) {
    if (soTimeout > 0) {
      this.soTimeout = soTimeout;
    }
  }

  public void setSocketBufferSize(int socketBufferSize) {
    if (socketBufferSize > 0) {
      this.socketBufferSize = socketBufferSize;
    }
  }

  public void setFrameMaxSize(int frameMaxSize) {
    if (frameMaxSize > 0) {
      this.frameMaxSize = frameMaxSize;
    }
  }

  public void setEpoll(boolean epoll) {
    this.epoll = epoll && supportEpoll();
  }

  public void setSendTimeout(int sendTimeout) {
    if (sendTimeout > 0) {
      this.sendTimeout = sendTimeout;
    }
  }

  public int getSendTimeout() {
    return sendTimeout;
  }

  public boolean isReuseAddress() {
    return reuseAddress;
  }

  public void setReuseAddress(boolean reuseAddress) {
    this.reuseAddress = reuseAddress;
  }

  public int getSoLinger() {
    return soLinger;
  }

  public void setSoLinger(int soLinger) {
    this.soLinger = soLinger;
  }

  public boolean isTcpNoDelay() {
    return tcpNoDelay;
  }

  public void setTcpNoDelay(boolean tcpNoDelay) {
    this.tcpNoDelay = tcpNoDelay;
  }

  public boolean isKeepAlive() {
    return keepAlive;
  }

  public void setKeepAlive(boolean keepAlive) {
    this.keepAlive = keepAlive;
  }

  public int getSoTimeout() {
    return soTimeout;
  }

  public int getSocketBufferSize() {
    return socketBufferSize;
  }

  public boolean isEpoll() {
    return epoll;
  }

  public String getEpollOsVersion() {
    return epollOsVersion;
  }

  public void setEpollOsVersion(String epollOsVersion) {
    this.epollOsVersion = epollOsVersion;
  }

  public int getFrameMaxSize() {
    return frameMaxSize;
  }

  public EventLoopGroup getIoLoopGroup() {
    return ioLoopGroup;
  }

  public void setIoLoopGroup(EventLoopGroup ioLoopGroup) {
    this.ioLoopGroup = ioLoopGroup;
  }

  public ExecutorService getWorkExecutor() {
    return workExecutor;
  }

  public void setWorkExecutor(ExecutorService workExecutor) {
    this.workExecutor = workExecutor;
  }

  public Supplier<ChannelHandler[]> getHandlerSupplier() {
    return handlerSupplier;
  }

  public void setHandlerSupplier(Supplier<ChannelHandler[]> handlerSupplier) {
    this.handlerSupplier = handlerSupplier;
  }

  public BufAllocatorFactory getAllocatorFactory() {
    return allocatorFactory;
  }

  public void setAllocatorFactory(BufAllocatorFactory allocatorFactory) {
    this.allocatorFactory = allocatorFactory;
  }

  protected EventLoopGroup createEventLoopGroup(int threads, ThreadFactory threadFactory) {
    if (this.isEpoll()) {
      return new EpollEventLoopGroup(threads, threadFactory);
    }
    return new NioEventLoopGroup(threads, threadFactory);
  }

  @SuppressFBWarnings("STT_STRING_PARSING_A_FIELD")
  protected boolean supportEpoll() {
    String osName = System.getProperty("os.name");
    String osVersion = System.getProperty("os.version");
    if (osName == null || osVersion == null) {
      return false;
    }

    if (osName.toLowerCase().startsWith("linux")) {
      if (epollOsVersion == null || epollOsVersion.isEmpty()) {
        return true;
      }

      String[] parts1 = osVersion.split("[\\.\\-]");
      String[] parts2 = epollOsVersion.split("[\\.\\-]");
      try {
        int v1 = 0;
        int v2 = 0;
        for (int i = 0; i < parts2.length; i++) {
          v2 = Integer.parseInt(parts2[i]);
          v1 = Integer.parseInt(parts1[i]);
          if (v1 > v2) {
            return true;
          }
          if (v1 < v2) {
            return false;
          }
        }
        return true;
      } catch (NumberFormatException ignored) {
      }
    }
    return false;
  }

  @Override
  public void close() {
    if (ioLoopGroup != null) {
      ioLoopGroup.shutdownGracefully();
    }
  }
}
