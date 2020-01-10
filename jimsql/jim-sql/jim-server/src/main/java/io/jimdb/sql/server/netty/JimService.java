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

import io.jimdb.core.Bootstraps;
import io.jimdb.core.Service;
import io.jimdb.core.config.JimConfig;
import io.jimdb.common.config.NettyServerConfig;
import io.jimdb.core.plugin.PluginFactory;
import io.jimdb.rpc.server.NettyServer;
import io.netty.channel.ChannelHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * @version V1.0
 */
public final class JimService extends Service {
  private static final Logger LOG = LoggerFactory.getLogger(JimService.class);

  private final JimConfig config;
  private final NettyServer server;

  public JimService(final JimConfig config) {
    Preconditions.checkArgument(config != null, "config can not be null");

    final NettyServerConfig serverConfig = config.getServerConfig();
    final ChannelHandler handler = new SessionHandler(serverConfig,
            PluginFactory.getSqlEngine(), PluginFactory.getStoreEngine());
    serverConfig.setHandlerSupplier(() -> {
      return new ChannelHandler[]{ new ProtocolDecoder(PluginFactory.getSqlEngine()), handler };
    });

    this.config = config;
    this.server = new NettyServer(serverConfig);
  }

  @Override
  protected void doStart() throws Exception {
    super.doStart();
    this.server.start();
  }

  @Override
  protected void afterStart() {
    if (LOG.isInfoEnabled()) {
      LOG.info("jim server is started");
    }
  }

  @Override
  protected void startError(Exception ex) {
    super.startError(ex);
    LOG.error("jim server start failed", ex);
  }

  @Override
  protected void doStop() {
    super.doStop();
    try {
      Bootstraps.stop(config);
      this.server.stop();
    } catch (Exception ex) {
      LOG.error("jim server stop error", ex);
    }
  }

  @Override
  protected void afterStop() {
    if (LOG.isInfoEnabled()) {
      LOG.info("jim server is stopped");
    }
  }
}
