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

package io.jimdb.sql.server;

import io.jimdb.core.Bootstraps;
import io.jimdb.core.config.JimConfig;
import io.jimdb.sql.server.netty.JimService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @version V1.0
 */
public final class JimBootstrap {
  private static final Logger LOG = LoggerFactory.getLogger(JimBootstrap.class);

  public static void main(String[] args) {
    try {
      JimConfig config = Bootstraps.init("jim.properties");
      JimService service = createServer(config);
      config = null;
      start(service);
    } catch (Exception e) {
      LOG.error("Start server error.", e);
      System.exit(1);
    }
  }

  public static JimService createServer(final JimConfig config) {
    return new JimService(config);
  }

  public static void start(JimService jimService) {
    if (!jimService.isStarted()) {
      if (LOG.isInfoEnabled()) {
        LOG.info("jimService will start...");
      }
      jimService.start();
    }

    Thread shutHook = new Thread() {
      @Override
      public void run() {
        if (LOG.isInfoEnabled()) {
          LOG.info("system is being shutdown.");
        }
        if (jimService.isStarted()) {
          jimService.stop();
        }
      }
    };
    shutHook.setDaemon(true);
    Runtime.getRuntime().addShutdownHook(shutHook);
  }
}
