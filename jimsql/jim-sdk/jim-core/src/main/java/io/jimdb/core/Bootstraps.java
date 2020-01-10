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
package io.jimdb.core;

import java.util.Properties;

import io.jimdb.core.config.JimConfig;
import io.jimdb.core.config.SystemProperties;
import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.exception.JimException;
import io.jimdb.core.plugin.PluginFactory;
import io.jimdb.core.plugin.SQLEngine;
import io.jimdb.common.utils.lang.IOUtil;
import io.jimdb.common.utils.lang.JimUncaughtExceptionHandler;
import io.netty.util.ResourceLeakDetector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.core.publisher.Hooks;
import reactor.core.scheduler.Schedulers;

/**
 * @version V1.0
 */
@SuppressFBWarnings("OCP_OVERLY_CONCRETE_PARAMETER")
public final class Bootstraps {
  private static final Logger LOG = LoggerFactory.getLogger(Bootstraps.class);

  private Bootstraps() {
  }

  public static JimConfig init(final String confFile) {
    Properties properties = IOUtil.loadResource(confFile);
    JimConfig config = new JimConfig(properties);
    init(config);
    return config;
  }

  public static void init(final JimConfig config) {
    // load plugin
    PluginFactory.init(config);
    final SQLEngine sqlEngine = PluginFactory.getSqlEngine();
    if (sqlEngine != null) {
      sqlEngine.setSQLExecutor(PluginFactory.getSqlExecutor());
    }

    // init global variable
    Thread.setDefaultUncaughtExceptionHandler(JimUncaughtExceptionHandler.getInstance());
    ResourceLeakDetector.setLevel(SystemProperties.getLeakLevel());
    // init reactor
    if (SystemProperties.getReactorDebug()) {
      Hooks.onOperatorDebug();
    }
    Hooks.onNextDropped(o -> LOG.error("Occur next event dropped : {}.", o));
    Hooks.onErrorDropped(e -> LOG.error("Occur error event dropped.", e));
    Hooks.onOperatorError((e, o) -> {
      LOG.error(String.format("OperatorError : %s", o), e);
      if (e instanceof JimException) {
        return e;
      }

      return DBException.get(ErrorModule.EXECUTOR, ErrorCode.ER_SYSTEM_INTERNAL, e);
    });
    if (config.getSchedulerFactory() != null) {
      Schedulers.setFactory(config.getSchedulerFactory());
    }
  }

  public static void stop(final JimConfig config) throws Exception {
    PluginFactory.close();
    config.close();
  }
}
