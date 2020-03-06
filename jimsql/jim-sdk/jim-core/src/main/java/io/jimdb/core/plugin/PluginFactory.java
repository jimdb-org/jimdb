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
package io.jimdb.core.plugin;

import io.jimdb.core.config.JimConfig;
import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.exception.BaseException;
import io.jimdb.core.plugin.store.Engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Plug-in manager.
 *
 * @version V1.0
 */
@SuppressFBWarnings("EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS")
public final class PluginFactory {
  private static final Logger LOG = LoggerFactory.getLogger(PluginFactory.class);

  private static SQLEngine sqlEngine;
  private static SQLExecutor sqlExecutor;
  private static Engine engine;
  private static PrivilegeEngine privilegeEngine;
  private static MetaStore metaStore;
  private static RouterStore routerStore;

  public static void init(JimConfig c) throws BaseException {
    loadPlugins(c);
    if (LOG.isInfoEnabled()) {
      LOG.info("PluginManager loaded and initialized all plugins.");
    }
  }

  public static SQLEngine getSqlEngine() {
    return sqlEngine;
  }

  public static SQLExecutor getSqlExecutor() {
    return sqlExecutor;
  }

  public static Engine getStoreEngine() {
    return engine;
  }

  public static PrivilegeEngine getPrivilegeEngine() {
    return privilegeEngine;
  }

  public static MetaStore getMetaStore() {
    return metaStore;
  }

  public static RouterStore getRouterStore() {
    return routerStore;
  }

  public static void close() throws Exception {
    sqlEngine.close();
    sqlExecutor.close();
    engine.close();
    privilegeEngine.close();
    metaStore.close();
    routerStore.close();
  }

  /**
   * Load all plugins.
   */
  private static void loadPlugins(JimConfig c) throws BaseException {
    try {
      Class<?> clazz = Class.forName(c.getMetaStoreClass());
      metaStore = (MetaStore) clazz.newInstance();

      clazz = Class.forName(c.getRouterStoreClass());
      routerStore = (RouterStore) clazz.newInstance();

      clazz = Class.forName(c.getStoreEngineClass());
      engine = (Engine) clazz.newInstance();

      clazz = Class.forName(c.getSqlExecutorClass());
      sqlExecutor = (SQLExecutor) clazz.newInstance();

      clazz = Class.forName(c.getPrivilegeClass());
      privilegeEngine = (PrivilegeEngine) clazz.newInstance();

      clazz = Class.forName(c.getSqlEngineClass());
      sqlEngine = (SQLEngine) clazz.newInstance();

      metaStore.init(c);
      routerStore.init(c);
      engine.init(c);
      sqlExecutor.init(c);
      privilegeEngine.init(c);
      sqlEngine.init(c);
    } catch (Exception ex) {
      throw DBException.get(ErrorModule.SYSTEM, ErrorCode.ER_SYSTEM_PLUGIN_LOAD, ex);
    }
  }
}
