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
package io.jimdb.sql.ddl;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.exception.JimException;
import io.jimdb.core.model.meta.MetaData;
import io.jimdb.pb.Metapb.CatalogInfo;
import io.jimdb.pb.Metapb.TableInfo;
import io.jimdb.core.plugin.MetaStore;
import io.jimdb.common.utils.lang.NamedThreadFactory;
import io.jimdb.common.utils.os.SystemClock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Responsible for maintaining and synchronizing metadata.
 *
 * @version V1.0
 */
@SuppressFBWarnings("EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS")
final class DDLSyncer implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(DDLSyncer.class);

  private final long delay;
  private final long timeout;
  private final MetaStore metaStore;
  private final ScheduledExecutorService syncExecutor;
  private final AtomicBoolean loading = new AtomicBoolean(false);

  private volatile long updateTime = 0;
  private volatile long registerVersion = 0;

  DDLSyncer(MetaStore metaStore, long delay, long timeout) {
    this.delay = delay;
    this.timeout = 600000;
    this.metaStore = metaStore;
    this.syncExecutor = new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("DDL-Syncer-Executor", true));
    MetaData.Holder.setMetaData(new MetaData(0));
  }

  void start() {
    int i = 0;
    while (updateTime == 0) {
      try {
        this.loadMeta();
      } catch (JimException ex) {
        if (i++ > 10) {
          throw ex;
        }

        try {
          Thread.sleep(1000);
        } catch (Exception e) {
        }
      }
    }

    registerVersion = MetaData.Holder.getMetaData().getVersion();
    metaStore.register(registerVersion);
    metaStore.watch(MetaStore.WatchType.SCHEMAVERSION, l -> syncExecutor.execute(() -> syncMeta()));
    syncExecutor.scheduleWithFixedDelay(() -> syncMeta(), delay, delay, TimeUnit.MILLISECONDS);
  }

  @Override
  public void close() {
    if (syncExecutor != null) {
      syncExecutor.shutdown();
    }
    metaStore.unRegister();
  }

  void waitMetaSynced(final long expectVer) {
    try {
      long start = SystemClock.currentTimeMillis();
      while (true) {
        if (this.checkMetaSynced(expectVer)) {
          return;
        }

        if (SystemClock.currentTimeMillis() - start > 2 * timeout) {
          throw DBException.get(ErrorModule.DDL, ErrorCode.ER_INTERNAL_ERROR, false, String.format("wait all sqlnode meta sync timeout(%d)", 2 * timeout));
        }
        Thread.sleep(100);
      }
    } catch (JimException ex) {
      throw ex;
    } catch (Exception ex) {
      throw DBException.get(ErrorModule.DDL, ErrorCode.ER_INTERNAL_ERROR, ex);
    }
  }

  private boolean checkMetaSynced(final long expectVer) {
    Map<String, Long> versions = metaStore.getRegisters();
    if (versions == null || versions.isEmpty()) {
      return false;
    }

    for (Map.Entry<String, Long> kv : versions.entrySet()) {
      if (kv.getValue().longValue() < expectVer) {
        LOG.warn("Jimsql[{}] cant sync expect meta version, [self, expect]=[{},{}]", kv.getKey(), kv.getValue(), expectVer);
        return false;
      }
    }

    return true;
  }

  private void syncMeta() {
    try {
      this.loadMeta();
    } catch (Exception ex) {
      LOG.error("DDLSyncer load meta from etcd error", ex);
    }
  }

  private void loadMeta() {
    if (!loading.compareAndSet(false, true)) {
      return;
    }

    if (updateTime != 0 && (SystemClock.currentTimeMillis() - updateTime) > timeout) {
      Thread shutHook = new Thread() {
        @Override
        public void run() {
          LOG.error("syncer load ddl meta timeout({}) and system will shutdown", timeout);
          Runtime.getRuntime().exit(1);
        }
      };
      shutHook.setDaemon(true);
      shutHook.start();
      return;
    }

    try {
      long curVersion = MetaData.Holder.getMetaData().getVersion();
      long syncVersion = metaStore.addAndGetVersion(0);
      if (curVersion != syncVersion) {
        Map<CatalogInfo, List<TableInfo>> metas = metaStore.getCatalogAndTables();
        MetaData metaData = new MetaData(syncVersion);
        if (metas != null && metas.size() > 0) {
          metas.forEach((catalogInfo, tableInfos) -> metaData.addCatalog(catalogInfo, tableInfos));
        }
        MetaData.Holder.setMetaData(metaData);
      }

      if (registerVersion != syncVersion) {
        metaStore.register(syncVersion);
        registerVersion = syncVersion;
      }
      updateTime = SystemClock.currentTimeMillis();
    } finally {
      loading.compareAndSet(true, false);
    }
  }
}
