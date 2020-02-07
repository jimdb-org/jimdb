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
package io.jimdb.sql.privilege;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.jimdb.common.exception.JimException;
import io.jimdb.common.utils.lang.NamedThreadFactory;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.model.privilege.PrivilegeType;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.plugin.MetaStore;
import io.jimdb.core.values.Value;
import io.jimdb.sql.privilege.cache.CatalogCache;
import io.jimdb.sql.privilege.cache.PrivilegeCache;
import io.jimdb.sql.privilege.cache.PrivilegeCacheHolder;
import io.jimdb.sql.privilege.cache.TableCache;
import io.jimdb.sql.privilege.cache.UserCache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @version V1.0
 */
final class PrivilegeSyncer implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(PrivilegeSyncer.class);

  private final long delay;
  private final MetaStore metaStore;
  private final ScheduledExecutorService syncExecutor;
  private final AtomicBoolean loading = new AtomicBoolean(false);

  PrivilegeSyncer(MetaStore metaStore, long delay) {
    this.delay = delay;
    this.metaStore = metaStore;
    this.syncExecutor = new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("Prvilege-Syncer-Executor", true));
  }

  void start() {
    int i = 0;
    while (true) {
      try {
        this.load();
        break;
      } catch (JimException ex) {
        if (i++ > 10) {
          throw ex;
        }

        try {
          Thread.sleep(1000);
        } catch (Exception e) {
          LOG.error(e.getMessage());
        }
      }
    }

    metaStore.watch(MetaStore.WatchType.PRIVERSION, l -> syncExecutor.execute(this::sync));
    syncExecutor.scheduleWithFixedDelay(this::sync, delay, delay, TimeUnit.MILLISECONDS);
  }

  @Override
  public void close() {
    if (syncExecutor != null) {
      syncExecutor.shutdown();
    }
  }

  private void sync() {
    try {
      this.load();
    } catch (Exception ex) {
      LOG.error("PrivilegeSyncer load privileges error", ex);
    }
  }

  private void load() {
    if (!loading.compareAndSet(false, true)) {
      return;
    }

    try {
      long curVersion = PrivilegeCacheHolder.get().getVersion();
      long syncVersion = metaStore.addAndGetPrivVersion(0);
      if (curVersion == syncVersion) {
        return;
      }

      ExecResult userResult = PrivilegeStore.loadUser();
      ExecResult dbResult = PrivilegeStore.loadDB();
      ExecResult tableResult = PrivilegeStore.loadTable();
      PrivilegeCacheHolder.set(new PrivilegeCache(buildUserCache(userResult), buildCatalogCache(dbResult), buildTableCache(tableResult), syncVersion));
    } finally {
      loading.compareAndSet(true, false);
    }
  }

  private List<UserCache> buildUserCache(ExecResult result) {
    List<UserCache> userCaches = new ArrayList<>();
    ColumnExpr[] columns = result.getColumns();
    result.forEach(row -> {
      int priv = 0;
      String user = "";
      String host = "";
      String password = "";
      for (ColumnExpr column : columns) {
        Value value = row.get(column.getOffset());
        if ("user".equalsIgnoreCase(column.getAliasCol())) {
          user = value.getString();
        } else if ("host".equalsIgnoreCase(column.getAliasCol())) {
          host = value.getString();
        } else if ("password".equalsIgnoreCase(column.getAliasCol())) {
          password = value.getString();
        } else {
          PrivilegeType type = PrivilegeType.getType(column.getAliasCol());
          if (type != null && "Y".equalsIgnoreCase(value.getString())) {
            priv |= type.getCode();
          }
        }
      }

      userCaches.add(new UserCache(user, host, password, priv));
    });
    return userCaches;
  }

  private List<CatalogCache> buildCatalogCache(ExecResult result) {
    List<CatalogCache> catalogCaches = new ArrayList<>();
    ColumnExpr[] columns = result.getColumns();
    result.forEach(row -> {
      int priv = 0;
      String user = "";
      String host = "";
      String catalog = "";
      for (ColumnExpr column : columns) {
        Value value = row.get(column.getOffset());
        if ("user".equalsIgnoreCase(column.getAliasCol())) {
          user = value.getString();
        } else if ("host".equalsIgnoreCase(column.getAliasCol())) {
          host = value.getString();
        } else if ("db".equalsIgnoreCase(column.getAliasCol())) {
          catalog = value.getString();
        } else {
          PrivilegeType type = PrivilegeType.getType(column.getAliasCol());
          if (type != null && "Y".equalsIgnoreCase(value.getString())) {
            priv |= type.getCode();
          }
        }
      }

      catalogCaches.add(new CatalogCache(user, host, catalog, priv));
    });
    return catalogCaches;
  }

  private List<TableCache> buildTableCache(ExecResult result) {
    List<TableCache> tableCaches = new ArrayList<>();
    ColumnExpr[] columns = result.getColumns();
    result.forEach(row -> {
      int priv = 0;
      String user = "";
      String host = "";
      String catalog = "";
      String table = "";
      for (ColumnExpr column : columns) {
        Value value = row.get(column.getOffset());
        if ("user".equalsIgnoreCase(column.getAliasCol())) {
          user = value.getString();
        } else if ("host".equalsIgnoreCase(column.getAliasCol())) {
          host = value.getString();
        } else if ("db".equalsIgnoreCase(column.getAliasCol())) {
          catalog = value.getString();
        } else if ("table_name".equalsIgnoreCase(column.getAliasCol())) {
          table = value.getString();
        } else if ("Table_priv".equalsIgnoreCase(column.getAliasCol())) {
          if (value != null) {
            String[] psTypes = value.getString().split(",");
            for (String psType : psTypes) {
              PrivilegeType type = PrivilegeType.getType(psType);
              if (type != null) {
                priv |= type.getCode();
              }
            }
          }
        }
      }

      tableCaches.add(new TableCache(user, host, catalog, table, priv));
    });
    return tableCaches;
  }
}
