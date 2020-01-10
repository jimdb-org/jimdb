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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import io.jimdb.core.config.JimConfig;
import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.exception.JimException;
import io.jimdb.core.model.privilege.PrivilegeInfo;
import io.jimdb.core.model.privilege.PrivilegeType;
import io.jimdb.core.model.privilege.UserInfo;
import io.jimdb.pb.Ddlpb;
import io.jimdb.core.plugin.MetaStore;
import io.jimdb.core.plugin.PluginFactory;
import io.jimdb.core.plugin.PrivilegeEngine;
import io.jimdb.sql.privilege.cache.CatalogCache;
import io.jimdb.sql.privilege.cache.PrivilegeCache;
import io.jimdb.sql.privilege.cache.TableCache;
import io.jimdb.sql.privilege.cache.UserCache;
import io.jimdb.common.utils.lang.NamedThreadFactory;
import io.jimdb.common.utils.lang.StringUtil;
import io.jimdb.common.utils.os.SystemClock;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

/**
 * @version V1.0
 */
public final class CacheablePrivilege implements PrivilegeEngine {
  private static final Logger LOG = LoggerFactory.getLogger(CacheablePrivilege.class);

  private long delay;
  private MetaStore metaStore;
  private PrivilegeWorker worker;
  private PrivilegeSyncer syncer;
  private ScheduledExecutorService responseExecutor;

  @Override
  public void init(JimConfig config) {
    this.delay = Math.min(config.getPrvilegeLease(), 1000);
    this.metaStore = PluginFactory.getMetaStore();

    int errCount = 0;
    while (true) {
      boolean isLock = false;
      try {
        if (PrivilegeStore.isInited()) {
          break;
        }

        isLock = metaStore.tryLock(MetaStore.TaskType.PRITASK, null);
        if (!isLock) {
          continue;
        }

        PrivilegeStore.init(config.getPrivilegeStore(), config.getPrivilegeReplica());
        break;
      } catch (JimException ex) {
        if (errCount++ > 10) {
          throw ex;
        }
      } finally {
        if (isLock) {
          metaStore.unLock(MetaStore.TaskType.PRITASK, null);
        }
      }

      try {
        Thread.sleep(1000);
      } catch (InterruptedException ignored) {
      }
    }

    this.syncer = new PrivilegeSyncer(metaStore, config.getPrvilegeLease());
    this.syncer.start();
    this.worker = new PrivilegeWorker(metaStore);
    this.worker.start();
    responseExecutor = new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("Privilege-TaskResp-Executor", true));
  }

  @Override
  public void close() {
    if (worker != null) {
      worker.close();
    }
    if (syncer != null) {
      syncer.close();
    }
    if (responseExecutor != null) {
      responseExecutor.shutdown();
    }
  }

  @Override
  public void flush() {
    metaStore.addAndGetPrivVersion(1);
  }

  @Override
  public boolean verify(UserInfo userInfo, PrivilegeInfo... privilegeInfo) {
    PrivilegeCache privilege = PrivilegeCache.Holder.get();
    for (PrivilegeInfo info : privilegeInfo) {
      UserCache userCache = privilege.matchUser(userInfo.getUser(), userInfo.getHost());
      if (userCache != null && (userCache.getPrivilege() & info.getType().getCode()) > 0) {
        return true;
      }

      CatalogCache catalogCache = privilege.matchCatalog(userInfo.getUser(), userInfo.getHost(), info.getCatalog());
      if (catalogCache != null && (catalogCache.getPrivilege() & info.getType().getCode()) > 0) {
        return true;
      }

      TableCache tableCache = privilege.matchTable(userInfo.getUser(), userInfo.getHost(), info.getCatalog(), info.getTable());
      if (tableCache != null && (tableCache.getPrivilege() & info.getType().getCode()) > 0) {
        return true;
      }
    }
    return false;
  }

  @Override
  public List<String> showGrant(UserInfo userInfo, String user, String host) {
    PrivilegeCache privilege = PrivilegeCache.Holder.get();
    List<String> grants = new ArrayList<>(16);
    List<UserCache> userCaches = privilege.getUserPrivileges();
    int currentPriv = 0;
    boolean globalGrant = false;
    for (UserCache userCache : userCaches) {
      if (userCache.getUser().equals(user) && StringUtil.matchString(host, userCache.getHost())) {
        currentPriv |= userCache.getPrivilege();
        globalGrant = true;
      } else {
        if (userCache.getUser().equals(userInfo.getUser()) && userCache.getHost().equals(userInfo.getHost())) {
          globalGrant = true;
          currentPriv |= userCache.getPrivilege();
        }
      }
    }

    String grantStrs = PrivilegeType.privilegesToString(currentPriv);
    if (grantStrs.length() > 0) {
      String grantStr = String.format("GRANT %s ON *.* TO '%s'@'%s'", grantStrs, user, host);
      grants.add(grantStr);
    }
    if (grantStrs.length() == 0 && globalGrant) {
      String grantStr = String.format("GRANT USAGE ON *.* TO '%s'@'%s'", user, host);
      grants.add(grantStr);
    }

    Map<String, Integer> catalogPrivsMap = new HashMap<>();
    List<CatalogCache> catalogCaches = privilege.getCatalogPrivileges();
    for (CatalogCache catalogCache : catalogCaches) {
      if (catalogCache.getUser().equals(user) && StringUtil.matchString(host, catalogCache.getHost())) {
        Integer privs = catalogPrivsMap.get(catalogCache.getCatalog());
        if (privs != null) {
          privs |= catalogCache.getPrivilege();
          catalogPrivsMap.put(catalogCache.getCatalog(), privs);
        } else {
          catalogPrivsMap.put(catalogCache.getCatalog(), catalogCache.getPrivilege());
        }
      } else {
        if (catalogCache.getUser().equals(userInfo.getUser()) && StringUtil.matchString(host, catalogCache.getHost())) {
          Integer privs = catalogPrivsMap.get(catalogCache.getCatalog());
          if (privs != null) {
            privs |= catalogCache.getPrivilege();
            catalogPrivsMap.put(catalogCache.getCatalog(), privs);
          } else {
            catalogPrivsMap.put(catalogCache.getCatalog(), catalogCache.getPrivilege());
          }
        }
      }
    }
    catalogPrivsMap.forEach((k, v) -> {
      String catalogPrivs = PrivilegeType.privilegesToString(v);
      if (catalogPrivs.length() > 0) {
        String catalog = String.format("GRANT USAGE ON *.* TO '%s'@'%s'", user, host);
        grants.add(catalog);
      }
    });

    Map<String, Integer> tablePrivsMap = new HashMap<>();
    List<TableCache> tableCaches = privilege.getTablePrivileges();
    for (TableCache tableCache : tableCaches) {
      String cacheKey = String.format("%s.%s", tableCache.getCatalog(), tableCache.getTable());
      if (tableCache.getUser().equals(user) && tableCache.getHost().equals(host)) {
        if (catalogPrivsMap.get(tableCache.getCatalog()) != null) {
          Integer tablePriv = tablePrivsMap.get(cacheKey);
          if (tablePriv == null) {
            tablePrivsMap.put(cacheKey, tableCache.getPrivilege());
          } else {
            tablePrivsMap.put(cacheKey, tablePriv | tableCache.getPrivilege());
          }
        }
      } else {
        if (tableCache.getUser().equals(userInfo.getUser()) && tableCache.getHost().equals(userInfo.getHost())) {
          if (catalogPrivsMap.get(tableCache.getCatalog()) != null) {
            Integer privs = tablePrivsMap.get(cacheKey);
            if (privs != null) {
              privs |= tableCache.getPrivilege();
              catalogPrivsMap.put(tableCache.getCatalog(), privs);
            } else {
              catalogPrivsMap.put(tableCache.getCatalog(), tableCache.getPrivilege());
            }
          }
        }
      }
    }

    tablePrivsMap.forEach((k, v) -> {
      String tablePrivs = PrivilegeType.privilegesToString(v);
      if (tablePrivs.length() > 0) {
        String table = String.format("GRANT %s ON %s TO '%s'@'%s'", tablePrivs, k, user, host);
        grants.add(table);
      }
    });

    return grants;
  }

  public boolean catalogIsVisible(UserInfo userInfo, String user, String host, String db) {
    PrivilegeCache privilege = PrivilegeCache.Holder.get();
    if (!catalogIsVisible(privilege, user, host, db)) {
      if (catalogIsVisible(privilege, userInfo.getUser(), userInfo.getHost(), db)) {
        return true;
      }
    } else {
      return true;
    }
    return false;
  }

  public boolean catalogIsVisible(PrivilegeCache privilege, String user, String host, String db) {
    UserCache userCache = privilege.matchUser(user, host);
    if (null != userCache && userCache.getPrivilege() != 0) {
      return true;
    }

    CatalogCache catalogCache = privilege.matchCatalog(user, host, db);
    if (null != catalogCache && catalogCache.getPrivilege() != 0) {
      return true;
    }

    TableCache tableCache = privilege.matchTable(user, host, db);
    if (null != tableCache && tableCache.getPrivilege() != 0) {
      return true;
    }

    return false;
  }

  @Override
  public boolean auth(UserInfo userInfo, String catalog, byte[] auth, byte[] salt) {
    if (StringUtils.isBlank(userInfo.getHost())) {
      return false;
    }

    if (catalog != null && !verify(userInfo, new PrivilegeInfo(catalog, null, PrivilegeType.SHOW_DB_PRIV))) {
      return false;
    }

    String remoteIp = userInfo.getHost().split(":")[0];
    String password = connectionVerification(userInfo.getUser(), remoteIp);
    if (password == null) {
      return false;
    }
    return ScrambleUtil.checkPassword(password, salt, auth);
  }

  @Override
  public String getPassword(String user, String host) {
    return connectionVerification(user, host);
  }

  @Override
  public Flux<Boolean> grant(Ddlpb.OpType type, Ddlpb.PrivilegeOp op) {
    Ddlpb.Task.Builder taskBuilder = Ddlpb.Task.newBuilder();
    taskBuilder.setId(metaStore.allocTaskID(MetaStore.TaskType.PRITASK))
            .setOp(type)
            .setData(op.toByteString())
            .setCreateTime(SystemClock.currentTimeMillis())
            .setState(Ddlpb.TaskState.Init);
    Ddlpb.Task task = taskBuilder.build();
    metaStore.storeTask(MetaStore.TaskType.PRITASK, null, task);
    return responseTask(task);
  }

  private Flux<Boolean> responseTask(final Ddlpb.Task task) {
    return Flux.create(sink -> {
      TaskResponseSyncer syncer = new TaskResponseSyncer(task.getId(), sink);
      syncer.start();
    });
  }

  private String connectionVerification(String user, String host) {
    PrivilegeCache privilege = PrivilegeCache.Holder.get();
    UserCache matchUser = privilege.matchUser(user, host);
    return null == matchUser ? null : matchUser.getPassword();
  }

  /**
   * Task response syncer.
   */
  final class TaskResponseSyncer implements Runnable {
    private final long taskID;
    private final FluxSink<Boolean> sink;
    private ScheduledFuture<?> future;

    TaskResponseSyncer(long taskID, FluxSink<Boolean> sink) {
      this.sink = sink;
      this.taskID = taskID;
    }

    void start() {
      future = responseExecutor.scheduleWithFixedDelay(this, delay, delay, TimeUnit.MILLISECONDS);
    }

    @Override
    public void run() {
      Ddlpb.Task task = null;
      try {
        task = metaStore.getTask(MetaStore.TaskType.PRIHISTORY, Ddlpb.Task.newBuilder().setId(taskID).build());
      } catch (Exception ex) {
        LOG.error("get task[" + taskID + "] from history_queue error", ex);
      }

      if (task == null) {
        return;
      }

      try {
        if (task.getState() == Ddlpb.TaskState.Success) {
          sink.next(Boolean.TRUE);
          sink.complete();
        } else {
          sink.error(DBException.get(ErrorModule.PRIVILEGE, ErrorCode.valueOf(task.getErrorCode()), false, task.getError()));
        }
      } catch (Exception ex) {
        LOG.error("reply task[" + taskID + "] error", ex);
      } finally {
        future.cancel(true);
      }
    }
  }
}
