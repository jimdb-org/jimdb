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
package io.jimdb.meta;

import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import io.jimdb.core.config.JimConfig;
import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.meta.client.MetaException;
import io.jimdb.core.model.meta.RangeInfo;
import io.jimdb.meta.route.RoutePolicy;
import io.jimdb.core.plugin.RouterStore;
import io.jimdb.common.utils.callback.Callback;
import io.jimdb.common.utils.lang.NamedThreadFactory;
import io.jimdb.common.utils.retry.RetryCallback;
import io.jimdb.common.utils.retry.RetryPolicy;
import io.jimdb.common.utils.retry.RetryTask;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * @version V1.0
 */
public final class RouterManager {
  private static final Logger LOG = LoggerFactory.getLogger(RouterManager.class);

  private RouterStore routerStore;
  private long clusterId;

  private ConcurrentMap<Long, RoutePolicy> tableRouterMap = new ConcurrentHashMap<Long, RoutePolicy>();

  private RetryPolicy routeRetryPolicy;

  private ScheduledExecutorService retryExecutor;

  private ExecutorService callbackService;

  public RouterManager(RouterStore routerStore, JimConfig c) {
    long clusterId = c.getMetaCluster();
    Preconditions.checkArgument(clusterId != 0, "client id must not be 0");
    this.clusterId = clusterId;
    this.routerStore = routerStore;
    this.routeRetryPolicy = buildRouteRetryPolicy();
    this.retryExecutor = new ScheduledThreadPoolExecutor(4, new NamedThreadFactory("Route-Retry-Executor", true));
    this.callbackService = c.getOutboundExecutor();
  }

  private RetryPolicy buildRouteRetryPolicy() {
    return new RetryPolicy.Builder().retryDelay(30).maxRetryDelay(3000)
            .maxRetry(20)
            .expireTime(0)
            .useExponentialBackOff(true)
            .backOffMultiplier(1.2)
            .build();
  }

  public RoutePolicy getOrCreatePolicy(long dbId, long tableId) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("getPolicy table[{}] router policy", tableId);
    }
    RoutePolicy routePolicy = tableRouterMap.get(tableId);
    if (routePolicy == null) {
      synchronized (this) {
        if ((routePolicy = tableRouterMap.get(tableId)) == null) {
          routePolicy = new RoutePolicy(this.routerStore, this.clusterId, dbId, tableId);
          tableRouterMap.put(tableId, routePolicy);
        }
      }
    }
    return routePolicy;
  }

  public RoutePolicy getPolicy(long tableId) {
    return tableRouterMap.get(tableId);
  }

  public void removePolicy(long tableId) {
    tableRouterMap.remove(tableId);
  }

  public void retryRoute(int dbId, int tableId, byte[] key, Callback future) {
    this.retryExecutor.execute(new RetryTask("Retry-Route-Task", this.routeRetryPolicy, retryExecutor, new RetryCallback() {
      @Override
      public boolean execute() {
        if (LOG.isWarnEnabled()) {
          LOG.warn("locate table {} key at retry task", tableId);
        }
        try {
          RangeInfo rangeInfo = getOrCreatePolicy(dbId, tableId).getRangeInfoByKey(key);
          if (rangeInfo == null || StringUtils.isBlank(rangeInfo.getLeaderAddr())) {
            return false;
          }
        } catch (Throwable e) {
          if (LOG.isWarnEnabled()) {
            LOG.warn("Get route key {} by async error {}", Arrays.toString(key), e);
          }
          if (e instanceof MetaException) {
            MetaException error = (MetaException) e;
            if (error.getCode() == ErrorCode.ER_BAD_DB_ERROR || error.getCode() == ErrorCode.ER_BAD_TABLE_ERROR) {
              callbackService.execute(() -> future.failed(e));
              return true;
            }
          }
          return false;
        }
        callbackService.execute(() -> future.success(true));
        return true;
      }

      @Override
      public void onTerminate(boolean success) {
        callbackService.execute(() -> {
          if (!success) {
            future.failed(DBException.get(ErrorModule.ENGINE, ErrorCode.ER_CONTEXT_TIMEOUT, "get route from remote"));
          }
        });
      }
    }));
  }
}
