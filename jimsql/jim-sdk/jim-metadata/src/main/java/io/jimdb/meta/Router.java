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
import io.jimdb.core.model.meta.Table;
import io.jimdb.meta.client.MetaException;
import io.jimdb.core.model.meta.RangeInfo;
import io.jimdb.meta.route.RoutingPolicy;
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

import reactor.core.publisher.Flux;

/**
 * @version V1.0
 */
public final class Router {
  private static final Logger LOG = LoggerFactory.getLogger(Router.class);

  private RouterStore routerStore;
  private long clusterId;

  private ConcurrentMap<Long, RoutingPolicy> tableRouterMap = new ConcurrentHashMap<Long, RoutingPolicy>();

  private RetryPolicy retryPolicy;

  private ScheduledExecutorService retryExecutor;

  private ExecutorService callbackService;

  public Router(RouterStore routerStore, JimConfig c) {
    long clusterId = c.getMetaCluster();
    Preconditions.checkArgument(clusterId != 0, "client id must not be 0");
    this.clusterId = clusterId;
    this.routerStore = routerStore;
    this.retryPolicy = buildRetryPolicy();
    this.retryExecutor = new ScheduledThreadPoolExecutor(4, new NamedThreadFactory("Route-Retry-Executor", true));
    this.callbackService = c.getOutboundExecutor();
  }

  private RetryPolicy buildRetryPolicy() {
    return new RetryPolicy.Builder().retryDelay(30).maxRetryDelay(3000)
            .maxRetry(20)
            .expireTime(0)
            .useExponentialBackOff(true)
            .backOffMultiplier(1.2)
            .build();
  }

  public RoutingPolicy getOrCreatePolicy(long dbId, long tableId) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("getPolicy table[{}] router policy", tableId);
    }
    RoutingPolicy routingPolicy = tableRouterMap.get(tableId);
    if (routingPolicy == null) {
      synchronized (this) {
        if ((routingPolicy = tableRouterMap.get(tableId)) == null) {
          routingPolicy = new RoutingPolicy(this.routerStore, this.clusterId, dbId, tableId);
          tableRouterMap.put(tableId, routingPolicy);
        }
      }
    }
    return routingPolicy;
  }

  public RoutingPolicy getPolicy(long tableId) {
    return tableRouterMap.get(tableId);
  }

  public void removePolicy(long tableId) {
    tableRouterMap.remove(tableId);
  }

  public void retryRoute(int dbId, int tableId, byte[] key, Callback future) {
    this.retryExecutor.execute(new RetryTask("Retry-Route-Task", this.retryPolicy, retryExecutor, new RetryCallback() {
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
              callbackService.execute(() -> future.fail(e));
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
            future.fail(DBException.get(ErrorModule.ENGINE, ErrorCode.ER_CONTEXT_TIMEOUT, "get route from remote"));
          }
        });
      }
    }));
  }

  public Flux<Boolean> getRoutingFlux(Table table, byte[] key) {
    return Flux.create(sink -> {
      Callback callback = new Callback() {
        @Override
        public void success(boolean value) {
          sink.next(value);
        }

        @Override
        public void fail(Throwable cause) {
//          if (cause instanceof MetaException) {
//            MetaException error = (MetaException) cause;
//            if (error.getCode() == ErrorCode.ER_BAD_DB_ERROR || error.getCode() == ErrorCode.ER_BAD_TABLE_ERROR) {
//              //todo update table、routePolicy
////              storeCtx.setTable();
//              storeCtx.getRpcManager().removePolicy(storeCtx.getTable().getId());
////              storeCtx.setRoutePolicy();
//              sink.next(Boolean.TRUE);
//              return;
//            }
//          }
          sink.error(cause);
        }
      };
      this.retryRoute(table.getCatalogId(), table.getId(), key, callback);
    });
  }
}
