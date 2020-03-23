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
package io.jimdb.meta.route;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.exception.BaseException;
import io.jimdb.common.exception.RangeRouteException;
import io.jimdb.core.model.meta.RangeInfo;
import io.jimdb.pb.Basepb;
import io.jimdb.pb.Errorpb;
import io.jimdb.core.plugin.RouterStore;
import io.jimdb.common.utils.lang.ByteUtil;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * route policy
 *
 * @version 1.0
 */
public final class RoutingPolicy {
  private static final Logger LOGGER = LoggerFactory.getLogger(RoutingPolicy.class);

  //route cache
  private RouteCache cache;
  private long clusterId;

  public RoutingPolicy(RouterStore routerStore, long clusterId, long dbId, long tableId) {
    this.clusterId = clusterId;
    this.cache = new RouteCache(routerStore, dbId, tableId);
  }

  public RangeInfo getRangeInfoByKeyFromCache(byte[] key) {
    return cache.getRangeByKey(key);
  }

  public RangeInfo getRangeInfoByKey(byte[] key) {
    // getPolicy route by key from local
    RangeInfo rangeInfo = cache.getRangeByKey(key);
    if (rangeInfo != null && StringUtils.isNotBlank(rangeInfo.getLeaderAddr())) {
      if (LOGGER.isInfoEnabled()) {
        LOGGER.info("local range for key from cache:[{}], range info: id[{}], start[{}], end[{}]",
                ByteUtil.bytes2hex01(key), rangeInfo.getId(), ByteUtil.bytes2hex01(rangeInfo.getStartKey()),
                ByteUtil.bytes2hex01(rangeInfo.getEndKey()));
      }
      return rangeInfo;
    }
    // getPolicy route by key from remote
    refreshRouteFromRemote(key);

    rangeInfo = cache.getRangeByKey(key);
    if (rangeInfo != null && StringUtils.isNotBlank(rangeInfo.getLeaderAddr())) {
      if (LOGGER.isInfoEnabled()) {
        LOGGER.info("local range for key:[{}], range info: id[{}], start[{}], end[{}]",
                ByteUtil.bytes2hex01(key), rangeInfo.getId(), ByteUtil.bytes2hex01(rangeInfo.getStartKey()),
                ByteUtil.bytes2hex01(rangeInfo.getEndKey()));
      }
    }
    return rangeInfo;
  }

  public boolean removeRangeByKey(byte[] key, RangeInfo oldRangeInfo) {
    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("start to removePolicy key {}", ByteUtil.bytes2hex01(key));
    }
    // removePolicy route by key from local
    return cache.removeRangeByKey(key, oldRangeInfo);
  }

  public void updateRouteCacheForNewLeader(RangeInfo rangeInfo, long leaderId, final Basepb.RangeEpoch rangeEpoch) {
    // update local route cache
    cache.updateRouteCacheForNewLeader(rangeInfo, leaderId, rangeEpoch);
  }

  public void updateRouteCacheByStaleEpoch(Errorpb.Error.StaleEpoch staleEpoch, RangeInfo rangeInfo) {
    Basepb.Range oldRange = null;
    Basepb.Range newRange = null;
    if (staleEpoch.hasOldRange()) {
      oldRange = staleEpoch.getOldRange();
    }
    if (staleEpoch.hasNewRange()) {
      newRange = staleEpoch.getNewRange();
    }

    if (oldRange == null) {
      cache.updateRouteCacheForStaleEpoch(newRange, rangeInfo);
    } else if (newRange == null) {
      cache.updateRouteCacheForStaleEpoch(oldRange, rangeInfo);
    } else {
      // update local route cache
      cache.updateRouteCacheForStaleEpoch(oldRange, newRange, rangeInfo);
    }
  }

  public void refreshRouteFromRemote(byte[] key) {
    cache.refreshRouteFromRemote(key);
  }

  public long getClusterId() {
    return this.clusterId;
  }

  public <T> Map<RangeInfo, List<T>> regroupByRoute(List<T> elements, Function<T, byte[]> keyFunction) throws BaseException {
    Map<RangeInfo, List<T>> groupMap = new HashMap<>();
    RangeInfo range = null;
    for (T element : elements) {
      byte[] key = keyFunction.apply(element);
      if (range != null && ByteUtil.compare(key, range.getStartKey()) >= 0 && ByteUtil.compare(key, range.getEndKey()) < 0) {
        List<T> group = groupMap.get(range);
        group.add(element);
        groupMap.put(range, group);
      } else {
        try {
          range = this.getRangeInfoByKey(key);
          if (range == null || StringUtils.isBlank(range.getLeaderAddr())) {
            LOGGER.warn("locate key no exist: range info is {}, key:{}, ", range == null ? "null" : range.getId(), Arrays.toString(key));
            throw RangeRouteException.get(ErrorModule.ENGINE, ErrorCode.ER_SHARD_NOT_EXIST, key);
          }
          List<T> group = groupMap.get(range);
          if (group == null) {
            group = new ArrayList<>();
            groupMap.put(range, group);
          }
          group.add(element);
        } catch (Throwable e) {
          LOGGER.warn("locate key failed, err:{}", e.getMessage());
          throw e;
        }
      }
    }
    return groupMap;
  }

  public Set<RangeInfo> getSerialRoute(byte[] start, byte[] end) {
    Set<RangeInfo> routeSet = new HashSet<>();
    byte[] key = start;
    RangeInfo range;
    while (true) {
      try {
        range = getRangeInfoByKey(key);
        if (range == null || StringUtils.isBlank(range.getLeaderAddr())) {
          LOGGER.error("locate route no exist by key {} for select, retry.", Arrays.toString(key));
          throw RangeRouteException.get(ErrorModule.ENGINE, ErrorCode.ER_SHARD_NOT_EXIST, key);
        }
        routeSet.add(range);
        key = range.getEndKey();
        if (ByteUtil.compare(key, start) < 0 || ByteUtil.compare(key, end) >= 0) {
          break;
        }
      } catch (Throwable e) {
        LOGGER.warn("locate key for serial route failed, err:{}", e.getMessage());
        throw e;
      }
    }
    return routeSet;
  }

  public boolean rangeExists(RangeRouteException routeException) {
    //maybe: table no exist, or range no exist
    RangeInfo rangeInfo = this.getRangeInfoByKeyFromCache(routeException.key);
    return rangeInfo != null && StringUtils.isNotBlank(rangeInfo.getLeaderAddr());
  }
}
