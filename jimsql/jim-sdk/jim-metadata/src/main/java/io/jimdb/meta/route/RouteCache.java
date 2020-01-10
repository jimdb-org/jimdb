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
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import io.jimdb.core.model.meta.RangeInfo;
import io.jimdb.pb.Basepb;
import io.jimdb.pb.Mspb;
import io.jimdb.core.plugin.RouterStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

/**
 * route cache
 *
 * @version 1.0
 */
public final class RouteCache extends LRUCache<RangeInfo, RangeInfo> {
  private static final Logger LOGGER = LoggerFactory.getLogger(RouteCache.class);

  // read-write lock for range cache
  private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

  private ConcurrentMap<Long, Basepb.Node> nodeCacheMap = new ConcurrentHashMap<>();

  private long dbId;
  private long tableId;

  private RouterStore routerStore;

  public RouteCache(RouterStore routerStore, long dbId, long tableId) {
    super(new TreeMap<RangeInfo, RangeInfo>());
    this.routerStore = routerStore;
    this.dbId = dbId;
    this.tableId = tableId;
  }

  /**
   * search range info by key
   *
   * @param key
   * @return RangeInfo
   */
  public RangeInfo getRangeByKey(byte[] key) {
    RangeInfo tempKey = new RangeInfo();
    tempKey.setStartKey(key);
    tempKey.setEndKey(key);

    rwLock.readLock().lock();
    try {
      return getObject(tempKey);
    } finally {
      rwLock.readLock().unlock();
    }
  }

  /***
   * load route info from master server by key and refresh local cache
   *
   * @param key
   */
  public void refreshRouteFromRemote(byte[] key) {
    Mspb.GetRouteResponse routeResponse = routerStore.getRoute(this.dbId, this.tableId, key, 1);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("local range from remote, key:{}, size:{}", key, routeResponse.getRoutesCount());
    }
    List<Basepb.Range> routesList = routeResponse.getRoutesList();
    if (routesList == null || routesList.isEmpty()) {
      return;
    }
    List<RangeInfo> rangeInfos = new ArrayList<>(routesList.size());
    for (Basepb.Range route : routesList) {
      // change route into to local range info object
      RangeInfo rangeInfo = generateRangeInfo(route);
      rangeInfos.add(rangeInfo);
    }

    rwLock.writeLock().lock();
    try {
      for (RangeInfo rangeInfo : rangeInfos) {
        LOGGER.warn("get rangeInfo from remote: {}", rangeInfo);
        // update local route cache
        updateLocalCache(rangeInfo);
      }
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  /**
   * convert Basepb.Route to RangeInfo
   *
   * @param range
   * @return
   */
  private RangeInfo generateRangeInfo(Basepb.RangeOrBuilder range) {
    RangeInfo rangeInfo = new RangeInfo();
    rangeInfo.setStartKey(range.getStartKey().toByteArray());
    rangeInfo.setEndKey(range.getEndKey().toByteArray());
    long nodeId = range.getLeader();
    if (nodeId == 0) {
      LOGGER.warn("generateRangeInfo: leader is 0, range:{}", range);
    } else {
      Basepb.Node leaderNode = getNode(nodeId);
      rangeInfo.setLeaderNode(leaderNode);
    }
    Basepb.RangeEpoch rangeEpoch = range.getRangeEpoch();
    List<Basepb.Peer> peersList = range.getPeersList();
    Map<Long, Basepb.Node> nodeMap = generatePeerMap(peersList);
    rangeInfo.setPeerList(peersList);
    rangeInfo.setNodeMap(nodeMap);
    rangeInfo.setId(range.getId());
    rangeInfo.setRangeEpoch(rangeEpoch);
    rangeInfo.setTableId(range.getTableId());
    return rangeInfo;
  }

  /**
   * key-value pairs of a range storage shards:  stored node Id and store node info
   *
   * @param peersList
   * @return
   */
  private Map<Long, Basepb.Node> generatePeerMap(List<Basepb.Peer> peersList) {
    Map<Long, Basepb.Node> peerMap = Maps.newHashMapWithExpectedSize(peersList.size());
    for (Basepb.Peer peer : peersList) {
      long nodeId = peer.getNodeId();
      Basepb.Node node = null;
      if (nodeId == 0) {
        LOGGER.warn("generatePeerMap: leader is 0, peer:{}", peer);
      } else {
        node = getNode(nodeId);
      }
      peerMap.put(nodeId, node);
    }
    return peerMap;
  }

  /**
   * update local route cache
   *
   * @param rangeInfo
   */
  private void updateLocalCache(RangeInfo rangeInfo) {
    rwLock.writeLock().lock();
    try {
      boolean cannotReplace;
      //delete loop. It can compatible to deal with  range merging, under range splitting
      RangeInfo cacheRange = rangeInfo;
      do {
        cacheRange = super.getObjectNoStat(cacheRange);
        if (cannotReplace = cannotReplace(rangeInfo, cacheRange)) {
          break;
        }
        LOGGER.warn("[updateLocalCache]remove cache {}, updateRange:{}", cacheRange, rangeInfo);
        super.removeObject(cacheRange);
      } while (cacheRange != null);
      // put RangeInfo to route cache
      if (!cannotReplace) {
        putObject(rangeInfo, rangeInfo);
      }
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  /**
   * loop to delete by rangeInfo
   *
   * @param key
   */
  public boolean removeRangeByKey(byte[] key, RangeInfo oldRangeInfo) {
    RangeInfo rangeInfo = new RangeInfo();
    rangeInfo.setStartKey(key);
    rangeInfo.setEndKey(key);
    rwLock.writeLock().lock();
    try {
      //delete loop. It can compatible to deal with  range merging, under range splitting
      RangeInfo cacheRange = rangeInfo;
      do {
        cacheRange = super.getObjectNoStat(cacheRange);
        if (cannotClear(oldRangeInfo, cacheRange)) {
          break;
        }
        LOGGER.warn("[removeRangeByKey]remove range by key {}, oldRange:{}, cacheRange:{}",
                key, rangeInfo.getId(), cacheRange);
        super.removeObject(cacheRange);
      } while (cacheRange != null);
      return true;
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  private boolean cannotReplace(RangeInfo newRange, RangeInfo cacheRange) {
    if (newRange == null) {
      //cannot replace
      return true;
    }

    if (cacheRange != null && cacheRange.getId() == newRange.getId()
            && compareRangerEpoch(cacheRange.getRangeEpoch(), newRange.getRangeEpoch()) > 0) {
//      LOGGER.warn("range{}: new epoch {} is lower cache epoch {}", newRange.getId(), newRange.getRangeEpoch(), cacheRange.getRangeEpoch());
      return true;
    }
    return false;
  }

  private boolean cannotClear(RangeInfo oldRange, RangeInfo cacheRange) {
    if (oldRange == null) {
      //can clear
      return false;
    }

    if (cacheRange != null && cacheRange.getId() == oldRange.getId()
            && compareRangerEpoch(cacheRange.getRangeEpoch(), oldRange.getRangeEpoch()) > 0) {
//      LOGGER.warn("range{}: old epoch {} is lower cache epoch {}", oldRange.getId(), oldRange.getRangeEpoch(), cacheRange.getRangeEpoch());
      return true;
    }
    return false;
  }

  /**
   * @param rangeInfo
   * @param nodeId
   * @param rangeInfo
   */
  public void updateRouteCacheForNewLeader(RangeInfo rangeInfo, long nodeId, final Basepb.RangeEpoch rangeEpoch) {
    rwLock.writeLock().lock();
    try {
      RangeInfo cacheRange = getObjectNoStat(rangeInfo);
      if (cacheRange != null && cacheRange.getId() == rangeInfo.getId()
              && cacheRange.getLeaderNode().getId() != nodeId) {
        RangeInfo newRangeInfo = cloneForNewLeader(cacheRange, nodeId, rangeEpoch);
        LOGGER.warn("[updateRouteCacheForNewLeader]remove range {}, new Range:{}", cacheRange, newRangeInfo);
        removeObject(cacheRange);
        putObject(newRangeInfo, newRangeInfo);
      }
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  private RangeInfo cloneForNewLeader(RangeInfo rangeInfo, long nodeId, final Basepb.RangeEpoch rangeEpoch) {
    if (compareRangerEpoch(rangeEpoch, rangeInfo.getRangeEpoch()) > 0) {
      LOGGER.warn("cloneForNewLeader: range {} new RangeEpoch:{}, cache RangeEpoch{}", rangeInfo.getId(), rangeEpoch,
              rangeInfo.getRangeEpoch());
    }
    RangeInfo newRangeInfo = rangeInfo.clone();
    if (nodeId == 0) {
      LOGGER.warn("updateRouteCacheForNewLeader: leader is 0, nodeId:{}, rangeEpoch:{}", nodeId, rangeEpoch);
    } else {
      Basepb.Node leaderNode = getNode(nodeId);
      newRangeInfo.setLeaderNode(leaderNode);
    }
//    newRangeInfo.setRangeEpoch(rangeEpoch);
    return newRangeInfo;
  }

  /**
   * update local route cache by ds returned exception info
   *
   * @param oldRange
   * @param newRange
   * @param rangeInfo
   */
  public void updateRouteCacheForStaleEpoch(Basepb.RangeOrBuilder oldRange, Basepb.RangeOrBuilder newRange, RangeInfo rangeInfo) {
    // old range split two new range  ex. old[0-10) news[0-5),[5,10)
    RangeInfo oldRangeInfo = convertRange(oldRange, rangeInfo);
    RangeInfo newRangeInfo = convertRange(newRange, rangeInfo);

    rwLock.writeLock().lock();
    try {
      RangeInfo cacheRange = super.getObjectNoStat(rangeInfo);
      if (cacheRange != null && cacheRange.getId() == oldRangeInfo.getId()
              && compareRangerEpoch(cacheRange.getRangeEpoch(), oldRangeInfo.getRangeEpoch()) < 0) {
        // removePolicy old range
        LOGGER.warn("[updateRouteCacheForStaleEpoch]remove cache range{}, put oldRange:{}, new Range{}",
                cacheRange, oldRangeInfo, newRangeInfo);
        removeObject(cacheRange);
        // add oldRange which return from ds and removePolicy least recently used key
        putObject(oldRangeInfo, oldRangeInfo);
        // add newRange which return from ds and removePolicy least recently used key
        putObject(newRangeInfo, newRangeInfo);
      }
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  /**
   * update local route cache by ds returned exception info
   *
   * @param range
   * @param rangeInfo
   */
  public void updateRouteCacheForStaleEpoch(Basepb.RangeOrBuilder range, RangeInfo rangeInfo) {
    // old range split two new range  ex. old[0-10) news[0-5),[5,10) update part
    RangeInfo updRangeInfo = convertRange(range, rangeInfo);
    this.updateLocalCache(updRangeInfo);
  }

  /**
   * convert Basepb.RangeOrBuilder& RangeInfo to new RangeInfo
   *
   * @param range
   * @param oldRangeInfo
   * @return
   */
  private RangeInfo convertRange(Basepb.RangeOrBuilder range, RangeInfo oldRangeInfo) {
    RangeInfo rangeInfo = new RangeInfo();
    rangeInfo.setStartKey(range.getStartKey().toByteArray());
    rangeInfo.setEndKey(range.getEndKey().toByteArray());
    rangeInfo.setPeerList(oldRangeInfo.getPeerList());
    rangeInfo.setNodeMap(oldRangeInfo.getNodeMap());
    long leaderId = range.getLeader();
    if (leaderId == 0) {
      LOGGER.warn("convertRange: leader is 0, range:{}", range);
    } else {
      Basepb.Node node = getNode(leaderId);
      rangeInfo.setLeaderNode(node);
    }
    rangeInfo.setRangeEpoch(range.getRangeEpoch());
    rangeInfo.setTableId(range.getTableId());
    rangeInfo.setId(range.getId());
    return rangeInfo;
  }

  private Basepb.Node getNode(long nodeId) {
    Basepb.Node node = nodeCacheMap.get(nodeId);
    if (node == null) {
      node = routerStore.getNode(nodeId, 3).getNode();
      Basepb.Node tempNode = nodeCacheMap.putIfAbsent(nodeId, node);
      if (tempNode != null) {
        node = tempNode;
      }
    }
    if (node == null) {
      LOGGER.error("get node err from nodeId{} null", nodeId);
    }

    return node;
  }

  public int compareRangerEpoch(Basepb.RangeEpochOrBuilder o1, Basepb.RangeEpochOrBuilder o2) {
    if (o1.getVersion() == o2.getVersion() && o1.getConfVer() == o2.getConfVer()) {
      return 0;
    }
    if ((o1.getVersion() == o2.getVersion() && o1.getConfVer() > o2.getConfVer())
            || (o1.getVersion() > o2.getVersion() && o1.getConfVer() == o2.getConfVer())
            || (o1.getVersion() > o2.getVersion() && o1.getConfVer() > o2.getConfVer())) {
      return 1;
    }
    return -1;
  }
}
