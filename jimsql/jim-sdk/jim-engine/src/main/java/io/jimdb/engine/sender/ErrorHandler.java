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
package io.jimdb.engine.sender;

import java.util.Arrays;

import io.jimdb.engine.client.RequestContext;
import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.exception.BaseException;
import io.jimdb.core.model.meta.RangeInfo;
import io.jimdb.meta.route.RoutePolicy;
import io.jimdb.pb.Errorpb.Error;
import io.jimdb.common.utils.lang.ByteUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.NettyByteString;

/**
 * @version V1.0
 */
public class ErrorHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(ErrorHandler.class);

  /**
   * uniformly handle returned exception from ds
   * <p>
   * 1.NotLeader exception;
   * handle: If the exception info contains a new leader, connect the new leader and update the local routing cache;
   * If the exception info does not contain the new leader, it will remove local routing cache.
   * </p>
   * <br>
   * <p>
   * 2.RangeNotFound exception;
   * handle: load routing info from ms and connect to the new ds leader for operation. If it failed, retry 3 times at specified interval.
   * If it succeeds, update the local routing cache.
   * </p>
   * <br>
   * <p>
   * 3.KeyNotInRange exception;
   * handle: load routing info from ms again and connect to the new ds leader for operation.
   * And throw an exception to the upper;
   * </p>
   * <br>
   * <p>
   * 4.StaleEpoch exception；
   * If the exception info contains the new ds leader, connect the new ds leader for operation.
   * </p>
   * <p>
   * 5.ServerIsBusy exception、6.EntryTooLarge exception、7.Timeout exception、8.RaftFail exception;
   * handle: throw an exception to the user;
   * </p>
   *
   * @param err
   * @param context
   * @throws BaseException
   */
  public static void handleRangeErr(Error err, RequestContext context, long msgId) throws BaseException {
    RangeInfo oldRangeInfo = context.getRangeInfo();
    byte[] key = NettyByteString.asByteArray(context.getKey());
    RoutePolicy routePolicy = context.getRoutePolicy();

    if (err.hasNotLeader()) {
      Error.NotLeader notLeaderErr = err.getNotLeader();
      //no leader
      if (!notLeaderErr.hasLeader()) {
        LOGGER.warn("ctxId {} msgId {}: ds report range {} no leader, key: {}, oldRange: {}, err: {}.",
                context.getCtxId(), msgId, oldRangeInfo.getId(), Arrays.toString(key), oldRangeInfo, err);
        //todo need delete ?
        routePolicy.removeRangeByKey(key, oldRangeInfo);
      } else {
        //update leader
        LOGGER.warn("ctxId {} msgId {}: ds report range {} not leader, need change leader, key {}, oldRange: {}, err: {}.",
                context.getCtxId(), msgId, oldRangeInfo.getId(), Arrays.toString(key), oldRangeInfo, err);
        routePolicy.updateRouteCacheForNewLeader(oldRangeInfo, notLeaderErr.getLeader().getNodeId(), notLeaderErr.getEpoch());
      }
      return;
    }

    //reassemble request
    if (err.hasRangeNotFound()) {
      RangeInfo cacheRange = routePolicy.getRangeInfoByKey(key);
      LOGGER.warn("ctxId {} msgId {}: ds report range {} `RangeNotFound`, key {}, oldRange: {}, cacheRange:{}, request:{}, err: {}.",
              context.getCtxId(), msgId, oldRangeInfo.getId(), Arrays.toString(key), oldRangeInfo, cacheRange, context.getReqBuilder(), err);
      if (cacheRange != null && cacheRange.getId() == oldRangeInfo.getId()
              && cacheRange.getLeaderNode().getId() != oldRangeInfo.getLeaderNode().getId()) {
        //do nothing
      } else {
        routePolicy.refreshRouteFromRemote(key);
      }
      throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_SHARD_ROUTE_CHANGE, oldRangeInfo.getLeaderAddr(), String.valueOf(oldRangeInfo.getId()));
    }

    //reassemble request
    if (err.hasOutOfBound()) {
      LOGGER.warn("ctxId {} msgId {}: ds report range {} `OutOfBound`, key {}, oldRange: {}, request:{}, err: {}.",
              context.getCtxId(), msgId, oldRangeInfo.getId(), Arrays.toString(key), oldRangeInfo, context.getReqBuilder(), err);
      throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_SHARD_ROUTE_CHANGE, oldRangeInfo.getLeaderAddr(), String.valueOf(oldRangeInfo.getId()));
    }

    //reassemble request
    if (err.hasStaleEpoch()) {
      LOGGER.warn("ctxId {} msgId {}: ds report range {} `StaleEpoch`, retry later, key {}, oldRange: {}, request:{}, err: {}.",
              context.getCtxId(), msgId, oldRangeInfo.getId(), Arrays.toString(key), oldRangeInfo, context.getReqBuilder(), err);
      routePolicy.updateRouteCacheByStaleEpoch(err.getStaleEpoch(), oldRangeInfo);
      throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_SHARD_ROUTE_CHANGE, oldRangeInfo.getLeaderAddr(), String.valueOf(oldRangeInfo.getId()));
    }

    if (err.hasServerError()) {
      LOGGER.warn("ctxId {} msgId {}: ds report range {} `ServerError`, key {}, oldRange: {}, err: {}.",
              context.getCtxId(), msgId, oldRangeInfo.getId(), Arrays.toString(key), oldRangeInfo, err);
      return;
    }

    if (err.hasRaftFail()) {
      LOGGER.warn("ctxId {} msgId {}: ds report range {} `RaftFail`, key {}, oldRange: {}, err: {}.",
              context.getCtxId(), msgId, oldRangeInfo.getId(), Arrays.toString(key), oldRangeInfo, err);
      throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_SHARD_RAFT_FAIL, oldRangeInfo.getLeaderAddr(), String.valueOf(oldRangeInfo.getId()));
    }

    // For other errors, we only drop cache here.
    // Because caller may need to re-split the request.
    LOGGER.error("ctxId {} msgId {}: ds report range {} `unknown error`, key {}, oldRange: {}, err: {}.",
            context.getCtxId(), msgId, oldRangeInfo.getId(), Arrays.toString(key), oldRangeInfo, err);
//    routePolicy.removeRangeByKey(key, null);
    throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_SHARD_RESPONSE_ERROR, ByteUtil.bytes2hex01(key), err.toString());
  }
}
