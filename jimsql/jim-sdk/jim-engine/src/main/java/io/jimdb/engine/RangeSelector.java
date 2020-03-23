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

package io.jimdb.engine;

import static io.jimdb.engine.RequestHandler.convertRowsToValueAccessors;
import static io.jimdb.engine.SelectHandler.SEND_SELECT_REQ_FUNC;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import io.jimdb.common.exception.BaseException;
import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.exception.RangeRouteException;
import io.jimdb.common.utils.lang.ByteUtil;
import io.jimdb.core.codec.KvPair;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.model.meta.RangeInfo;
import io.jimdb.engine.client.RequestContext;
import io.jimdb.meta.route.RoutingPolicy;
import io.jimdb.pb.Api;
import io.jimdb.pb.Txn;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.NettyByteString;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.core.publisher.Flux;

/**
 * TODO
 */
@SuppressFBWarnings()
public class RangeSelector {
  private static final Logger LOG = LoggerFactory.getLogger(RangeSelector.class);

  static final RangeSelectFunc RANGE_SELECT_FUNC = RangeSelector::rangeSelect;
  static final SingleRangeSelectFunc SINGLE_RANGE_SELECT_FUNC = RangeSelector::singleRangeSelect;


  ////////////////////////////////////////////// Multi-range select ///////////////////////////////////////////////////

  /**
   * Get rows based on given range
   *
   * @param storeCtx      TODO
   * @param reqBuilder    TODO
   * @param resultColumns TODO
   * @param kvPair        TODO
   * @return TODO
   */
  private static Flux<ValueAccessor[]> rangeSelect(ShardSender shardSender, StoreCtx storeCtx, MessageOrBuilder reqBuilder, ColumnExpr[] resultColumns, KvPair kvPair) {
    return txnRangeSelect(shardSender, storeCtx, reqBuilder, kvPair).map(rows -> convertRowsToValueAccessors(resultColumns, rows, storeCtx));
  }

  private static Flux<List<Txn.Row>> txnRangeSelect(ShardSender shardSender, StoreCtx storeCtx, MessageOrBuilder reqBuilder, KvPair kvPair) {
    return rangeSelect(shardSender, storeCtx, SEND_SELECT_REQ_FUNC, reqBuilder, kvPair.getKey(), kvPair.getValue());
  }

  // TODO 1. make the calls to different shards parallel
  //  2. define a union function to merge two value vectors with the same schema
  //  3. queue to buffer the responses?
  static <T> Flux<List<T>> rangeSelect(ShardSender shardSender, StoreCtx storeCtx, RequestHandler.RequestFunc func, MessageOrBuilder reqBuilder, ByteString start, ByteString end) {
    Flux<List<T>> flux = null;
    RoutingPolicy routePolicy = storeCtx.getRoutingPolicy();

    byte[] key = NettyByteString.asByteArray(start);
    RangeInfo rangeInfo;
    while (true) {
      rangeInfo = routePolicy.getRangeInfoByKey(key);
      if (rangeInfo == null || StringUtils.isBlank(rangeInfo.getLeaderAddr())) {
        LOG.error("locate route no exist by key {} for select, retry.", Arrays.toString(key));
        return Flux.error(RangeRouteException.get(ErrorModule.ENGINE, ErrorCode.ER_SHARD_NOT_EXIST, key));
      }

      Flux<List<T>> selectFlux = func.apply(shardSender, storeCtx, reqBuilder, NettyByteString.wrap(key), rangeInfo);
      if (flux == null) {
        flux = selectFlux;
      } else {
        flux = flux.zipWith(selectFlux, (f1, f2) -> getRows(f1, f2));
      }

      key = rangeInfo.getEndKey();
      if (ByteUtil.compare(key, start) < 0 || ByteUtil.compare(key, end) >= 0) {
        break;
      }
    }
    return flux;
  }

  private static <T> List<T> getRows(List<T> f1, List<T> f2) {
    if (f1.isEmpty()) {
      return f2;
    }
    if (f2.isEmpty()) {
      return f1;
    }
    List builders = new ArrayList<>(f1.size() + f2.size());
    builders.addAll(f1);
    builders.addAll(f2);
    return builders;
  }

  public static Function<Throwable, Flux<ValueAccessor[]>> getErrHandler(ShardSender shardSender, StoreCtx context, RangeSelectFunc func,
                                                                         Txn.SelectFlowRequest.Builder reqBuilder,
                                                                         ColumnExpr[] exprs, KvPair kvPair) {
    return throwable -> {
      if (context.canRetryWithDelay()) {
        if (throwable instanceof BaseException) {
          BaseException exception = (BaseException) throwable;
          if (exception.getCode() == ErrorCode.ER_SHARD_ROUTE_CHANGE) {
            return func.apply(shardSender, context, reqBuilder, exprs, kvPair).onErrorResume(getErrHandler(shardSender, context, func,
                reqBuilder, exprs, kvPair));
          } else if (exception.getCode() == ErrorCode.ER_SHARD_NOT_EXIST) {
            RangeRouteException routeException = (RangeRouteException) exception;
            if (context.getRoutingPolicy().rangeExists(routeException)) {
              return func.apply(shardSender, context, reqBuilder, exprs, kvPair).onErrorResume(getErrHandler(shardSender, context, func,
                  reqBuilder, exprs, kvPair));
            }
            return context.getRouter().getRoutingFlux(context.getTable(), routeException.key).flatMap(flag -> func.apply(shardSender, context, reqBuilder, exprs,
                kvPair).onErrorResume(getErrHandler(shardSender, context, func, reqBuilder, exprs, kvPair)));
          }
        }
      }
      LOG.warn("do on err resume immediate throw err:", throwable);
      return Flux.error(throwable);
    };
  }

  // SINGLE_RANGE_SELECT_FUNC
  private static Flux<ValueAccessor[]> singleRangeSelect(ShardSender shardSender, StoreCtx storeCtx, Txn.SelectFlowRequest.Builder reqBuilder,
                                                         ColumnExpr[] resultColumns, KvPair kvPair) {
    RoutingPolicy routingPolicy = storeCtx.getRoutingPolicy();
    ByteString start = kvPair.getKey();
    byte[] key = NettyByteString.asByteArray(start);
    RangeInfo rangeInfo = routingPolicy.getRangeInfoByKey(key);
    if (rangeInfo == null || StringUtils.isBlank(rangeInfo.getLeaderAddr())) {
      LOG.error("locate route no exist by key {} for select, retry.", Arrays.toString(key));
      throw RangeRouteException.get(ErrorModule.ENGINE, ErrorCode.ER_SHARD_NOT_EXIST, key);
    }

    RequestContext context = new RequestContext(storeCtx, NettyByteString.wrap(key), rangeInfo, reqBuilder, Api.RangeRequest.ReqCase.SELECT_FLOW);

    return shardSender.sendReq(context)
               .map(response -> {
                 Txn.SelectFlowResponse selectResponse = (Txn.SelectFlowResponse) response;
                 if (selectResponse.getCode() > 0) {
                   throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_SHARD_RESPONSE_CODE, "sendSingleRangeSelectReq",
                       String.valueOf(selectResponse.getCode()));
                 }
                 if (LOG.isInfoEnabled()) {
                   LOG.info("txn select response success: response row {}", selectResponse.getRowsList().size());
                 }
                 ByteString lastKey = selectResponse.getLastKey();
                 if (!lastKey.isEmpty()) {
                   reqBuilder.getProcessorsBuilder(0)
                       .getTableReadBuilder().getRangeBuilder().setEndKey(selectResponse.getLastKey());
                 }
                 return selectResponse.getRowsList();
               })
               .map(rows -> convertRowsToValueAccessors(resultColumns, rows, storeCtx));

  }


  ////////////////////////////////////////////// Single range select ///////////////////////////////////////////////////

  public static Function<Throwable, Flux<ValueAccessor[]>> getErrHandler(ShardSender shardSender, StoreCtx context, SingleRangeSelectFunc func,
                                                                         Txn.SelectFlowRequest.Builder reqBuilder,
                                                                         ColumnExpr[] exprs, KvPair kvPair) {
    return throwable -> {
      if (context.canRetryWithDelay()) {
        if (throwable instanceof BaseException) {
          BaseException exception = (BaseException) throwable;
          if (exception.getCode() == ErrorCode.ER_SHARD_ROUTE_CHANGE) {
            return func.apply(shardSender, context, reqBuilder, exprs, kvPair).onErrorResume(getErrHandler(shardSender, context, func,
                reqBuilder, exprs, kvPair));
          } else if (exception.getCode() == ErrorCode.ER_SHARD_NOT_EXIST) {
            RangeRouteException routeException = (RangeRouteException) exception;
            if (context.getRoutingPolicy().rangeExists(routeException)) {
              return func.apply(shardSender, context, reqBuilder, exprs, kvPair).onErrorResume(getErrHandler(shardSender, context, func,
                  reqBuilder, exprs, kvPair));
            }
            return context.getRouter().getRoutingFlux(context.getTable(), routeException.key).flatMap(flag -> func.apply(shardSender, context, reqBuilder, exprs,
                kvPair).onErrorResume(getErrHandler(shardSender, context, func, reqBuilder, exprs, kvPair)));
          }
        }
      }
      LOG.warn("do on err resume immediate throw err:", throwable);
      return Flux.error(throwable);
    };
  }

  /**
   * TODO
   */
  @FunctionalInterface
  interface RangeSelectFunc {
    Flux<ValueAccessor[]> apply(ShardSender shardSender, StoreCtx context, Txn.SelectFlowRequest.Builder reqBuilder, ColumnExpr[] exprs, KvPair kvPair);
  }

  /**
   * TODO
   */
  @FunctionalInterface
  interface SingleRangeSelectFunc {
    Flux<ValueAccessor[]> apply(ShardSender shardSender, StoreCtx context, Txn.SelectFlowRequest.Builder reqBuilder, ColumnExpr[] exprs, KvPair kvPair);
  }

}
