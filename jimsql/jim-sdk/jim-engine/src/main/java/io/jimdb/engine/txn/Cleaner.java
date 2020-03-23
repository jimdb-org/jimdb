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

package io.jimdb.engine.txn;

import java.util.function.Function;

import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.BaseException;
import io.jimdb.common.exception.RangeRouteException;
import io.jimdb.core.model.meta.RangeInfo;
import io.jimdb.engine.RequestHandler;
import io.jimdb.engine.ShardSender;
import io.jimdb.engine.StoreCtx;
import io.jimdb.engine.client.RequestContext;
import io.jimdb.pb.Api;
import io.jimdb.pb.Txn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.core.publisher.Flux;

/**
 * TODO
 */
@SuppressFBWarnings()
public class Cleaner extends RequestHandler {

  private static final Logger LOG = LoggerFactory.getLogger(Cleaner.class);

  public static RequestContext buildCleanupReqCtx(TxnConfig config, StoreCtx storeCtx) {
    Txn.ClearupRequest.Builder request = Txn.ClearupRequest.newBuilder()
                                             .setTxnId(config.getTxnId()).setPrimaryKey(config.getPriIntent().getKey());

    RangeInfo rangeInfo =  storeCtx.getRoutingPolicy().getRangeInfoByKey(request.getPrimaryKey().toByteArray());
    return new RequestContext(storeCtx, request.getPrimaryKey(), rangeInfo, request, Api.RangeRequest.ReqCase.CLEAR_UP);
  }

  public static Flux<Txn.ClearupResponse> cleanup(ShardSender shardSender, RequestContext context) {
    return shardSender.sendReq(context).map(response -> (Txn.ClearupResponse) response);
  }

  /**
   * TODO
   */

  @FunctionalInterface
  interface CleanupFunc {
    Flux<Txn.ClearupResponse> apply(StoreCtx context, TxnConfig config);
  }

  public static Function<Throwable, Flux<Txn.ClearupResponse>> getErrHandler(StoreCtx context, CleanupFunc func,
                                                                             TxnConfig config) {
    return throwable -> {
      if (context.canRetryWithDelay()) {
        if (throwable instanceof BaseException) {
          BaseException exception = (BaseException) throwable;
          if (exception.getCode() == ErrorCode.ER_SHARD_ROUTE_CHANGE) {
            return func.apply(context, config).onErrorResume(getErrHandler(context, func, config));
          } else if (exception.getCode() == ErrorCode.ER_SHARD_NOT_EXIST) {
            RangeRouteException routeException = (RangeRouteException) exception;
            if (context.getRoutingPolicy().rangeExists(routeException)) {
              return func.apply(context, config).onErrorResume(getErrHandler(context, func, config));
            }
            return context.getRouter().getRoutingFlux(context.getTable(), routeException.key).flatMap(flag -> func.apply(context, config)
                                                                                 .onErrorResume(getErrHandler(context, func, config)));
          }
        }
      }
      LOG.warn("do on err resume immediate throw err:", throwable);
      return Flux.error(throwable);
    };
  }
}
