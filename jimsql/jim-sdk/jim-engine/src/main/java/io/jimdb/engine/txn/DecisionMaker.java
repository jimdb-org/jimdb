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

import static io.jimdb.engine.ErrorHandler.retrieveException;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.BaseException;
import io.jimdb.common.exception.RangeRouteException;
import io.jimdb.core.model.meta.RangeInfo;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.engine.RequestHandler;
import io.jimdb.engine.ShardSender;
import io.jimdb.engine.StoreCtx;
import io.jimdb.engine.client.RequestContext;
import io.jimdb.meta.route.RoutingPolicy;
import io.jimdb.pb.Api;
import io.jimdb.pb.Txn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.core.publisher.Flux;

/**
 * TODO
 */
@SuppressFBWarnings()
public class DecisionMaker extends RequestHandler {
  private static final Logger LOG = LoggerFactory.getLogger(DecisionMaker.class);

  private static final DecideSecondaryFunc DECIDE_SEC_FUNC = DecisionMaker::decideSecondary;

  public static RequestContext buildPrimaryDecideReqCtx(TxnConfig config, StoreCtx storeCtx, Txn.TxnStatus status) {
    Txn.DecideRequest.Builder reqBuilder = buildTxnDecide4Primary(config, status);

    RangeInfo rangeInfo =  storeCtx.getRoutingPolicy().getRangeInfoByKey(reqBuilder.getKeys(0).toByteArray());
    return new RequestContext(storeCtx, reqBuilder.getKeys(0), rangeInfo, reqBuilder, Api.RangeRequest.ReqCase.DECIDE);
  }

  public static Txn.DecideRequest.Builder buildTxnDecide4Primary(TxnConfig config, Txn.TxnStatus status) {
    return Txn.DecideRequest.newBuilder()
               .setTxnId(config.getTxnId())
               .setStatus(status)
               .addKeys(config.getPriIntent().getKey())
               .setIsPrimary(true);
  }


  public static RequestContext buildDecideSecondaryReqCxt(StoreCtx storeCtx, String txnId, Txn.TxnStatus status,
                                                          List<ByteString> keyList, RangeInfo rangeInfo) {
    Txn.DecideRequest.Builder reqBuilder = Txn.DecideRequest.newBuilder()
                                               .setTxnId(txnId).setStatus(status)
                                               .addAllKeys(keyList);

    return new RequestContext(storeCtx, reqBuilder.getKeys(0), rangeInfo, reqBuilder, Api.RangeRequest.ReqCase.DECIDE);
  }

  public static Flux<Txn.DecideResponse> txnDecidePrimary(ShardSender shardSender, RequestContext context) {
    Txn.DecideRequestOrBuilder reqBuilder = (Txn.DecideRequestOrBuilder) context.getReqBuilder();

    return shardSender.sendReq(context).map(response -> (Txn.DecideResponse) response).map(response -> {
      if (response.hasErr()) {
        Txn.TxnError txnError = response.getErr();
        if (txnError.getErrType() == Txn.TxnError.ErrType.NOT_FOUND) {
          LOG.warn("[commit]decide txn{}: ds return it not found, ignore", reqBuilder.getTxnId());
        } else {
          BaseException err = retrieveException(txnError);
          LOG.error("[commit]decide txn{} primary intent error: {} ", reqBuilder.getTxnId(), err);
          throw err;
        }
      }
      if (LOG.isInfoEnabled()) {
        LOG.info("txn {} decide primary success", reqBuilder.getTxnId());
      }
      return response;
    });
  }


  public static Flux<Boolean> decideSecondary(ShardSender shardSender, StoreCtx context, String txnId, List<ByteString> keys, Txn.TxnStatus
                                                                                                         status) {
    if (LOG.isInfoEnabled()) {
      LOG.info("start to decide txn {} secondary", txnId);
    }

    if (keys == null || keys.isEmpty()) {
      return Flux.just(Boolean.TRUE);
    }

    RoutingPolicy routingPolicy = context.getRoutingPolicy();

    Flux<Boolean> flux = null;

    Map<RangeInfo, List<ByteString>> intentKeyGroupMap;
    try {
      intentKeyGroupMap = routingPolicy.regroupByRoute(keys, ByteString::toByteArray);
    } catch (Throwable e) {
      LOG.warn("txn {} decide Secondary {} error:{}", txnId, keys, e);
      return DecisionMaker.getErrHandler(shardSender, context, DECIDE_SEC_FUNC, txnId, status, keys, e);
    }

    for (Map.Entry<RangeInfo, List<ByteString>> entry : intentKeyGroupMap.entrySet()) {
      List<ByteString> keyList = entry.getValue();
      if (keyList == null || keyList.isEmpty()) {
        continue;
      }

      Flux<Boolean> child = txnDecide(shardSender, context, txnId, keyList, status, entry.getKey());
      child = child.onErrorResume(DecisionMaker.getErrHandler(shardSender, context, DECIDE_SEC_FUNC, txnId, status, keyList));
      if (flux == null) {
        flux = child;
      } else {
        flux = flux.zipWith(child, (f1, f2) -> {
          if (!f1.booleanValue() || !f2.booleanValue()) {
            return Boolean.FALSE;
          }
          return Boolean.TRUE;
        });
      }
    }
    return flux;
  }

  private static Flux<Boolean> txnDecide(ShardSender shardSender, StoreCtx context, String txnId, List<ByteString> keyList,
                                         Txn.TxnStatus status, RangeInfo rangeInfo) {
    RequestContext reqCxt = buildDecideSecondaryReqCxt(context, txnId, status, keyList, rangeInfo);
    return shardSender.sendReq(reqCxt).map(response -> (Txn.DecideResponse) response).map(r -> {
      if (r.hasErr()) {
        //store and so on
        return Boolean.FALSE;
      }
      return Boolean.TRUE;
    });
  }

  /**
   * TODO
   * @param <T>
   */

  @FunctionalInterface
  interface DecideSecondaryFunc<T> {
    Flux<Boolean> apply(ShardSender shardSender, StoreCtx context, String txnId, List<T> list, Txn.TxnStatus status);
  }

  public static <T> Function<Throwable, Flux> getErrHandler(ShardSender shardSender, StoreCtx storeCtx, DecideSecondaryFunc<T> func,
                                                            String txnId, Txn.TxnStatus status, List<T> list) {
    return throwable -> getErrHandler(shardSender, storeCtx, func, txnId, status, list, throwable);
  }

  public static <T> Flux getErrHandler(ShardSender shardSender, StoreCtx storeCtx, DecideSecondaryFunc<T> func,
                                       String txnId, Txn.TxnStatus status, List<T> list, Throwable throwable) {
    if (storeCtx.canRetryWithDelay()) {

      if (throwable instanceof BaseException) {
        BaseException exception = (BaseException) throwable;
        if (exception.getCode() == ErrorCode.ER_SHARD_ROUTE_CHANGE) {
          return func.apply(shardSender, storeCtx, txnId, list, status);
        } else if (exception.getCode() == ErrorCode.ER_SHARD_NOT_EXIST) {
          RangeRouteException routeException = (RangeRouteException) exception;
          if (storeCtx.getRoutingPolicy().rangeExists(routeException)) {
            return func.apply(shardSender, storeCtx, txnId, list, status);
          }
          return storeCtx.getRouter().getRoutingFlux(storeCtx.getTable(), routeException.key).flatMap(flag -> func.apply(shardSender, storeCtx, txnId, list, status));
        }
      }
    }
    LOG.warn("do on err resume immediate throw err", throwable);
    return Flux.error(throwable);
  }

  /**
   * TODO
   */

  @FunctionalInterface
  interface DecidePrimaryFunc {
    Flux<ExecResult> apply(ShardSender shardSender, StoreCtx context, TxnConfig config, Txn.TxnStatus status);
  }

  public static Function<Throwable, Flux<ExecResult>> getErrHandler(ShardSender shardSender, StoreCtx context, DecidePrimaryFunc func,
                                                                    TxnConfig config, Txn.TxnStatus status) {
    return throwable -> {
      if (context.canRetryWithDelay()) {
        if (throwable instanceof BaseException) {
          BaseException exception = (BaseException) throwable;
          if (exception.getCode() == ErrorCode.ER_SHARD_ROUTE_CHANGE) {
            return func.apply(shardSender, context, config, status).onErrorResume(getErrHandler(shardSender, context, func, config, status));
          } else if (exception.getCode() == ErrorCode.ER_SHARD_NOT_EXIST) {
            RangeRouteException routeException = (RangeRouteException) exception;
            if (context.getRoutingPolicy().rangeExists(routeException)) {
              return func.apply(shardSender, context, config, status).onErrorResume(getErrHandler(shardSender, context, func, config, status));
            }
            return context.getRouter().getRoutingFlux(context.getTable(), routeException.key).flatMap(flag -> func.apply(shardSender, context, config, status)
                                                                                 .onErrorResume(getErrHandler(shardSender, context, func, config, status)));
          }
        }
      }
      LOG.warn("do on err resume immediate throw err:", throwable);
      return Flux.error(throwable);
    };
  }
}
