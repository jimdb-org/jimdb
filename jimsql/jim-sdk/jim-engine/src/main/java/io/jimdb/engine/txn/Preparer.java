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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.BaseException;
import io.jimdb.common.exception.RangeRouteException;
import io.jimdb.core.model.meta.RangeInfo;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.model.result.impl.AckExecResult;
import io.jimdb.engine.ErrorHandler;
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
public class Preparer extends RequestHandler {

  private static final Logger LOG = LoggerFactory.getLogger(Preparer.class);

  // PREPARE_PRIMARY_FUNC is used in Txn1PLHandler and Txn2PLHandler
  static final PreparePrimaryFunc PREPARE_PRIMARY_FUNC = Preparer::txnPreparePrimary;

  public static RequestContext buildPrimaryPrepareReqCxt(TxnConfig config, StoreCtx storeCtx) {
    Txn.PrepareRequest.Builder primaryReq = buildPrepare4Primary(config);

    RangeInfo rangeInfo =  storeCtx.getRoutingPolicy().getRangeInfoByKey(primaryReq.getIntents(0).getKey().toByteArray());
    return new RequestContext(storeCtx, primaryReq.getIntents(0).getKey(), rangeInfo, primaryReq, Api.RangeRequest.ReqCase.PREPARE);
  }

  public static RequestContext buildSecondaryPrepareReqCxt(TxnConfig config, StoreCtx storeCtx, List<Txn.TxnIntent>
                                                                                                    intentList, RangeInfo rangeInfo) {
    Txn.PrepareRequest.Builder secondaryReq = buildPrepare4Secondary(config, intentList);
    return new RequestContext(storeCtx, secondaryReq.getIntents(0).getKey(), rangeInfo, secondaryReq, Api.RangeRequest.ReqCase.PREPARE);
  }

  public static Txn.PrepareRequest.Builder buildPrepare4Primary(TxnConfig config) {
    Txn.PrepareRequest.Builder bodyBuilder = Txn.PrepareRequest.newBuilder()
                                                 .setTxnId(config.getTxnId())
                                                 .setLocal(config.isLocal())
                                                 .addIntents(config.getPriIntent())
                                                 .setPrimaryKey(config.getPriIntent().getKey())
                                                 .setLockTtl(config.getLockTTl())
                                                 .setStrictCheck(true);
    List<Txn.TxnIntent> secIntents = config.getSecIntents();
    if (secIntents != null && secIntents.size() > 0) {
      for (Txn.TxnIntent intent : secIntents) {
        bodyBuilder.addSecondaryKeys(intent.getKey());
      }
    }
    return bodyBuilder;
  }

  public static Txn.PrepareRequest.Builder buildPrepare4Secondary(TxnConfig config, List<Txn.TxnIntent> intents) {
    return Txn.PrepareRequest.newBuilder()
               .setTxnId(config.getTxnId())
               .setPrimaryKey(config.getPriIntent().getKey())
               .addAllIntents(intents)
               .setLockTtl(config.getLockTTl())
               .setStrictCheck(true);
  }

  public static Flux<ExecResult> txnPrepare(ShardSender shardSender, StoreCtx storeCtx, RequestContext reqCtx) {

    return shardSender.sendReq(reqCtx).map(response -> (Txn.PrepareResponse) response).flatMap(handlePrepareResp(shardSender, storeCtx, reqCtx));

    // return sender.txnPrepare(reqCtx).flatMap(handlePrepareResp(storeCtx, reqCtx));
  }

  public static Flux<ExecResult> txnPreparePrimary(ShardSender shardSender, StoreCtx storeCtx, TxnConfig config) {
    RequestContext reqCtx = buildPrimaryPrepareReqCxt(config, storeCtx);
    return txnPrepare(shardSender, storeCtx, reqCtx);
  }

  public static Flux<ExecResult> txnPrepareSecondary(ShardSender shardSender, StoreCtx storeCtx, TxnConfig config, List<Txn.TxnIntent> groupList,
                                                     RangeInfo rangeInfo) {
    RequestContext reqCtx = buildSecondaryPrepareReqCxt(config, storeCtx, groupList, rangeInfo);
    return txnPrepare(shardSender, storeCtx, reqCtx);
  }

  public static Function<Txn.PrepareResponse, Flux<ExecResult>> handlePrepareResp(ShardSender shardSender, StoreCtx storeCtx, RequestContext
                                                                                                         reqCtx) {
    return response -> {
      Txn.PrepareRequestOrBuilder reqBuilder = (Txn.PrepareRequestOrBuilder) reqCtx.getReqBuilder();
      if (response.getErrorsCount() == 0) {
        if (LOG.isInfoEnabled()) {
          LOG.info("txn {} prepare success", reqBuilder.getTxnId());
        }
        return Flux.just(AckExecResult.getInstance());
      }
      List<Txn.LockError> expiredTxs = new ArrayList<>();
      for (Txn.TxnError txnError : response.getErrorsList()) {
        switch (txnError.getErrType()) {
          case LOCKED:
            Txn.LockError lockError = txnError.getLockErr();
            if (lockError.getInfo() != null && lockError.getInfo().getTimeout()) {
              expiredTxs.add(lockError);
            } else {
              //retry, wait existed lock timeout on valid context
              BaseException err = ErrorHandler.retrieveException(txnError);
              LOG.error("prepare txn [{}] exist no-timeout lock, need try. error:",
                  reqBuilder.getTxnId(), err);
            }
            break;
          default:
            BaseException err = ErrorHandler.retrieveException(txnError);
            LOG.error("prepare txn [{}] exist other error:", reqBuilder.getTxnId(), err);
            throw err;
        }
      }

      if (!expiredTxs.isEmpty()) {
        return recoverExpiredTxs(shardSender, storeCtx, expiredTxs).flatMap(r -> txnPrepare(shardSender, storeCtx, reqCtx));
      }

      return txnPrepare(shardSender, storeCtx, reqCtx);
    };
  }

  //recover expired transaction
  public static Flux<ExecResult> recoverExpiredTxs(ShardSender shardSender, StoreCtx storeCtx, List<Txn.LockError> lockErrors) {
    Flux<ExecResult> flux = null;
    for (Txn.LockError lockError : lockErrors) {
      Txn.LockInfo lockInfo = lockError.getInfo();
      Flux<Boolean> child;
      if (lockInfo.getIsPrimary()) {
        Txn.DecideRequest.Builder reqBuilder = Txn.DecideRequest.newBuilder()
                                                   .setTxnId(lockInfo.getTxnId()).setStatus(Txn.TxnStatus.ABORTED)
                                                   .addKeys(lockInfo.getPrimaryKey()).setRecover(false)
                                                   .setIsPrimary(true);
        child = Restorer.recoverFromPrimary(shardSender, storeCtx, reqBuilder, lockInfo.getSecondaryKeysList());
      } else {
        child = Restorer.recoverFromSecondary(shardSender, storeCtx, lockInfo.getTxnId(), lockInfo.getPrimaryKey(), Txn.TxnStatus.ABORTED);
      }
      if (flux == null) {
        flux = child.map(flag -> AckExecResult.getInstance());
      } else {
        flux = flux.zipWith(child, (f1, f2) -> f1);
      }
    }
    return flux;
  }

  /**
   * TODO
   * @param <T>
   */

  @FunctionalInterface
  interface PrepareSecondaryFunc<T> {
    Flux<ExecResult> apply(ShardSender shardSender, StoreCtx context, TxnConfig config, List<T> list);
  }

  public static <T> Function<Throwable, Flux> getErrHandler(ShardSender shardSender, StoreCtx storeCtx, PrepareSecondaryFunc<T> func,
                                                            TxnConfig config, List<T> list) {
    return throwable -> getErrHandler(shardSender, storeCtx, func, config, list, throwable);
  }

  public static <T> Flux getErrHandler(ShardSender shardSender, StoreCtx storeCtx, PrepareSecondaryFunc<T> func,
                                       TxnConfig config, List<T> list, Throwable throwable) {
    if (storeCtx.canRetryWithDelay()) {
      if (throwable instanceof BaseException) {
        BaseException exception = (BaseException) throwable;
        if (exception.getCode() == ErrorCode.ER_SHARD_ROUTE_CHANGE) {
          return func.apply(shardSender, storeCtx, config, list);
        } else if (exception.getCode() == ErrorCode.ER_SHARD_NOT_EXIST) {
          //maybe: table no exist, or range no exist
          RangeRouteException routeException = (RangeRouteException) exception;
          if (storeCtx.getRoutingPolicy().rangeExists(routeException)) {
            return func.apply(shardSender, storeCtx, config, list);
          }
          return storeCtx.getRouter().getRoutingFlux(storeCtx.getTable(), routeException.key).flatMap(flag -> func.apply(shardSender, storeCtx, config, list));
        }
      }
    }
    LOG.warn("do on err resume immediate throw err", throwable);
    return Flux.error(throwable);
  }

//  public static <T> Function<Throwable, Flux<T>> getErrHandler(StoreCtx storeCtx, Flux<T> publisher) {
//    return throwable -> {
//      if (storeCtx.canRetryWithDelay()) {
//        if (throwable instanceof JimException) {
//          JimException exception = (JimException) throwable;
//          if (exception.getCode() == ErrorCode.ER_SHARD_ROUTE_CHANGE) {
//            return publisher.onErrorResume(getErrHandler(storeCtx, publisher));
//          }
//          Flux<T> routeException = noShardFlux(storeCtx, publisher, exception);
//          if (routeException != null) {
//            return routeException;
//          }
//        }
//      }
//      LOG.warn("do on err resume immediate throw err:", throwable);
//      return Flux.error(throwable);
//    };
//  }

  /**
   * TODO
   */

  @FunctionalInterface
  interface PreparePrimaryFunc {
    Flux<ExecResult> apply(ShardSender shardSender, StoreCtx context, TxnConfig config);
  }

  public static Function<Throwable, Flux<ExecResult>> getErrHandler(ShardSender shardSender, StoreCtx context, PreparePrimaryFunc func,
                                                                    TxnConfig config) {
    return throwable -> {
      if (context.canRetryWithDelay()) {
        if (throwable instanceof BaseException) {
          BaseException exception = (BaseException) throwable;
          if (exception.getCode() == ErrorCode.ER_SHARD_ROUTE_CHANGE) {
            return func.apply(shardSender, context, config).onErrorResume(getErrHandler(shardSender, context, func, config));
          } else if (exception.getCode() == ErrorCode.ER_SHARD_NOT_EXIST) {
            RangeRouteException routeException = (RangeRouteException) exception;
            if (context.getRoutingPolicy().rangeExists(routeException)) {
              return func.apply(shardSender, context, config).onErrorResume(getErrHandler(shardSender, context, func, config));
            }
            return context.getRouter().getRoutingFlux(context.getTable(), routeException.key).flatMap(flag -> func.apply(shardSender, context, config)
                                                                                 .onErrorResume(getErrHandler(shardSender, context, func, config)));
          }
        }
      }
      LOG.warn("do on err resume immediate throw err:", throwable);
      return Flux.error(throwable);
    };
  }
}
