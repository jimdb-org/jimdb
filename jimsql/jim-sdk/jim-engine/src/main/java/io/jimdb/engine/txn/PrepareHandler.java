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
import io.jimdb.engine.StoreCtx;
import io.jimdb.engine.client.RequestContext;
import io.jimdb.engine.sender.DispatcherImpl;
import io.jimdb.engine.sender.Util;
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
public class PrepareHandler extends TxnHandler {

  private static final Logger LOG = LoggerFactory.getLogger(PrepareHandler.class);

  // PREPARE_PRIMARY_FUNC is used in Txn1PLHandler and Txn2PLHandler
  static final PreparePrimaryFunc PREPARE_PRIMARY_FUNC = PrepareHandler::txnPreparePrimary;

  public static RequestContext buildPrimaryPrepareReqCxt(TxnConfig config, StoreCtx storeCtx) {
    Txn.PrepareRequest.Builder primaryReq = Util.buildPrepare4Primary(config);
    return new RequestContext(storeCtx, primaryReq.getIntents(0).getKey(), primaryReq, Api.RangeRequest.ReqCase.PREPARE);
  }

  public static RequestContext buildSecondaryPrepareReqCxt(TxnConfig config, StoreCtx storeCtx, List<Txn.TxnIntent>
                                                                                                    intentList, RangeInfo rangeInfo) {
    Txn.PrepareRequest.Builder secondaryReq = Util.buildPrepare4Secondary(config, intentList);
    return new RequestContext(storeCtx, secondaryReq.getIntents(0).getKey(), rangeInfo, secondaryReq, Api.RangeRequest.ReqCase.PREPARE);
  }

  public static Flux<ExecResult> txnPrepare(StoreCtx storeCtx, RequestContext reqCtx) {
    DispatcherImpl sender = storeCtx.getSender();
    return sender.txnPrepare(reqCtx).flatMap(handlePrepareResp(storeCtx, reqCtx));
  }

  public static Flux<ExecResult> txnPreparePrimary(StoreCtx storeCtx, TxnConfig config) {
    RequestContext reqCtx = buildPrimaryPrepareReqCxt(config, storeCtx);
    return txnPrepare(storeCtx, reqCtx);
  }

  public static Flux<ExecResult> txnPrepareSecondary(StoreCtx storeCtx, TxnConfig config, List<Txn.TxnIntent> groupList,
                                                     RangeInfo rangeInfo) {
    RequestContext reqCtx = buildSecondaryPrepareReqCxt(config, storeCtx, groupList, rangeInfo);
    return txnPrepare(storeCtx, reqCtx);
  }

  public static Function<Txn.PrepareResponse, Flux<ExecResult>> handlePrepareResp(StoreCtx storeCtx, RequestContext
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
              BaseException err = convertTxnErr(txnError);
              LOG.error("prepare txn [{}] exist no-timeout lock, need try. error:",
                  reqBuilder.getTxnId(), err);
            }
            break;
          default:
            BaseException err = convertTxnErr(txnError);
            LOG.error("prepare txn [{}] exist other error:", reqBuilder.getTxnId(), err);
            throw err;
        }
      }

      if (!expiredTxs.isEmpty()) {
        return recoverExpiredTxs(storeCtx, expiredTxs).flatMap(r -> txnPrepare(storeCtx, reqCtx));
      }

      return txnPrepare(storeCtx, reqCtx);
    };
  }

  //recover expired transaction
  public static Flux<ExecResult> recoverExpiredTxs(StoreCtx storeCtx, List<Txn.LockError> lockErrors) {
    Flux<ExecResult> flux = null;
    for (Txn.LockError lockError : lockErrors) {
      Txn.LockInfo lockInfo = lockError.getInfo();
      Flux<Boolean> child;
      if (lockInfo.getIsPrimary()) {
        Txn.DecideRequest.Builder reqBuilder = Txn.DecideRequest.newBuilder()
                                                   .setTxnId(lockInfo.getTxnId()).setStatus(Txn.TxnStatus.ABORTED)
                                                   .addKeys(lockInfo.getPrimaryKey()).setRecover(false)
                                                   .setIsPrimary(true);
        child = RecoverHandler.recoverFromPrimary(storeCtx, reqBuilder, lockInfo.getSecondaryKeysList());
      } else {
        child = RecoverHandler.recoverFromSecondary(storeCtx, lockInfo.getTxnId(), lockInfo.getPrimaryKey(), Txn.TxnStatus.ABORTED);
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
    Flux<ExecResult> apply(StoreCtx context, TxnConfig config, List<T> list);
  }

  public static <T> Function<Throwable, Flux> getErrHandler(StoreCtx storeCtx, PrepareSecondaryFunc<T> func,
                                                            TxnConfig config, List<T> list) {
    return throwable -> getErrHandler(storeCtx, func, config, list, throwable);
  }

  public static <T> Flux getErrHandler(StoreCtx storeCtx, PrepareSecondaryFunc<T> func,
                                       TxnConfig config, List<T> list, Throwable throwable) {
    if (storeCtx.canRetryWithDelay()) {
      if (throwable instanceof BaseException) {
        BaseException exception = (BaseException) throwable;
        if (exception.getCode() == ErrorCode.ER_SHARD_ROUTE_CHANGE) {
          return func.apply(storeCtx, config, list);
        } else if (exception.getCode() == ErrorCode.ER_SHARD_NOT_EXIST) {
          //maybe: table no exist, or range no exist
          RangeRouteException routeException = (RangeRouteException) exception;
          if (existRange(storeCtx, routeException)) {
            return func.apply(storeCtx, config, list);
          }
          return getRouteFlux(storeCtx, routeException.key).flatMap(flag -> func.apply(storeCtx, config, list));
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
    Flux<ExecResult> apply(StoreCtx context, TxnConfig config);
  }

  public static Function<Throwable, Flux<ExecResult>> getErrHandler(StoreCtx context, PreparePrimaryFunc func,
                                                                    TxnConfig config) {
    return throwable -> {
      if (context.canRetryWithDelay()) {
        if (throwable instanceof BaseException) {
          BaseException exception = (BaseException) throwable;
          if (exception.getCode() == ErrorCode.ER_SHARD_ROUTE_CHANGE) {
            return func.apply(context, config).onErrorResume(getErrHandler(context, func, config));
          } else if (exception.getCode() == ErrorCode.ER_SHARD_NOT_EXIST) {
            RangeRouteException routeException = (RangeRouteException) exception;
            if (existRange(context, routeException)) {
              return func.apply(context, config).onErrorResume(getErrHandler(context, func, config));
            }
            return getRouteFlux(context, routeException.key).flatMap(flag -> func.apply(context, config)
                                                                                 .onErrorResume(getErrHandler(context, func, config)));
          }
        }
      }
      LOG.warn("do on err resume immediate throw err:", throwable);
      return Flux.error(throwable);
    };
  }
}
