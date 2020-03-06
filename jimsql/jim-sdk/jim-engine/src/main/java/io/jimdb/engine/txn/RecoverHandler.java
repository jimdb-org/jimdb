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

import java.util.List;
import java.util.function.Function;

import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.BaseException;
import io.jimdb.common.exception.RangeRouteException;
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

import com.google.protobuf.ByteString;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

/**
 * TODO
 */
@SuppressFBWarnings()
public class RecoverHandler extends TxnHandler {

  private static final Logger LOG = LoggerFactory.getLogger(RecoverHandler.class);

  private static final RecoverFunc RECOVER_FUNC = RecoverHandler::recover;

  public static void recoverFromPrimary(FluxSink<ExecResult> sink, TxnConfig config, StoreCtx context) {
    RECOVER_FUNC.apply(context, config).onErrorResume(
        RecoverHandler.getErrHandler(context, RECOVER_FUNC, config)).subscribe(
        new TxnHandler.CommitSubscriber<>(rs -> sink.next(rs), err -> sink.error(err)));
  }

  private static Flux<ExecResult> recover(StoreCtx context, TxnConfig config) {
    Txn.DecideRequest.Builder reqBuilder = Util.buildTxnDecide4Primary(config, Txn.TxnStatus.ABORTED);
    return RecoverHandler.recoverFromPrimary(context, reqBuilder, config.getSecKeys()).map(f -> AckExecResult.getInstance
                                                                                                              ());
  }

  /**
   * try to rollback and flag aborted status
   * rollback secondary intents
   * rollback primary intents
   *
   * @param storeCtx store context
   * @param reqBuilder request builder
   * @param secondaryKeys secondary keys
   * @return flux of the response for the prepare request
   */
  public static Flux<Boolean> recoverFromPrimary(StoreCtx storeCtx, Txn.DecideRequest.Builder reqBuilder,
                                                 List<ByteString> secondaryKeys) {
    DispatcherImpl sender = storeCtx.getSender();
    //first try to decide expired tx to aborted status
    RequestContext reqCtx = new RequestContext(storeCtx, reqBuilder.getKeys(0), reqBuilder, Api.RangeRequest.ReqCase.DECIDE);
    return sender.txnDecide(reqCtx).flatMap(response -> {
      Txn.TxnStatus applyStatus = Txn.TxnStatus.ABORTED;
      if (response.hasErr()) {
        switch (response.getErr().getErrType()) {
          case STATUS_CONFLICT:
            applyStatus = Txn.TxnStatus.COMMITTED;
            LOG.warn("rollback txn {} error, because ds let commit", reqBuilder.getTxnId());
            break;
          case NOT_FOUND:
          case TXN_CONFLICT:
            BaseException exception = convertTxnErr(response.getErr());
            LOG.warn("recover: retry to aborted txn{} primary intent and return err:{}, ignore err", reqBuilder
                                                                                                         .getTxnId(), exception.getMessage());
            return Flux.just(Boolean.TRUE);
          default:
            BaseException err = convertTxnErr(response.getErr());
            LOG.error("recover txn {} error:{}", reqBuilder.getTxnId(), err.getMessage());
            throw err;
        }
      }
      List<ByteString> recoverSecondaryKeys = secondaryKeys;
      if (reqBuilder.getRecover()) {
        recoverSecondaryKeys = response.getSecondaryKeysList();
      }

      Txn.ClearupRequest.Builder clearUpRequest = Txn.ClearupRequest.newBuilder()
                                                      .setTxnId(reqBuilder.getTxnId()).setPrimaryKey(reqBuilder.getKeys(0));
      RequestContext cleanupCxt = new RequestContext(storeCtx, clearUpRequest.getPrimaryKey(), clearUpRequest,
          Api.RangeRequest.ReqCase.CLEAR_UP);
      Flux<Boolean> cleanupFlux = CleanupHandler.cleanup(cleanupCxt, sender).map(r -> Boolean.TRUE);

      if (recoverSecondaryKeys != null && !recoverSecondaryKeys.isEmpty()) {
        return DecideHandler.decideSecondary(storeCtx, reqBuilder.getTxnId(), recoverSecondaryKeys, applyStatus)
                   .flatMap(flag -> {
                     if (!flag) {
                       return Flux.just(flag);
                     }
                     return cleanupFlux;
                   });
      } else {
        return cleanupFlux;
      }
    });
  }

  public static Flux<Boolean> recoverFromSecondary(StoreCtx context, String txnId, ByteString primaryKey, Txn
                                                                                                              .TxnStatus status) {

    Txn.DecideRequest.Builder reqBuilder = Txn.DecideRequest.newBuilder()
                                               .setTxnId(txnId)
                                               .addKeys(primaryKey).setIsPrimary(true)
                                               .setRecover(true).setStatus(status);
    return recoverFromPrimary(context, reqBuilder, null);
  }


  //todo opt
  public static Flux<Txn.TxnStatus> recoverFromSecondaryAsync(StoreCtx storeCtx, String txnId, ByteString primaryKey) {

    Txn.DecideRequest.Builder reqBuilder = Txn.DecideRequest.newBuilder()
                                               .setTxnId(txnId)
                                               .addKeys(primaryKey).setIsPrimary(true)
                                               .setRecover(true).setStatus(Txn.TxnStatus.ABORTED);

    RequestContext context = new RequestContext(storeCtx, reqBuilder.getKeys(0), reqBuilder, Api.RangeRequest.ReqCase.DECIDE);
    DispatcherImpl sender = storeCtx.getSender();
    return sender.txnDecide(context).map(response -> {
      Txn.TxnStatus applyStatus = Txn.TxnStatus.ABORTED;
      if (response.hasErr()) {
        switch (response.getErr().getErrType()) {
          case STATUS_CONFLICT:
            LOG.warn("rollback txn {} error, because ds let commit", reqBuilder.getTxnId());
            applyStatus = Txn.TxnStatus.COMMITTED;
            break;
          case NOT_FOUND:
          case TXN_CONFLICT:
            BaseException exception = convertTxnErr(response.getErr());
            LOG.warn("recover: retry to aborted txn{} primary intent and return err:{}, ignore err", reqBuilder
                                                                                                         .getTxnId(), exception.getMessage());
            return Txn.TxnStatus.TXN_INIT;
          default:
            BaseException err = convertTxnErr(response.getErr());
            LOG.error("recover txn {} error:{}", reqBuilder.getTxnId(), err.getMessage());
            throw err;
        }
      }

      //todo async
//              List<ByteString> recoverSecondaryKeys = response.getSecondaryKeysList();
//              if (recoverSecondaryKeys != null && recoverSecondaryKeys.size() > 0) {
//                decideSecondary(sender, routePolicy, reqBuilder.getTxnId(),
//                        recoverSecondaryKeys, applyStatus, Instant.now().plusMillis(200))
//                        .flatMap(r -> clearUp(sender, routePolicy, reqBuilder.getTxnId(),
//                                reqBuilder.getKeys(0), timeout));
//              }
//              clearUp(sender, routePolicy, reqBuilder.getTxnId(),
//                      reqBuilder.getKeys(0), timeout);

      return applyStatus;
    });
  }

  /**
   * TODO
   */

  @FunctionalInterface
  interface RecoverFunc {
    Flux<ExecResult> apply(StoreCtx context, TxnConfig config);
  }

  public static Function<Throwable, Flux<ExecResult>> getErrHandler(StoreCtx context, RecoverFunc func, TxnConfig config) {
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
