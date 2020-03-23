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
import static io.jimdb.engine.txn.DecisionMaker.buildTxnDecide4Primary;

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

import com.google.protobuf.ByteString;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

/**
 * TODO
 */
@SuppressFBWarnings()
public class Restorer extends RequestHandler {

  private static final Logger LOG = LoggerFactory.getLogger(Restorer.class);

  private static final RecoverFunc RECOVER_FUNC = Restorer::recover;

  public static void recoverFromPrimary(ShardSender shardSender, FluxSink<ExecResult> sink, TxnConfig config, StoreCtx context) {
    RECOVER_FUNC.apply(shardSender, context, config).onErrorResume(
        Restorer.getErrHandler(shardSender, context, RECOVER_FUNC, config)).subscribe(
        new TransactionImpl.CommitSubscriber<>(rs -> sink.next(rs), err -> sink.error(err)));
  }

  private static Flux<ExecResult> recover(ShardSender shardSender, StoreCtx context, TxnConfig config) {
    Txn.DecideRequest.Builder reqBuilder = buildTxnDecide4Primary(config, Txn.TxnStatus.ABORTED);
    return Restorer.recoverFromPrimary(shardSender, context, reqBuilder, config.getSecKeys()).map(f -> AckExecResult.getInstance
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
  public static Flux<Boolean> recoverFromPrimary(ShardSender shardSender, StoreCtx storeCtx, Txn.DecideRequest.Builder reqBuilder,
                                                 List<ByteString> secondaryKeys) {
    //first try to decide expired tx to aborted status
    RangeInfo rangeInfo =  storeCtx.getRoutingPolicy().getRangeInfoByKey(reqBuilder.getKeys(0).toByteArray());
    RequestContext reqCtx = new RequestContext(storeCtx, reqBuilder.getKeys(0), rangeInfo, reqBuilder, Api.RangeRequest.ReqCase.DECIDE);
    return shardSender.sendReq(reqCtx).map(response -> (Txn.DecideResponse) response).flatMap(response -> {
      Txn.TxnStatus applyStatus = Txn.TxnStatus.ABORTED;
      if (response.hasErr()) {
        switch (response.getErr().getErrType()) {
          case STATUS_CONFLICT:
            applyStatus = Txn.TxnStatus.COMMITTED;
            LOG.warn("rollback txn {} error, because ds let commit", reqBuilder.getTxnId());
            break;
          case NOT_FOUND:
          case TXN_CONFLICT:
            BaseException exception = retrieveException(response.getErr());
            LOG.warn("recover: retry to aborted txn{} primary intent and return err:{}, ignore err", reqBuilder
                                                                                                         .getTxnId(), exception.getMessage());
            return Flux.just(Boolean.TRUE);
          default:
            BaseException err = retrieveException(response.getErr());
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

      RangeInfo cleanupRangeInfo =  storeCtx.getRoutingPolicy().getRangeInfoByKey(clearUpRequest.getPrimaryKey().toByteArray());
      RequestContext cleanupCxt = new RequestContext(storeCtx, clearUpRequest.getPrimaryKey(), cleanupRangeInfo, clearUpRequest,
          Api.RangeRequest.ReqCase.CLEAR_UP);
      Flux<Boolean> cleanupFlux = Cleaner.cleanup(shardSender, cleanupCxt).map(r -> Boolean.TRUE);

      if (recoverSecondaryKeys != null && !recoverSecondaryKeys.isEmpty()) {
        return DecisionMaker.decideSecondary(shardSender, storeCtx, reqBuilder.getTxnId(), recoverSecondaryKeys, applyStatus)
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

  public static Flux<Boolean> recoverFromSecondary(ShardSender shardSender, StoreCtx context, String txnId, ByteString primaryKey, Txn
                                                                                                              .TxnStatus status) {

    Txn.DecideRequest.Builder reqBuilder = Txn.DecideRequest.newBuilder()
                                               .setTxnId(txnId)
                                               .addKeys(primaryKey).setIsPrimary(true)
                                               .setRecover(true).setStatus(status);
    return recoverFromPrimary(shardSender, context, reqBuilder, null);
  }


  //todo opt
  public static Flux<Txn.TxnStatus> recoverFromSecondaryAsync(ShardSender shardSender, StoreCtx storeCtx, String txnId, ByteString primaryKey) {

    Txn.DecideRequest.Builder reqBuilder = Txn.DecideRequest.newBuilder()
                                               .setTxnId(txnId)
                                               .addKeys(primaryKey).setIsPrimary(true)
                                               .setRecover(true).setStatus(Txn.TxnStatus.ABORTED);

    RangeInfo cleanupRangeInfo =  storeCtx.getRoutingPolicy().getRangeInfoByKey(reqBuilder.getKeys(0).toByteArray());
    RequestContext context = new RequestContext(storeCtx, reqBuilder.getKeys(0), cleanupRangeInfo, reqBuilder, Api.RangeRequest.ReqCase.DECIDE);
    return shardSender.sendReq(context).map(response -> (Txn.DecideResponse) response).map(response -> {
      Txn.TxnStatus applyStatus = Txn.TxnStatus.ABORTED;
      if (response.hasErr()) {
        switch (response.getErr().getErrType()) {
          case STATUS_CONFLICT:
            LOG.warn("rollback txn {} error, because ds let commit", reqBuilder.getTxnId());
            applyStatus = Txn.TxnStatus.COMMITTED;
            break;
          case NOT_FOUND:
          case TXN_CONFLICT:
            BaseException exception = ErrorHandler.retrieveException(response.getErr());
            LOG.warn("recover: retry to aborted txn{} primary intent and return err:{}, ignore err", reqBuilder
                                                                                                         .getTxnId(), exception.getMessage());
            return Txn.TxnStatus.TXN_INIT;
          default:
            BaseException err = ErrorHandler.retrieveException(response.getErr());
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
    Flux<ExecResult> apply(ShardSender shardSender, StoreCtx context, TxnConfig config);
  }

  public static Function<Throwable, Flux<ExecResult>> getErrHandler(ShardSender shardSender, StoreCtx context, RecoverFunc func, TxnConfig config) {
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
