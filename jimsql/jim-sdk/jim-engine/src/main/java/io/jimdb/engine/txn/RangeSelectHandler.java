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
import io.jimdb.core.codec.KvPair;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.engine.StoreCtx;
import io.jimdb.engine.sender.DispatcherImpl;
import io.jimdb.pb.Txn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.MessageOrBuilder;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.core.publisher.Flux;

/**
 * TODO
 */
@SuppressFBWarnings()
public class RangeSelectHandler extends TxnHandler {
  private static final Logger LOG = LoggerFactory.getLogger(RangeSelectHandler.class);

  static final RangeSelectFunc RANGE_SELECT_FUNC = RangeSelectHandler::rangeSelect;
  static final SingleRangeSelectFunc SINGLE_RANGE_SELECT_FUNC = RangeSelectHandler::singleRangeSelect;

  /**
   * Get rows based on given range
   * @param storeCtx TODO
   * @param reqBuilder TODO
   * @param resultColumns TODO
   * @param kvPair TODO
   * @return TODO
   */
  public static Flux<ValueAccessor[]> rangeSelect(StoreCtx storeCtx, MessageOrBuilder reqBuilder,
                                                  ColumnExpr[] resultColumns, KvPair kvPair) {
    DispatcherImpl sender = storeCtx.getSender();
    return sender.txnRangeSelect(storeCtx, reqBuilder, kvPair)
               .map(rows -> handleFlowValue(resultColumns, rows, storeCtx));
  }

  // SINGLE_RANGE_SELECT_FUNC
  protected static Flux<ValueAccessor[]> singleRangeSelect(StoreCtx storeCtx, Txn.SelectFlowRequest.Builder reqBuilder,
                                                           ColumnExpr[] resultColumns, KvPair kvPair) {
    DispatcherImpl sender = storeCtx.getSender();
    return sender.txnSingleRangeSelect(storeCtx, reqBuilder, kvPair)
               .map(rows -> handleFlowValue(resultColumns, rows, storeCtx));
  }

  /**
   * TODO
   */
  @FunctionalInterface
  interface RangeSelectFunc {
    Flux<ValueAccessor[]> apply(StoreCtx context, Txn.SelectFlowRequest.Builder reqBuilder, ColumnExpr[] exprs,
                                KvPair kvPair);
  }

  public static Function<Throwable, Flux<ValueAccessor[]>> getErrHandler(StoreCtx context, RangeSelectFunc func,
                                                                         Txn.SelectFlowRequest.Builder reqBuilder,
                                                                         ColumnExpr[] exprs, KvPair kvPair) {
    return throwable -> {
      if (context.canRetryWithDelay()) {
        if (throwable instanceof BaseException) {
          BaseException exception = (BaseException) throwable;
          if (exception.getCode() == ErrorCode.ER_SHARD_ROUTE_CHANGE) {
            return func.apply(context, reqBuilder, exprs, kvPair).onErrorResume(getErrHandler(context, func,
                reqBuilder, exprs, kvPair));
          } else if (exception.getCode() == ErrorCode.ER_SHARD_NOT_EXIST) {
            RangeRouteException routeException = (RangeRouteException) exception;
            if (existRange(context, routeException)) {
              return func.apply(context, reqBuilder, exprs, kvPair).onErrorResume(getErrHandler(context, func,
                  reqBuilder, exprs, kvPair));
            }
            return getRouteFlux(context, routeException.key).flatMap(flag -> func.apply(context, reqBuilder, exprs,
                kvPair).onErrorResume(getErrHandler(context, func, reqBuilder, exprs, kvPair)));
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
  interface SingleRangeSelectFunc {
    Flux<ValueAccessor[]> apply(StoreCtx context, Txn.SelectFlowRequest.Builder reqBuilder, ColumnExpr[] exprs, KvPair
                                                                                                                    kvPair);
  }

  public static Function<Throwable, Flux<ValueAccessor[]>> getErrHandler(StoreCtx context, SingleRangeSelectFunc func,
                                                                         Txn.SelectFlowRequest.Builder reqBuilder,
                                                                         ColumnExpr[] exprs, KvPair kvPair) {
    return throwable -> {
      if (context.canRetryWithDelay()) {
        if (throwable instanceof BaseException) {
          BaseException exception = (BaseException) throwable;
          if (exception.getCode() == ErrorCode.ER_SHARD_ROUTE_CHANGE) {
            return func.apply(context, reqBuilder, exprs, kvPair).onErrorResume(getErrHandler(context, func,
                reqBuilder, exprs, kvPair));
          } else if (exception.getCode() == ErrorCode.ER_SHARD_NOT_EXIST) {
            RangeRouteException routeException = (RangeRouteException) exception;
            if (existRange(context, routeException)) {
              return func.apply(context, reqBuilder, exprs, kvPair).onErrorResume(getErrHandler(context, func,
                  reqBuilder, exprs, kvPair));
            }
            return getRouteFlux(context, routeException.key).flatMap(flag -> func.apply(context, reqBuilder, exprs,
                kvPair).onErrorResume(getErrHandler(context, func, reqBuilder, exprs, kvPair)));
          }
        }
      }
      LOG.warn("do on err resume immediate throw err:", throwable);
      return Flux.error(throwable);
    };
  }
}
