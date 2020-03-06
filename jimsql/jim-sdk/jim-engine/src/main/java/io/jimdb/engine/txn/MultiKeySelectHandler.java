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
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.engine.StoreCtx;
import io.jimdb.engine.sender.DispatcherImpl;
import io.jimdb.pb.Txn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.MessageOrBuilder;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.core.publisher.Flux;

/**
 * TODO
 */
@SuppressFBWarnings()
public class MultiKeySelectHandler extends TxnHandler {
  private static final Logger LOG = LoggerFactory.getLogger(MultiKeySelectHandler.class);
  static final MultiKeySelectFunc MULTI_KEY_SELECT_FUNC = MultiKeySelectHandler::multiKeySelect;

  /**
   * Get rows based on given indices or keys
   * @param storeCtx  TODO
   * @param reqBuilder TODO
   * @param resultColumns TODO
   * @param keys TODO
   * @return TODO
   */
  public static Flux<ValueAccessor[]> multiKeySelect(StoreCtx storeCtx, MessageOrBuilder reqBuilder,
                                                     ColumnExpr[] resultColumns, List<ByteString> keys) {
    DispatcherImpl sender = storeCtx.getSender();
    return sender.txnMultiKeySelect(storeCtx, reqBuilder, keys)
               .map(rows -> handleFlowValue(resultColumns, rows, storeCtx));
  }

  /**
   * TODO
   */
  @FunctionalInterface
  interface MultiKeySelectFunc {
    Flux<ValueAccessor[]> apply(StoreCtx context, Txn.SelectFlowRequest.Builder reqBuilder, ColumnExpr[] exprs,
                                List<ByteString> keys);
  }


  public static Function<Throwable, Flux<ValueAccessor[]>> getErrHandler(StoreCtx context, MultiKeySelectFunc func,
                                                                         Txn.SelectFlowRequest.Builder reqBuilder,
                                                                         ColumnExpr[] exprs, List<ByteString> keys) {
    return throwable -> {
      if (context.canRetryWithDelay()) {
        if (throwable instanceof BaseException) {
          BaseException exception = (BaseException) throwable;
          if (exception.getCode() == ErrorCode.ER_SHARD_ROUTE_CHANGE) {
            return func.apply(context, reqBuilder, exprs, keys).onErrorResume(getErrHandler(context, func,
                reqBuilder, exprs, keys));
          } else if (exception.getCode() == ErrorCode.ER_SHARD_NOT_EXIST) {
            RangeRouteException routeException = (RangeRouteException) exception;
            if (existRange(context, routeException)) {
              return func.apply(context, reqBuilder, exprs, keys).onErrorResume(getErrHandler(context, func,
                  reqBuilder, exprs, keys));
            }
            return getRouteFlux(context, routeException.key).flatMap(flag -> func.apply(context, reqBuilder, exprs,
                keys)
                                                                                 .onErrorResume(getErrHandler(context, func, reqBuilder, exprs, keys)));
          }
        }
      }
      LOG.warn("do on err resume immediate throw err:", throwable);
      return Flux.error(throwable);
    };
  }
}
