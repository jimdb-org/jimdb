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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.BaseException;
import io.jimdb.common.exception.RangeRouteException;
import io.jimdb.common.utils.lang.ByteUtil;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.model.meta.RangeInfo;
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
public class MultiKeySelector { //extends TransactionImpl {
  private static final Logger LOG = LoggerFactory.getLogger(MultiKeySelector.class);
  static final MultiKeySelectFunc MULTI_KEY_SELECT_FUNC = MultiKeySelector::multiKeySelect;

  /**
   * Get rows based on given indices or keys
   * @param storeCtx  TODO
   * @param reqBuilder TODO
   * @param resultColumns TODO
   * @param keys TODO
   * @return TODO
   */
  public static Flux<ValueAccessor[]> multiKeySelect(ShardSender shardSender, StoreCtx storeCtx, MessageOrBuilder reqBuilder,
                                                     ColumnExpr[] resultColumns, List<ByteString> keys) {
    return innerMultiKeySelect(shardSender, storeCtx, SEND_SELECT_REQ_FUNC, reqBuilder, keys)
               .map(rows -> convertRowsToValueAccessors(resultColumns, rows, storeCtx));
  }

  private static Flux<List<Txn.Row>> innerMultiKeySelect(ShardSender shardSender, StoreCtx storeCtx, RequestHandler.RequestFunc func,
                                                       MessageOrBuilder reqBuilder, List<ByteString> keys) {
    Flux<List<Txn.Row>> flux = null;
    Collections.sort(keys, (k1, k2) -> ByteUtil.compare(k1, k2));

    Map<RangeInfo, List<ByteString>> keyGroupMap;
    try {
      keyGroupMap = storeCtx.getRoutingPolicy().regroupByRoute(keys, ByteString::toByteArray);
    } catch (Throwable e) {
      return Flux.error(e);
    }
    for (Map.Entry<RangeInfo, List<ByteString>> entry : keyGroupMap.entrySet()) {
      List<ByteString> keyList = entry.getValue();
      if (keyList == null || keyList.isEmpty()) {
        continue;
      }
      Flux<List<Txn.Row>> selectFlux = func.apply(shardSender, storeCtx, reqBuilder, keyList.get(0), entry.getKey());
      if (flux == null) {
        flux = selectFlux;
      } else {
        flux = flux.zipWith(selectFlux, (f1, f2) -> getRows(f1, f2));
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


  /**
   * TODO
   */
  @FunctionalInterface
  interface MultiKeySelectFunc {
    Flux<ValueAccessor[]> apply(ShardSender shardSender, StoreCtx context, Txn.SelectFlowRequest.Builder reqBuilder, ColumnExpr[] exprs,
                                List<ByteString> keys);
  }


  public static Function<Throwable, Flux<ValueAccessor[]>> getErrHandler(ShardSender shardSender, StoreCtx context, MultiKeySelectFunc func,
                                                                         Txn.SelectFlowRequest.Builder reqBuilder,
                                                                         ColumnExpr[] exprs, List<ByteString> keys) {
    return throwable -> {
      if (context.canRetryWithDelay()) {
        if (throwable instanceof BaseException) {
          BaseException exception = (BaseException) throwable;
          if (exception.getCode() == ErrorCode.ER_SHARD_ROUTE_CHANGE) {
            return func.apply(shardSender, context, reqBuilder, exprs, keys).onErrorResume(getErrHandler(shardSender, context, func,
                reqBuilder, exprs, keys));
          } else if (exception.getCode() == ErrorCode.ER_SHARD_NOT_EXIST) {
            RangeRouteException routeException = (RangeRouteException) exception;
            if (context.getRoutingPolicy().rangeExists(routeException)) {
              return func.apply(shardSender, context, reqBuilder, exprs, keys).onErrorResume(getErrHandler(shardSender, context, func,
                  reqBuilder, exprs, keys));
            }
            return context.getRouter().getRoutingFlux(context.getTable(), routeException.key).flatMap(flag -> func.apply(shardSender, context, reqBuilder, exprs,
                keys)
                                                                                 .onErrorResume(getErrHandler(shardSender, context, func, reqBuilder, exprs, keys)));
          }
        }
      }
      LOG.warn("do on err resume immediate throw err:", throwable);
      return Flux.error(throwable);
    };
  }

}
