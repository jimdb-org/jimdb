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

import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.exception.BaseException;
import io.jimdb.common.exception.RangeRouteException;
import io.jimdb.core.model.meta.Index;
import io.jimdb.engine.StoreCtx;
import io.jimdb.engine.sender.DispatcherImpl;
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
public class ScanHandler extends TxnHandler {
  private static final Logger LOG = LoggerFactory.getLogger(ScanHandler.class);

  private static final List<Txn.KeyValue> KET_VALUE_EMPTY_LIST_INSTANCE = new ArrayList<>();

  /**
   * TODO
   * @param storeCtx TODO
   * @param request TODO
   * @return TODO
   */
  public static Flux<List<Txn.KeyValue>> txnScan(StoreCtx storeCtx, Txn.ScanRequest.Builder request) {
    DispatcherImpl sender = storeCtx.getSender();
    return sender.txnScan(storeCtx, request).flatMap(keyValues -> {
      if (keyValues == null || keyValues.isEmpty()) {
        LOG.warn("scan: context {} txnScan {} is empty ", storeCtx.getCxtId(), request);
        return Flux.just(KET_VALUE_EMPTY_LIST_INSTANCE);
      }

      List<Txn.KeyValue> kvs = new ArrayList<>(keyValues.size());

      Flux<List<Txn.KeyValue>> flux = null;
      for (int i = 0; i < keyValues.size(); i++) {
        Txn.KeyValue keyValue = keyValues.get(i);
        if (!keyValue.hasIntent()) {
          kvs.add(i, keyValue);
          continue;
        }

        kvs.add(i, null);
        final int j = i;
        Flux<List<Txn.KeyValue>> scanRowFlux = handleScanRow(storeCtx, request, keyValue).map(r -> {
          if (r == ByteString.EMPTY) {
            LOG.warn("scan: context {} handle intent {} is empty ", storeCtx.getCxtId(), keyValue);
            kvs.set(j, null);
          } else {
            Txn.KeyValue temp = Txn.KeyValue.newBuilder().setKey(keyValue.getKey()).setValue(r).build();
            kvs.set(j, temp);
          }
          return kvs;
        });
        if (flux == null) {
          flux = scanRowFlux;
        } else {
          flux = flux.zipWith(scanRowFlux, (f1, f2) -> f1);
        }
      }

      if (flux == null) {
        flux = Flux.just(kvs);
      }
      return flux;
    });
  }

  private static Flux<ByteString> handleScanRow(StoreCtx storeCtx, Txn.ScanRequest.Builder reqBuilder, Txn.KeyValue
                                                                                                          keyValue) {
    Txn.ValueIntent valueIntent = keyValue.getIntent();
    DispatcherImpl sender = storeCtx.getSender();
    return getTxnStatusFlux(storeCtx, valueIntent.getTxnId(), valueIntent.getPrimaryKey(), valueIntent.getTimeout())
               .flatMap(status -> {
                 if (LOG.isInfoEnabled()) {
                   LOG.info("scan: confirm txn[{}] status:[{}] ", valueIntent.getTxnId(), status);
                 }
                 ByteString value = null;
                 switch (status.getNumber()) {
                   //aborted
                   case Txn.TxnStatus.ABORTED_VALUE:
                     value = keyValue.getValue();
                     break;
                   case Txn.TxnStatus.COMMITTED_VALUE:
                     switch (valueIntent.getOpTypeValue()) {
                       case Txn.OpType.INSERT_VALUE:
                         value = valueIntent.getValue();
                         break;
                       default:
                         break;
                     }
                     break;
                   default:
                     //retry read
                     return sender.txnScan(storeCtx, reqBuilder).map(kvs -> {
                       if (!kvs.isEmpty() && kvs.size() > 1) {
                         throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_TXN_ROW_SIZE, "1", String.valueOf(kvs
                                                                                                                      .size()));
                       }
                       if (!kvs.isEmpty() && kvs.size() == 1 && kvs.get(0).getValue() != null) {
                         return kvs.get(0).getValue();
                       }
                       return ByteString.EMPTY;
                     });
                 }
                 if (value == null) {
                   value = ByteString.EMPTY;
                 }
                 return Flux.just(value);
               });
  }

  /**
   * TODO
   */
  @FunctionalInterface
  interface ScanPKFunc {
    Flux<List<ByteString>> apply(StoreCtx storeCtx, Txn.ScanRequest.Builder request, Index index);
  }

  public static Function<Throwable, Flux<List<ByteString>>> getErrHandler(StoreCtx context, ScanPKFunc func,
                                                                          Txn.ScanRequest.Builder reqBuilder, Index
                                                                                                                  index) {
    return throwable -> {
      if (context.canRetryWithDelay()) {
        if (throwable instanceof BaseException) {
          BaseException exception = (BaseException) throwable;
          if (exception.getCode() == ErrorCode.ER_SHARD_ROUTE_CHANGE) {
            return func.apply(context, reqBuilder, index).onErrorResume(getErrHandler(context, func, reqBuilder,
                index));
          } else if (exception.getCode() == ErrorCode.ER_SHARD_NOT_EXIST) {
            RangeRouteException routeException = (RangeRouteException) exception;
            if (existRange(context, routeException)) {
              return func.apply(context, reqBuilder, index).onErrorResume(getErrHandler(context, func, reqBuilder,
                  index));
            }
            return getRouteFlux(context, routeException.key).flatMap(flag -> func.apply(context, reqBuilder, index)
                                                                                 .onErrorResume(getErrHandler(context, func, reqBuilder, index)));
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
  interface ScanVerFunc {
    Flux<Long> apply(StoreCtx storeCtx, ByteString key, Index index);
  }

  public static Function<Throwable, Flux<Long>> getErrHandler(StoreCtx context, ScanVerFunc func, ByteString key, Index index) {
    return throwable -> {
      if (context.canRetryWithDelay()) {
        if (throwable instanceof BaseException) {
          BaseException exception = (BaseException) throwable;
          if (exception.getCode() == ErrorCode.ER_SHARD_ROUTE_CHANGE) {
            return func.apply(context, key, index).onErrorResume(getErrHandler(context, func, key, index));
          } else if (exception.getCode() == ErrorCode.ER_SHARD_NOT_EXIST) {
            RangeRouteException routeException = (RangeRouteException) exception;
            if (existRange(context, routeException)) {
              return func.apply(context, key, index).onErrorResume(getErrHandler(context, func, key, index));
            }
            return getRouteFlux(context, routeException.key).flatMap(flag -> func.apply(context, key, index)
                                                                                 .onErrorResume(getErrHandler(context, func, key, index)));
          }
        }
      }
      LOG.warn("do on err resume immediate throw err:", throwable);
      return Flux.error(throwable);
    };
  }

}
