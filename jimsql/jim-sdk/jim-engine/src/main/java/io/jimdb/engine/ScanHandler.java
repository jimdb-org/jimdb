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

import static java.util.Collections.EMPTY_LIST;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import io.jimdb.common.exception.BaseException;
import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.exception.RangeRouteException;
import io.jimdb.common.utils.lang.ByteUtil;
import io.jimdb.core.codec.Codec;
import io.jimdb.core.codec.KvPair;
import io.jimdb.core.model.meta.Index;
import io.jimdb.core.model.meta.RangeInfo;
import io.jimdb.engine.client.RequestContext;
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
public class ScanHandler extends RequestHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(ScanHandler.class);

  private static final List<Txn.KeyValue> KET_VALUE_EMPTY_LIST_INSTANCE = new ArrayList<>();
  private static final RequestFunc<List<Txn.KeyValue>, Txn.ScanRequest.Builder> SEND_SCAN_REQ_FUNC = ScanHandler::sendScanReq;
  public static final ScanVerFunc SCAN_VER_FUNC = ScanHandler::txnScanForVersion;
  public static final ScanPKFunc SCAN_PK_FUNC = ScanHandler::txnScanForPk;

  // sendScanReqFunc
  private static Flux<List<Txn.KeyValue>> sendScanReq(ShardSender shardSender, StoreCtx storeCtx, Txn.ScanRequest.Builder request,
                                                      ByteString key, RangeInfo rangeInfo) {
    RequestContext context = new RequestContext(storeCtx, key, rangeInfo, request, Api.RangeRequest.ReqCase.SCAN);

    return shardSender.sendReq(context).map(response -> {
      Txn.ScanResponse scanResponse = (Txn.ScanResponse) response;
      if (scanResponse.getCode() > 0) {
        throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_SHARD_RESPONSE_CODE, "sendScanReq",
            String.valueOf(scanResponse.getCode()));
      }
      return scanResponse.getKvsList();
    });
  }

  private static Flux<Long> txnScanForVersion(ShardSender shardSender, StoreCtx storeCtx, ByteString startKey, Index index) {
    ByteString endKey = Codec.nextKey(startKey);
    Txn.ScanRequest.Builder request = Txn.ScanRequest.newBuilder()
                                          .setStartKey(startKey)
                                          .setEndKey(endKey)
                                          .setOnlyOne(true);

    return ScanHandler.txnScan(shardSender, storeCtx, request)
               .map(kvs -> {
                 if (kvs.size() != 1) {
                   throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_SHARD_RESPONSE_VERSION_DUP, kvs.toString());
                 }
                 if (kvs.isEmpty() || kvs.get(0) == null || kvs.get(0).getValue() == null) {
                   LOGGER.error("txnScanForVersion: ctx {}  scan result {}", storeCtx.getCxtId(), kvs);
                   throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_SHARD_RESPONSE_VERSION_DUP, kvs.toString());
                 }
                 Long version = kvs.get(0).getVersion();
                 if (LOGGER.isDebugEnabled()) {
                   LOGGER.debug("txn scan index {} kv, version:{}", index.getName(), version);
                 }
                 if (version == 0) {
                   LOGGER.error("txn scan index {} kv, version:{}", index.getName(), version);
                 }
                 return version;
               });
  }

  public static Flux<Boolean> txnScanForExist(ShardSender shardSender, StoreCtx storeCtx, KvPair idxKvPair, Index index) {
    ByteString endKey = Codec.nextKey(idxKvPair.getKey());
    Txn.ScanRequest.Builder request = Txn.ScanRequest.newBuilder()
                                          .setStartKey(idxKvPair.getKey())
                                          .setEndKey(endKey)
                                          .setOnlyOne(true);

    return ScanHandler.txnScan(shardSender, storeCtx, request)
               .map(kvs -> {
                 if (kvs.isEmpty() || kvs.get(0) == null || kvs.get(0).getValue() == null) {
                   return Boolean.FALSE;
                 }
                 if (kvs.size() > 1) {
                   throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_SHARD_RESPONSE_VERSION_DUP, kvs.toString());
                 }
                 if (index.isUnique()) {
                   ByteString value = kvs.get(0).getValue();
                   if (!value.isEmpty() && ByteUtil.compare(idxKvPair.getValue(), value) != 0) {
                     LOGGER.error("add index {}.{} scan duplicate, {}", index.getTable().getName(), index.getName(),
                         Arrays.toString(idxKvPair.getKey().toByteArray()));
                     throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_DUP_ENTRY,
                         Arrays.toString(idxKvPair.getKey().toByteArray()), "INDEX");
                   }
                 }

                 return Boolean.TRUE;
               });
  }

  private static Flux<List<ByteString>> txnScanForPk(ShardSender shardSender, StoreCtx storeCtx, Txn.ScanRequest.Builder request, Index index) {
    request.setMaxCount(10000);
    return ScanHandler.txnScan(shardSender, storeCtx, request).map(kvs -> {
      if (kvs.isEmpty()) {
        return EMPTY_LIST;
      }

      List<ByteString> pkValuesList = new ArrayList<>(kvs.size());
      for (Txn.KeyValue kv : kvs) {
        if (kv == null || kv.getValue() == null) {
          continue;
        }

        try {
          ByteString pkValues = kv.getValue();
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("txn scan index {} kv, pkValues:{}", index.getName(), pkValues);
          }
          pkValuesList.add(pkValues);
        } catch (Throwable e) {
          LOGGER.error("decode record key from [{}] err:{}", kv, e);
          throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_RPC_REQUEST_CODEC, e);
        }
      }
      return pkValuesList;
    });
  }

  /**
   * TODO
   *
   * @param storeCtx TODO
   * @param request  TODO
   * @return TODO
   */
  private static Flux<List<Txn.KeyValue>> txnScan(ShardSender shardSender, StoreCtx storeCtx, Txn.ScanRequest.Builder request) {
//    ByteString start = request.getStartKey();
//    ByteString end = request.getEndKey();

    return internalTxnScan(shardSender, storeCtx, request).flatMap(keyValues -> {
      if (keyValues == null || keyValues.isEmpty()) {
        LOGGER.warn("scan: context {} txnScan {} is empty ", storeCtx.getCxtId(), request);
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
        Flux<List<Txn.KeyValue>> scanRowFlux = handleScanRow(shardSender, storeCtx, request, keyValue).map(r -> {
          if (r == ByteString.EMPTY) {
            LOGGER.warn("scan: context {} handle intent {} is empty ", storeCtx.getCxtId(), keyValue);
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

  /**
   * Send and process a Scan request  TODO refactor this function, it has been shared in two places
   *
   * @param storeCtx   store context
   * @param reqBuilder request builder
   * @return processed Scan result
   */
  private static Flux<List<Txn.KeyValue>> internalTxnScan(ShardSender shardSender, StoreCtx storeCtx, Txn.ScanRequest.Builder reqBuilder) {
    ByteString start = reqBuilder.getStartKey();
    ByteString end = reqBuilder.getEndKey();

    return RangeSelector.rangeSelect(shardSender, storeCtx, SEND_SCAN_REQ_FUNC, reqBuilder, start, end);
  }

  private static Flux<ByteString> handleScanRow(ShardSender shardSender, StoreCtx storeCtx, Txn.ScanRequest.Builder reqBuilder, Txn.KeyValue keyValue) {
    Txn.ValueIntent valueIntent = keyValue.getIntent();
    return getTxnStatus(shardSender, storeCtx, valueIntent.getTxnId(), valueIntent.getPrimaryKey(), valueIntent.getTimeout())
               .flatMap(status -> {
                 if (LOGGER.isInfoEnabled()) {
                   LOGGER.info("scan: confirm txn[{}] status:[{}] ", valueIntent.getTxnId(), status);
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
                     return internalTxnScan(shardSender, storeCtx, reqBuilder).map(kvs -> {
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

  public static Function<Throwable, Flux<List<ByteString>>> getErrHandler(ShardSender shardSender, StoreCtx context, ScanPKFunc func,
                                                                          Txn.ScanRequest.Builder reqBuilder, Index index) {
    return throwable -> {
      if (context.canRetryWithDelay()) {
        if (throwable instanceof BaseException) {
          BaseException exception = (BaseException) throwable;
          if (exception.getCode() == ErrorCode.ER_SHARD_ROUTE_CHANGE) {
            return func.apply(shardSender, context, reqBuilder, index).onErrorResume(getErrHandler(shardSender, context, func, reqBuilder,
                index));
          } else if (exception.getCode() == ErrorCode.ER_SHARD_NOT_EXIST) {
            RangeRouteException routeException = (RangeRouteException) exception;
            if (context.getRoutingPolicy().rangeExists(routeException)) {
              return func.apply(shardSender, context, reqBuilder, index).onErrorResume(getErrHandler(shardSender, context, func, reqBuilder,
                  index));
            }
            return context.getRouter().getRoutingFlux(context.getTable(), routeException.key).flatMap(flag -> func.apply(shardSender, context, reqBuilder, index)
                                                                                                                .onErrorResume(getErrHandler(shardSender, context, func, reqBuilder, index)));
          }
        }
      }
      LOGGER.warn("do on err resume immediate throw err:", throwable);
      return Flux.error(throwable);
    };
  }

  public static Function<Throwable, Flux<Long>> getErrHandler(ShardSender shardSender, StoreCtx context, ScanVerFunc func, ByteString key, Index index) {
    return throwable -> {
      if (context.canRetryWithDelay()) {
        if (throwable instanceof BaseException) {
          BaseException exception = (BaseException) throwable;
          if (exception.getCode() == ErrorCode.ER_SHARD_ROUTE_CHANGE) {
            return func.apply(shardSender, context, key, index).onErrorResume(getErrHandler(shardSender, context, func, key, index));
          } else if (exception.getCode() == ErrorCode.ER_SHARD_NOT_EXIST) {
            RangeRouteException routeException = (RangeRouteException) exception;
            if (context.getRoutingPolicy().rangeExists(routeException)) {
              return func.apply(shardSender, context, key, index).onErrorResume(getErrHandler(shardSender, context, func, key, index));
            }
            return context.getRouter().getRoutingFlux(context.getTable(), routeException.key).flatMap(flag -> func.apply(shardSender, context, key, index)
                                                                                                                .onErrorResume(getErrHandler(shardSender, context, func, key, index)));
          }
        }
      }
      LOGGER.warn("do on err resume immediate throw err:", throwable);
      return Flux.error(throwable);
    };
  }

  /**
   * TODO
   */
  @FunctionalInterface
  interface ScanPKFunc {
    Flux<List<ByteString>> apply(ShardSender shardSender, StoreCtx storeCtx, Txn.ScanRequest.Builder request, Index index);
  }

  /**
   * TODO
   */
  @FunctionalInterface
  interface ScanVerFunc {
    Flux<Long> apply(ShardSender shardSender, StoreCtx storeCtx, ByteString key, Index index);
  }

}
