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

import static io.jimdb.engine.RequestHandler.getTxnStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import io.jimdb.common.exception.BaseException;
import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.exception.RangeRouteException;
import io.jimdb.common.utils.lang.ByteUtil;
import io.jimdb.core.codec.Codec;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.model.meta.RangeInfo;
import io.jimdb.engine.client.RequestContext;
import io.jimdb.pb.Api;
import io.jimdb.pb.Txn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.NettyByteString;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.core.publisher.Flux;

/**
 * TODO
 */
@SuppressFBWarnings()
public class KeyGetter { //extends TransactionImpl {
  private static final Logger LOGGER = LoggerFactory.getLogger(KeyGetter.class);

  private static final List<Txn.RowValue> ROW_VALUE_EMPTY_LIST_INSTANCE = new ArrayList<>();
  private static final Txn.RowValue ROW_VALUE_EMPTY_INSTANCE = Txn.RowValue.newBuilder().build();

  static final KeyGetFunc KEY_GET_FUNC = KeyGetter::keyGet;

  // private final RequestHandler.Func<List<Txn.Row>, Txn.SelectRequest.Builder> sendKeyGetReqFunc = KeyGetter::sendKeyGetReq;

  /**
   * Get rows based on primary key or unique index.
   *
   * @param storeCtx      TODO
   * @param reqBuilder    TODO
   * @param resultColumns TODO
   * @return TODO
   */
  public static Flux<ValueAccessor[]> keyGet(ShardSender shardSender, StoreCtx storeCtx, Txn.SelectRequest.Builder reqBuilder, ColumnExpr[]
                                                                                                          resultColumns) {
    return txnKeyGetNoDecode(shardSender, storeCtx, reqBuilder).map(rows -> {
      List<ValueAccessor> decodeList = new ArrayList<>();
      rows.stream().filter(row -> row != null && !row.getFields().isEmpty()).forEach(row -> {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("decode row: {}", ByteUtil.bytes2hex01(NettyByteString.asByteArray(row.getFields())));
        }
        decodeList.add(Codec.decodeRow(resultColumns, row, storeCtx.getTimeZone()));
      });
      ValueAccessor[] rowArray = new ValueAccessor[decodeList.size()];
      return decodeList.toArray(rowArray);
    });
  }

  private static Flux<List<Txn.RowValue>> txnKeyGetNoDecode(ShardSender shardSender, StoreCtx storeCtx, Txn.SelectRequest.Builder reqBuilder) {
    return txnKeyGet(shardSender, storeCtx, reqBuilder).flatMap(rows -> {
      if (rows == null || rows.isEmpty()) {
        return Flux.just(ROW_VALUE_EMPTY_LIST_INSTANCE);
      }

      List<Txn.RowValue> rowValues = new ArrayList<Txn.RowValue>(rows.size());

      Flux<List<Txn.RowValue>> flux = null;
      for (int i = 0; i < rows.size(); i++) {
        Txn.Row row = rows.get(i);
        if (!row.hasIntent()) {
          rowValues.add(i, row.getValue());
          continue;
        }

        rowValues.add(i, null);
        final int j = i;
        Flux<List<Txn.RowValue>> txnRowFlux = handleTxnRow(shardSender, storeCtx, reqBuilder, row).map(r -> {
          rowValues.set(j, r);
          return rowValues;
        });
        if (flux == null) {
          flux = txnRowFlux;
        } else {
          flux = flux.zipWith(txnRowFlux, (f1, f2) -> f1);
        }
      }

      if (flux == null) {
        flux = Flux.just(rowValues);
      }
      return flux;
    });
  }

  /**
   * Send and process a KeyGet request
   *
   * @param storeCtx   store context
   * @param reqBuilder request builder
   * @return processed KeyGet result
   */
  private static Flux<List<Txn.Row>> txnKeyGet(ShardSender shardSender, StoreCtx storeCtx, Txn.SelectRequest.Builder reqBuilder) {
    if (!reqBuilder.getKey().isEmpty()) {
      return sendKeyGetReq(shardSender, storeCtx, reqBuilder, reqBuilder.getKey(), null);
    }

    // This should never happen since the KeyGet only applies to point query on primary key or unique index
    LOGGER.error("KeyGet request with null key");
    throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_SHARD_RESPONSE_CODE, "txnKeyGet");
  }

  // sendKeyGetReqFunc
  private static Flux<List<Txn.Row>> sendKeyGetReq(ShardSender shardSender, StoreCtx storeCtx, Txn.SelectRequest.Builder reqBuilder, ByteString key,
                                                   RangeInfo rangeInfo) {
    RequestContext context = new RequestContext(storeCtx, key, rangeInfo, reqBuilder, Api.RangeRequest.ReqCase.SELECT);

    return shardSender.sendReq(context).map(response -> {
      Txn.SelectResponse selectResponse = (Txn.SelectResponse) response;
      if (selectResponse.getCode() > 0) {
        throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_SHARD_RESPONSE_CODE, "sendKeyGetReq",
            String.valueOf(selectResponse.getCode()));
      }
      if (LOGGER.isInfoEnabled()) {
        LOGGER.info("txn select response success: column size {}, response row {}", reqBuilder.getFieldListCount(),
            selectResponse.getRowsList().size());
      }
      return selectResponse.getRowsList();
    });
  }

  private static Flux<Txn.RowValue> handleTxnRow(ShardSender shardSender, StoreCtx storeCtx, Txn.SelectRequest.Builder reqBuilder, Txn.Row row) {
    Txn.RowIntent rowIntent = row.getIntent();
    return getTxnStatus(shardSender, storeCtx, rowIntent.getTxnId(), rowIntent.getPrimaryKey(), rowIntent.getTimeout())
               .flatMap(status -> {
                 if (LOGGER.isInfoEnabled()) {
                   LOGGER.info("select: confirm txn[{}] status:[{}] ", rowIntent.getTxnId(), status);
                 }
                 Txn.RowValue rowValue = null;
                 switch (status.getNumber()) {
                   //aborted
                   case Txn.TxnStatus.ABORTED_VALUE:
                     rowValue = row.getValue();
                     break;
                   case Txn.TxnStatus.COMMITTED_VALUE:
                     switch (rowIntent.getOpTypeValue()) {
                       case Txn.OpType.INSERT_VALUE:
                         rowValue = rowIntent.getValue();
                         break;
                       default:
                         break;
                     }
                     break;
                   default:
                     //retry read
                     Txn.SelectRequest.Builder builder = Txn.SelectRequest.newBuilder()
                                                             .setKey(row.getKey())
                                                             .addAllFieldList(reqBuilder.getFieldListList());
                     return txnKeyGet(shardSender, storeCtx, builder).map(retryRows -> {
                       if (retryRows != null && retryRows.size() > 1) {
                         throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_TXN_ROW_SIZE, "1", String.valueOf
                                                                                                              (retryRows.size
                                                                                                                             ()));
                       }
                       if (retryRows != null && retryRows.size() == 1 && retryRows.get(0).getValue() != null) {
                         return retryRows.get(0).getValue();
                       }
                       return ROW_VALUE_EMPTY_INSTANCE;
                     });
                 }
                 if (rowValue == null) {
                   rowValue = ROW_VALUE_EMPTY_INSTANCE;
                 }
                 return Flux.just(rowValue);
               });
  }

  public static Function<Throwable, Flux<ValueAccessor[]>> getErrHandler(ShardSender shardSender, StoreCtx context, KeyGetFunc func,
                                                                         Txn.SelectRequest.Builder reqBuilder,
                                                                         ColumnExpr[] exprs) {
    return throwable -> {
      if (context.canRetryWithDelay()) {
        if (throwable instanceof BaseException) {
          BaseException exception = (BaseException) throwable;
          if (exception.getCode() == ErrorCode.ER_SHARD_ROUTE_CHANGE) {
            return func.apply(shardSender, context, reqBuilder, exprs).onErrorResume(getErrHandler(shardSender, context, func, reqBuilder,
                exprs));
          } else if (exception.getCode() == ErrorCode.ER_SHARD_NOT_EXIST) {
            RangeRouteException routeException = (RangeRouteException) exception;
            if (context.getRoutingPolicy().rangeExists(routeException)) {
              return func.apply(shardSender, context, reqBuilder, exprs).onErrorResume(getErrHandler(shardSender, context, func, reqBuilder,
                  exprs));
            }
            return context.getRouter().getRoutingFlux(context.getTable(), routeException.key).flatMap(flag -> func.apply(shardSender, context, reqBuilder, exprs)
                                                                                 .onErrorResume(getErrHandler(shardSender, context, func, reqBuilder, exprs)));
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
  interface KeyGetFunc {
    Flux<ValueAccessor[]> apply(ShardSender shardSender, StoreCtx context, Txn.SelectRequest.Builder reqBuilder, ColumnExpr[] exprs);
  }
}
