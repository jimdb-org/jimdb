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
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.exception.BaseException;
import io.jimdb.common.exception.RangeRouteException;
import io.jimdb.common.utils.callback.Callback;
import io.jimdb.common.utils.lang.ByteUtil;
import io.jimdb.core.codec.Codec;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.RowValueAccessor;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.model.meta.RangeInfo;
import io.jimdb.core.model.meta.Table;
import io.jimdb.engine.StoreCtx;
import io.jimdb.engine.sender.DispatcherImpl;
import io.jimdb.pb.Txn;

import org.apache.commons.lang3.StringUtils;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.NettyByteString;

import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;

/**
 * Transaction handler that defines functions to be used in Transaction
 */
public class TxnHandler {
  private static final Logger LOG = LoggerFactory.getLogger(TxnHandler.class);

  protected static final RowValueAccessor[] ROW_VALUE_ACCESSORS_EMPTY = new RowValueAccessor[0];

  public static Flux<Txn.TxnStatus> txnGetLockInfo(StoreCtx storeCtx, String txnId, ByteString primaryKey) {
    Txn.GetLockInfoRequest.Builder request = Txn.GetLockInfoRequest.newBuilder()
                                                 .setTxnId(txnId).setKey(primaryKey);
    DispatcherImpl sender = storeCtx.getSender();
    return sender.txnGetLockInfo(storeCtx, request).map(response -> {
      if (response.hasErr()) {
        if (response.getErr().getErrType() == Txn.TxnError.ErrType.NOT_FOUND) {
          return Txn.TxnStatus.TXN_INIT;
        }
        BaseException err = convertTxnErr(response.getErr());
        LOG.error("get txn  {} lock info error:{}", txnId, err.getMessage());
        throw err;
      }
      return response.getInfo().getStatus();
    });
  }

  protected static ValueAccessor[] handleFlowValue(ColumnExpr[] resultColumns, List<Txn.Row> rows, StoreCtx storeCtx) {
    if (rows == null || rows.isEmpty()) {
      return TxnHandler.ROW_VALUE_ACCESSORS_EMPTY;
    }
    List<ValueAccessor> decodeList = new ArrayList<>();
    rows.stream().filter(row -> row != null && row.getValue() != null && !row.getValue().getFields().isEmpty()).forEach(row -> {
      Txn.RowValue rowValue = row.getValue();
      if (LOG.isInfoEnabled()) {
        LOG.info("decode row: {}", ByteUtil.bytes2hex01(NettyByteString.asByteArray(rowValue.getFields())));
      }
      decodeList.add(Codec.decodeRowWithOpt(resultColumns, rowValue, row.getPks(), storeCtx.getTimeZone()));
    });
    ValueAccessor[] rowArray = new ValueAccessor[decodeList.size()];
    return decodeList.toArray(rowArray);
  }

  public static Flux<Txn.TxnStatus> getTxnStatusFlux(StoreCtx storeCtx, String txnId, ByteString primaryKey, boolean
          txnTimeout) {
    if (txnTimeout) {
      if (LOG.isInfoEnabled()) {
        LOG.info("found timeout txn, need confirm txn[{}] status", txnId);
      }
      return RecoverHandler.recoverFromSecondaryAsync(storeCtx, txnId, primaryKey);
    } else {
      if (LOG.isInfoEnabled()) {
        LOG.info("found no timeout txn, need confirm txn[{}] status", txnId);
      }
      return txnGetLockInfo(storeCtx, txnId, primaryKey);
    }
  }

  public static BaseException convertTxnErr(Txn.TxnError txError) {
    switch (txError.getErrType()) {
      case SERVER_ERROR:
        Txn.ServerError serverErr = txError.getServerErr();
        return DBException.get(ErrorModule.ENGINE, ErrorCode.ER_SHARD_RESPONSE_ERROR,
                String.valueOf(serverErr.getCode()), serverErr.getMsg());
      case LOCKED:
        return DBException.get(ErrorModule.ENGINE, ErrorCode.ER_LOCK_ABORTED);
      case UNEXPECTED_VER:
        Txn.UnexpectedVer versionErr = txError.getUnexpectedVer();
        return DBException.get(ErrorModule.ENGINE, ErrorCode.ER_TXN_VERSION_CONFLICT,
                String.valueOf(versionErr.getExpectedVer()), String.valueOf(versionErr.getActualVer()));
      case STATUS_CONFLICT:
        Txn.StatusConflict statusConflict = txError.getStatusConflict();
        return DBException.get(ErrorModule.ENGINE, ErrorCode.ER_TXN_STATUS_CONFLICT, statusConflict.getStatus().name());
      case NOT_FOUND:
        return DBException.get(ErrorModule.ENGINE, ErrorCode.ER_SHARD_RESPONSE_ERROR,
                txError.getErrType().name(), "NOT FOUND");
      case NOT_UNIQUE:
        return DBException.get(ErrorModule.ENGINE, ErrorCode.ER_DUP_ENTRY,
                Arrays.toString(txError.getNotUnique().getKey().toByteArray()), "INDEX");
      case TXN_CONFLICT:
        Txn.TxnConflict txnConflict = txError.getTxnConflict();
        return DBException.get(ErrorModule.ENGINE, ErrorCode.ER_TXN_CONFLICT, txnConflict.getExpectedTxnId(),
                txnConflict.getActualTxnId());
      default:
        return DBException.get(ErrorModule.ENGINE, ErrorCode.ER_SHARD_RESPONSE_ERROR,
                txError.getErrType().name(), "UNKNOWN Transaction Error");
    }
  }

  protected static boolean existRange(StoreCtx storeCtx, RangeRouteException routeException) {
    //maybe: table no exist, or range no exist
    RangeInfo rangeInfo = storeCtx.getRoutePolicy().getRangeInfoByKeyFromCache(routeException.key);
    if (rangeInfo != null && StringUtils.isNotBlank(rangeInfo.getLeaderAddr())) {
      return true;
    }
    return false;
  }

  protected static Flux<Boolean> getRouteFlux(StoreCtx storeCtx, byte[] key) {
    return Flux.create(sink -> {
      Callback callback = new Callback() {
        @Override
        public void success(boolean value) {
          sink.next(value);
        }

        @Override
        public void fail(Throwable cause) {
//          if (cause instanceof MetaException) {
//            MetaException error = (MetaException) cause;
//            if (error.getCode() == ErrorCode.ER_BAD_DB_ERROR || error.getCode() == ErrorCode.ER_BAD_TABLE_ERROR) {
//              //todo update table、routePolicy
////              storeCtx.setTable();
//              storeCtx.getRpcManager().removePolicy(storeCtx.getTable().getId());
////              storeCtx.setRoutePolicy();
//              sink.next(Boolean.TRUE);
//              return;
//            }
//          }
          sink.error(cause);
        }
      };
      Table table = storeCtx.getTable();
      storeCtx.getRpcManager().retryRoute(table.getCatalogId(), table.getId(), key, callback);
    });
  }

  /**
   * Txn commit subscriber
   *
   * @param <T> TODO
   */
  static final class CommitSubscriber<T> extends BaseSubscriber<T> {
    private static final Logger LOG = LoggerFactory.getLogger(CommitSubscriber.class);

    private final Consumer<T> success;
    private final Consumer<Throwable> error;
    private boolean ok = false;

    CommitSubscriber(final Consumer<T> success, final Consumer<Throwable> error) {
      this.success = success;
      this.error = error;
    }

    @Override
    protected void hookOnSubscribe(Subscription subscription) {
      request(1);
    }

    @Override
    protected void hookOnNext(final T value) {
      success.accept(value);
      ok = true;
      dispose();
    }

    @Override
    protected void hookOnError(final Throwable throwable) {
      error.accept(throwable);
    }

    @Override
    protected void hookOnCancel() {
      if (ok) {
        return;
      }
      error.accept(DBException.get(ErrorModule.ENGINE, ErrorCode.ER_SYSTEM_CANCELLED));
    }
  }

  //  private static <T> Flux<T> noShardFlux(StoreCtx context, Flux<T> publisher, JimException exception) {
//    if (exception.getCode() != ErrorCode.ER_SHARD_NOT_EXIST) {
//      return null;
//    }
//
////    //maybe: table no exist, or range no exist
//    RangeRouteException routeException = (RangeRouteException) exception;
//    RangeInfo rangeInfo = context.getRoutePolicy().getRangeInfoByKeyFromCache(routeException.key);
//    if (rangeInfo != null && !StringUtils.isBlank(rangeInfo.getLeaderAddr())) {
//      return publisher.onErrorResume(getErrHandler(context, publisher));
//    }
//    return getRouteFlux(context, routeException.key)
//            .flatMap(flag -> publisher.onErrorResume(getErrHandler(context, publisher)));
//  }

  //  /**
//   * @param <T>
//   * @version V1.0
//   */
//  static final class ErrorHandlerCallback<T> implements Callback {
//    private static final Logger LOG = LoggerFactory.getLogger(ErrorHandlerCallback.class);
//
//    private final Flux<T> publisher;
//    private final CommitSubscriber<T> subscriber;
//    private final FluxSink sink;
////    FunctionalInterface
//
//    private final TxnContext context;
//
//    ErrorHandlerCallback(final Flux<T> publisher, final CommitSubscriber<T> subscriber, FluxSink sink, TxnContext context) {
//
//      this.publisher = publisher;
//      this.subscriber = subscriber;
//      this.sink = sink;
//      this.context = context;
//    }
//
//    public void success(boolean value) {
////      LOG.warn("txn {} ErrorHandlerCallback success", this.context.getConfig().getTxnId());
//      publisher.onErrorResume(getErrHandler(context, publisher)).subscribe(this.subscriber);
//    }
//
//    public void failed(final Throwable throwable) {
////      LOG.warn("txn {} ErrorHandlerCallback failed {}", this.context.getConfig().getTxnId(), throwable);
//      sink.error(throwable);
//    }
//  }
}
