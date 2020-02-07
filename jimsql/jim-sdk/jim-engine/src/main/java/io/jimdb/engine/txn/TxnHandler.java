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
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.exception.JimException;
import io.jimdb.common.exception.RangeRouteException;
import io.jimdb.common.utils.callback.Callback;
import io.jimdb.common.utils.lang.ByteUtil;
import io.jimdb.core.codec.Codec;
import io.jimdb.core.codec.KvPair;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.RowValueAccessor;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.model.meta.Index;
import io.jimdb.core.model.meta.RangeInfo;
import io.jimdb.core.model.meta.Table;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.model.result.impl.AckExecResult;
import io.jimdb.engine.StoreCtx;
import io.jimdb.engine.client.RequestContext;
import io.jimdb.engine.sender.DistSender;
import io.jimdb.engine.sender.Util;
import io.jimdb.meta.route.RoutePolicy;
import io.jimdb.pb.Api.RangeRequest.ReqCase;
import io.jimdb.pb.Txn;

import org.apache.commons.lang3.StringUtils;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.NettyByteString;

import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

/**
 * @version V1.0
 */
public class TxnHandler {
  private static final Logger LOG = LoggerFactory.getLogger(TxnHandler.class);

  private static final DecideSecondaryFunc DECIDE_SEC_FUNC = TxnHandler::decideSecondary;
  private static final RecoverFunc RECOVER_FUNC = TxnHandler::recover;

  private static final Txn.RowValue ROW_VALUE_EMPTY_INSTANCE = Txn.RowValue.newBuilder().build();
  private static final List<Txn.RowValue> ROW_VALUE_EMPTY_LIST_INSTANCE = new ArrayList<>();
  private static final List<Txn.KeyValue> KET_VALUE_EMPTY_LIST_INSTANCE = new ArrayList<>();
  protected static final RowValueAccessor[] ROW_VALUE_ACCESSORS_EMPTY = new RowValueAccessor[0];

  public static RequestContext buildPriPrepareReqCxt(TxnConfig config, StoreCtx storeCtx) {
    Txn.PrepareRequest.Builder primaryReq = Util.buildPrepare4Primary(config);
    return new RequestContext(storeCtx, primaryReq.getIntents(0).getKey(), primaryReq, ReqCase.PREPARE);
  }

  public static RequestContext buildSecsPrepareReqCxt(TxnConfig config, StoreCtx storeCtx, List<Txn.TxnIntent> intentList,
                                                      RangeInfo rangeInfo) {
    Txn.PrepareRequest.Builder secondaryReq = Util.buildPrepare4Secondary(config, intentList);
    return new RequestContext(storeCtx, secondaryReq.getIntents(0).getKey(), rangeInfo, secondaryReq, ReqCase.PREPARE);
  }

  public static RequestContext buildPriDecideReqCtx(TxnConfig config, StoreCtx storeCtx, Txn.TxnStatus status) {
    Txn.DecideRequest.Builder reqBuilder = Util.buildTxnDecide4Primary(config, status);
    return new RequestContext(storeCtx, reqBuilder.getKeys(0), reqBuilder, ReqCase.DECIDE);
  }

  public static RequestContext buildDecideSecsReqCxt(StoreCtx storeCtx, String txnId, Txn.TxnStatus status,
                                                     List<ByteString> keyList, RangeInfo rangeInfo) {
    Txn.DecideRequest.Builder reqBuilder = Txn.DecideRequest.newBuilder()
            .setTxnId(txnId).setStatus(status)
            .addAllKeys(keyList);

    return new RequestContext(storeCtx, reqBuilder.getKeys(0), rangeInfo, reqBuilder, ReqCase.DECIDE);
  }

  public static RequestContext buildClearUpReqCtx(TxnConfig config, StoreCtx storeCtx) {
    Txn.ClearupRequest.Builder request = Txn.ClearupRequest.newBuilder()
            .setTxnId(config.getTxnId()).setPrimaryKey(config.getPriIntent().getKey());
    return new RequestContext(storeCtx, request.getPrimaryKey(), request, ReqCase.CLEAR_UP);
  }

  public static Flux<ExecResult> txnPrepare(StoreCtx storeCtx, RequestContext reqCtx) {
    DistSender sender = storeCtx.getSender();
    return sender.txnPrepare(reqCtx).flatMap(handlePrepareResp(storeCtx, reqCtx));
  }

  public static Flux<ExecResult> txnPreparePri(StoreCtx storeCtx, TxnConfig config) {
    RequestContext reqCtx = TxnHandler.buildPriPrepareReqCxt(config, storeCtx);
    return txnPrepare(storeCtx, reqCtx);
  }

  public static Flux<ExecResult> txnPrepareSec(StoreCtx storeCtx, TxnConfig config, List<Txn.TxnIntent> groupList,
                                               RangeInfo rangeInfo) {
    RequestContext reqCtx = TxnHandler.buildSecsPrepareReqCxt(config, storeCtx, groupList, rangeInfo);
    return txnPrepare(storeCtx, reqCtx);
  }

  public static Function<Txn.PrepareResponse, Flux<ExecResult>> handlePrepareResp(StoreCtx storeCtx, RequestContext reqCtx) {
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
              JimException err = convertTxnErr(txnError);
              LOG.error("prepare txn [{}] exist no-timeout lock, need try. error:",
                      reqBuilder.getTxnId(), err);
            }
            break;
          default:
            JimException err = convertTxnErr(txnError);
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
        child = recoverFromPrimary(storeCtx, reqBuilder, lockInfo.getSecondaryKeysList());
      } else {
        child = recoverFromSecondary(storeCtx, lockInfo.getTxnId(), lockInfo.getPrimaryKey(), Txn.TxnStatus.ABORTED);
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
   * try to rollback and flag aborted status
   * rollback secondary intents
   * rollback primary intents
   *
   * @param storeCtx
   * @param reqBuilder
   * @param secondaryKeys
   * @return
   */
  public static Flux<Boolean> recoverFromPrimary(StoreCtx storeCtx, Txn.DecideRequest.Builder reqBuilder,
                                                 List<ByteString> secondaryKeys) {
    DistSender sender = storeCtx.getSender();
    //first try to decide expired tx to aborted status
    RequestContext reqCtx = new RequestContext(storeCtx, reqBuilder.getKeys(0), reqBuilder, ReqCase.DECIDE);
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
            JimException exception = convertTxnErr(response.getErr());
            LOG.warn("recover: retry to aborted txn{} primary intent and return err:{}, ignore err", reqBuilder.getTxnId(), exception.getMessage());
            return Flux.just(Boolean.TRUE);
          default:
            JimException err = convertTxnErr(response.getErr());
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
      RequestContext clearUpCxt = new RequestContext(storeCtx, clearUpRequest.getPrimaryKey(), clearUpRequest, ReqCase.CLEAR_UP);
      Flux<Boolean> clearUpFlux = clearUp(clearUpCxt, sender).map(r -> Boolean.TRUE);

      if (recoverSecondaryKeys != null && !recoverSecondaryKeys.isEmpty()) {
        return decideSecondary(storeCtx, reqBuilder.getTxnId(), recoverSecondaryKeys, applyStatus)
                .flatMap(flag -> {
                  if (!flag) {
                    return Flux.just(flag);
                  }
                  return clearUpFlux;
                });
      } else {
        return clearUpFlux;
      }
    });
  }

  public static Flux<Boolean> recoverFromSecondary(StoreCtx context, String txnId, ByteString primaryKey, Txn.TxnStatus status) {

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

    RequestContext context = new RequestContext(storeCtx, reqBuilder.getKeys(0), reqBuilder, ReqCase.DECIDE);
    DistSender sender = storeCtx.getSender();
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
            JimException exception = convertTxnErr(response.getErr());
            LOG.warn("recover: retry to aborted txn{} primary intent and return err:{}, ignore err", reqBuilder.getTxnId(), exception.getMessage());
            return Txn.TxnStatus.TXN_INIT;
          default:
            JimException err = convertTxnErr(response.getErr());
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

  public static Flux<Boolean> decideSecondary(StoreCtx context, String txnId, List<ByteString> keys, Txn.TxnStatus status) {
    if (LOG.isInfoEnabled()) {
      LOG.info("start to decide txn {} secondary", txnId);
    }

    if (keys == null || keys.isEmpty()) {
      return Flux.just(Boolean.TRUE);
    }

    RoutePolicy routePolicy = context.getRoutePolicy();

    Flux<Boolean> flux = null;

    Map<RangeInfo, List<ByteString>> intentKeyGroupMap;
    try {
      intentKeyGroupMap = routePolicy.regroupByRoute(keys, ByteString::toByteArray);
    } catch (Throwable e) {
      LOG.warn("txn {} decide Secondary {} error:{}", txnId, keys, e);
      return TxnHandler.getErrHandler(context, DECIDE_SEC_FUNC, txnId, status, keys, e);
    }

    for (Map.Entry<RangeInfo, List<ByteString>> entry : intentKeyGroupMap.entrySet()) {
      List<ByteString> keyList = entry.getValue();
      if (keyList == null || keyList.isEmpty()) {
        continue;
      }

      Flux<Boolean> child = txnDecide(context, txnId, keyList, status, entry.getKey());
      child = child.onErrorResume(TxnHandler.getErrHandler(context, DECIDE_SEC_FUNC, txnId, status, keyList));
      if (flux == null) {
        flux = child;
      } else {
        flux = flux.zipWith(child, (f1, f2) -> {
          if (!f1.booleanValue() || !f2.booleanValue()) {
            return Boolean.FALSE;
          }
          return Boolean.TRUE;
        });
      }
    }
    return flux;
  }

  private static Flux<Boolean> txnDecide(StoreCtx context, String txnId, List<ByteString> keyList,
                                         Txn.TxnStatus status, RangeInfo rangeInfo) {
    DistSender sender = context.getSender();
    RequestContext reqCxt = buildDecideSecsReqCxt(context, txnId, status, keyList, rangeInfo);
    return sender.txnDecide(reqCxt).map(r -> {
      if (r.hasErr()) {
        //store and so on
        return Boolean.FALSE;
      }
      return Boolean.TRUE;
    });
  }

  public static Flux<Txn.ClearupResponse> clearUp(RequestContext context, DistSender sender) {
    return sender.clearUp(context);
  }

  public static Flux<Txn.DecideResponse> txnDecidePrimary(RequestContext context, DistSender sender) {
    Txn.DecideRequestOrBuilder reqBuilder = (Txn.DecideRequestOrBuilder) context.getReqBuilder();
    return sender.txnDecide(context).map(response -> {
      if (response.hasErr()) {
        Txn.TxnError txnError = response.getErr();
        if (txnError.getErrType() == Txn.TxnError.ErrType.NOT_FOUND) {
          LOG.warn("[commit]decide txn{}: ds return it not found, ignore", reqBuilder.getTxnId());
        } else {
          JimException err = convertTxnErr(txnError);
          LOG.error("[commit]decide txn{} primary intent error: {} ", reqBuilder.getTxnId(), err);
          throw err;
        }
      }
      if (LOG.isInfoEnabled()) {
        LOG.info("txn {} decide primary success", reqBuilder.getTxnId());
      }
      return response;
    });
  }

  public static Flux<List<Txn.RowValue>> txnSelectNoDecode(StoreCtx storeCtx, Txn.SelectRequest.Builder reqBuilder) {
    DistSender sender = storeCtx.getSender();
    return sender.txnSelect(storeCtx, reqBuilder).flatMap(rows -> {
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
        Flux<List<Txn.RowValue>> txnRowFlux = handleTxnRow(storeCtx, reqBuilder, row).map(r -> {
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

  public static Flux<Txn.RowValue> handleTxnRow(StoreCtx storeCtx, Txn.SelectRequest.Builder reqBuilder, Txn.Row row) {
    Txn.RowIntent rowIntent = row.getIntent();
    DistSender sender = storeCtx.getSender();
    return getTxnStatusFlux(storeCtx, rowIntent.getTxnId(), rowIntent.getPrimaryKey(), rowIntent.getTimeout()).flatMap(status -> {
      if (LOG.isInfoEnabled()) {
        LOG.info("select: confirm txn[{}] status:[{}] ", rowIntent.getTxnId(), status);
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
          return sender.txnSelect(storeCtx, builder).map(retryRows -> {
            if (retryRows != null && retryRows.size() > 1) {
              throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_TXN_ROW_SIZE, "1", String.valueOf(retryRows.size()));
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

  public static Flux<ValueAccessor[]> select(StoreCtx storeCtx, Txn.SelectRequest.Builder reqBuilder, ColumnExpr[] resultColumns) {
    return txnSelectNoDecode(storeCtx, reqBuilder).map(rows -> {
      List<ValueAccessor> decodeList = new ArrayList<>();
      rows.stream().filter(row -> row != null && !row.getFields().isEmpty()).forEach(row -> {
        if (LOG.isDebugEnabled()) {
          LOG.debug("decode row: {}", ByteUtil.bytes2hex01(NettyByteString.asByteArray(row.getFields())));
        }
        decodeList.add(Codec.decodeRow(resultColumns, row));
      });
      ValueAccessor[] rowArray = new ValueAccessor[decodeList.size()];
      return decodeList.toArray(rowArray);
    });
  }

  //Txn.SelectFlowRequest.Builder
  public static Flux<ValueAccessor[]> selectFlowForRange(StoreCtx storeCtx, MessageOrBuilder reqBuilder,
                                                         ColumnExpr[] resultColumns, KvPair kvPair) {
    DistSender sender = storeCtx.getSender();
    return sender.txnSelectFlowForRange(storeCtx, reqBuilder, kvPair)
            .map(rows -> handleFlowValue(resultColumns, rows));
  }

  public static Flux<ValueAccessor[]> selectFlowForKeys(StoreCtx storeCtx, MessageOrBuilder reqBuilder,
                                                        ColumnExpr[] resultColumns, List<ByteString> keys) {
    DistSender sender = storeCtx.getSender();
    return sender.txnSelectFlowForKeys(storeCtx, reqBuilder, keys)
            .map(rows -> handleFlowValue(resultColumns, rows));
  }

  //Txn.SelectFlowRequest.Builder
  protected static Flux<ValueAccessor[]> selectFlowStream(StoreCtx storeCtx, Txn.SelectFlowRequest.Builder reqBuilder,
                                                          ColumnExpr[] resultColumns, KvPair kvPair) {
    DistSender sender = storeCtx.getSender();
    return sender.txnSelectFlowStream(storeCtx, reqBuilder, kvPair).map(rows -> handleFlowValue(resultColumns, rows));
  }

  private static ValueAccessor[] handleFlowValue(ColumnExpr[] resultColumns, List<Txn.Row> rows) {
    if (rows == null || rows.isEmpty()) {
      return TxnHandler.ROW_VALUE_ACCESSORS_EMPTY;
    }
    List<ValueAccessor> decodeList = new ArrayList<>();
    rows.stream().filter(row -> row != null && row.getValue() != null && !row.getValue().getFields().isEmpty()).forEach(row -> {
      Txn.RowValue rowValue = row.getValue();
      if (LOG.isInfoEnabled()) {
        LOG.info("decode row: {}", ByteUtil.bytes2hex01(NettyByteString.asByteArray(rowValue.getFields())));
      }
      decodeList.add(Codec.decodeRowWithOpt(resultColumns, rowValue, row.getPks()));
    });
    ValueAccessor[] rowArray = new ValueAccessor[decodeList.size()];
    return decodeList.toArray(rowArray);
  }

  public static Flux<Txn.TxnStatus> txnGetLockInfo(StoreCtx storeCtx, String txnId, ByteString primaryKey) {
    Txn.GetLockInfoRequest.Builder request = Txn.GetLockInfoRequest.newBuilder()
            .setTxnId(txnId).setKey(primaryKey);
    DistSender sender = storeCtx.getSender();
    return sender.txnGetLockInfo(storeCtx, request).map(response -> {
      if (response.hasErr()) {
        if (response.getErr().getErrType() == Txn.TxnError.ErrType.NOT_FOUND) {
          return Txn.TxnStatus.TXN_INIT;
        }
        JimException err = convertTxnErr(response.getErr());
        LOG.error("get txn  {} lock info error:{}", txnId, err.getMessage());
        throw err;
      }
      return response.getInfo().getStatus();
    });
  }

  public static Flux<List<Txn.KeyValue>> txnScan(StoreCtx storeCtx, Txn.ScanRequest.Builder request) {
    DistSender sender = storeCtx.getSender();
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

  public static Flux<ByteString> handleScanRow(StoreCtx storeCtx, Txn.ScanRequest.Builder reqBuilder, Txn.KeyValue keyValue) {
    Txn.ValueIntent valueIntent = keyValue.getIntent();
    DistSender sender = storeCtx.getSender();
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
                      throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_TXN_ROW_SIZE, "1", String.valueOf(kvs.size()));
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

  public static Flux<Txn.TxnStatus> getTxnStatusFlux(StoreCtx storeCtx, String txnId, ByteString primaryKey, boolean txnTimeout) {
    if (txnTimeout) {
      if (LOG.isInfoEnabled()) {
        LOG.info("found timeout txn, need confirm txn[{}] status", txnId);
      }
      return recoverFromSecondaryAsync(storeCtx, txnId, primaryKey);
    } else {
      if (LOG.isInfoEnabled()) {
        LOG.info("found no timeout txn, need confirm txn[{}] status", txnId);
      }
      return txnGetLockInfo(storeCtx, txnId, primaryKey);
    }
  }

  public static <T> Function<Throwable, Flux> getErrHandler(StoreCtx storeCtx, PrepareSecondaryFunc<T> func,
                                                            TxnConfig config, List<T> list) {
    return throwable -> getErrHandler(storeCtx, func, config, list, throwable);
  }

  public static <T> Flux getErrHandler(StoreCtx storeCtx, PrepareSecondaryFunc<T> func,
                                       TxnConfig config, List<T> list, Throwable throwable) {
    if (storeCtx.canRetryWithDelay()) {
      if (throwable instanceof JimException) {
        JimException exception = (JimException) throwable;
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

  private static boolean existRange(StoreCtx storeCtx, RangeRouteException routeException) {
    //maybe: table no exist, or range no exist
    RangeInfo rangeInfo = storeCtx.getRoutePolicy().getRangeInfoByKeyFromCache(routeException.key);
    if (rangeInfo != null && StringUtils.isNotBlank(rangeInfo.getLeaderAddr())) {
      return true;
    }
    return false;
  }

  public static <T> Function<Throwable, Flux> getErrHandler(StoreCtx storeCtx, DecideSecondaryFunc<T> func,
                                                            String txnId, Txn.TxnStatus status, List<T> list) {
    return throwable -> getErrHandler(storeCtx, func, txnId, status, list, throwable);
  }

  public static <T> Flux getErrHandler(StoreCtx storeCtx, DecideSecondaryFunc<T> func,
                                       String txnId, Txn.TxnStatus status, List<T> list, Throwable throwable) {
    if (storeCtx.canRetryWithDelay()) {

      if (throwable instanceof JimException) {
        JimException exception = (JimException) throwable;
        if (exception.getCode() == ErrorCode.ER_SHARD_ROUTE_CHANGE) {
          return func.apply(storeCtx, txnId, list, status);
        } else if (exception.getCode() == ErrorCode.ER_SHARD_NOT_EXIST) {
          RangeRouteException routeException = (RangeRouteException) exception;
          if (existRange(storeCtx, routeException)) {
            return func.apply(storeCtx, txnId, list, status);
          }
          return getRouteFlux(storeCtx, routeException.key).flatMap(flag -> func.apply(storeCtx, txnId, list, status));
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

  public static Function<Throwable, Flux<ExecResult>> getErrHandler(StoreCtx context, PreparePrimaryFunc func, TxnConfig config) {
    return throwable -> {
      if (context.canRetryWithDelay()) {
        if (throwable instanceof JimException) {
          JimException exception = (JimException) throwable;
          if (exception.getCode() == ErrorCode.ER_SHARD_ROUTE_CHANGE) {
            return func.apply(context, config).onErrorResume(getErrHandler(context, func, config));
          } else if (exception.getCode() == ErrorCode.ER_SHARD_NOT_EXIST) {
            RangeRouteException routeException = (RangeRouteException) exception;
            if (existRange(context, routeException)) {
              return func.apply(context, config).onErrorResume(getErrHandler(context, func, config));
            }
            return getRouteFlux(context, routeException.key).flatMap(flag -> func.apply(context, config).onErrorResume(getErrHandler(context, func, config)));
          }
        }
      }
      LOG.warn("do on err resume immediate throw err:", throwable);
      return Flux.error(throwable);
    };
  }

  public static Function<Throwable, Flux<ExecResult>> getErrHandler(StoreCtx context, DecidePrimaryFunc func, TxnConfig config, Txn.TxnStatus status) {
    return throwable -> {
      if (context.canRetryWithDelay()) {
        if (throwable instanceof JimException) {
          JimException exception = (JimException) throwable;
          if (exception.getCode() == ErrorCode.ER_SHARD_ROUTE_CHANGE) {
            return func.apply(context, config, status).onErrorResume(getErrHandler(context, func, config, status));
          } else if (exception.getCode() == ErrorCode.ER_SHARD_NOT_EXIST) {
            RangeRouteException routeException = (RangeRouteException) exception;
            if (existRange(context, routeException)) {
              return func.apply(context, config, status).onErrorResume(getErrHandler(context, func, config, status));
            }
            return getRouteFlux(context, routeException.key).flatMap(flag -> func.apply(context, config, status).onErrorResume(getErrHandler(context, func, config, status)));
          }
        }
      }
      LOG.warn("do on err resume immediate throw err:", throwable);
      return Flux.error(throwable);
    };
  }

  public static Function<Throwable, Flux<Txn.ClearupResponse>> getErrHandler(StoreCtx context, CleanUpFunc func, TxnConfig config) {
    return throwable -> {
      if (context.canRetryWithDelay()) {
        if (throwable instanceof JimException) {
          JimException exception = (JimException) throwable;
          if (exception.getCode() == ErrorCode.ER_SHARD_ROUTE_CHANGE) {
            return func.apply(context, config).onErrorResume(getErrHandler(context, func, config));
          } else if (exception.getCode() == ErrorCode.ER_SHARD_NOT_EXIST) {
            RangeRouteException routeException = (RangeRouteException) exception;
            if (existRange(context, routeException)) {
              return func.apply(context, config).onErrorResume(getErrHandler(context, func, config));
            }
            return getRouteFlux(context, routeException.key).flatMap(flag -> func.apply(context, config).onErrorResume(getErrHandler(context, func, config)));
          }
        }
      }
      LOG.warn("do on err resume immediate throw err:", throwable);
      return Flux.error(throwable);
    };
  }

  public static Function<Throwable, Flux<ExecResult>> getErrHandler(StoreCtx context, RecoverFunc func, TxnConfig config) {
    return throwable -> {
      if (context.canRetryWithDelay()) {
        if (throwable instanceof JimException) {
          JimException exception = (JimException) throwable;
          if (exception.getCode() == ErrorCode.ER_SHARD_ROUTE_CHANGE) {
            return func.apply(context, config).onErrorResume(getErrHandler(context, func, config));
          } else if (exception.getCode() == ErrorCode.ER_SHARD_NOT_EXIST) {
            RangeRouteException routeException = (RangeRouteException) exception;
            if (existRange(context, routeException)) {
              return func.apply(context, config).onErrorResume(getErrHandler(context, func, config));
            }
            return getRouteFlux(context, routeException.key).flatMap(flag -> func.apply(context, config).onErrorResume(getErrHandler(context, func, config)));
          }
        }
      }
      LOG.warn("do on err resume immediate throw err:", throwable);
      return Flux.error(throwable);
    };
  }

  public static Function<Throwable, Flux<ValueAccessor[]>> getErrHandler(StoreCtx context, SelectFunc func,
                                                                         Txn.SelectRequest.Builder reqBuilder, ColumnExpr[] exprs) {
    return throwable -> {
      if (context.canRetryWithDelay()) {
        if (throwable instanceof JimException) {
          JimException exception = (JimException) throwable;
          if (exception.getCode() == ErrorCode.ER_SHARD_ROUTE_CHANGE) {
            return func.apply(context, reqBuilder, exprs).onErrorResume(getErrHandler(context, func, reqBuilder, exprs));
          } else if (exception.getCode() == ErrorCode.ER_SHARD_NOT_EXIST) {
            RangeRouteException routeException = (RangeRouteException) exception;
            if (existRange(context, routeException)) {
              return func.apply(context, reqBuilder, exprs).onErrorResume(getErrHandler(context, func, reqBuilder, exprs));
            }
            return getRouteFlux(context, routeException.key).flatMap(flag -> func.apply(context, reqBuilder, exprs).onErrorResume(getErrHandler(context, func, reqBuilder, exprs)));
          }
        }
      }
      LOG.warn("do on err resume immediate throw err:", throwable);
      return Flux.error(throwable);
    };
  }

  public static Function<Throwable, Flux<ValueAccessor[]>> getErrHandler(StoreCtx context, SelectFlowFunc func,
                                                                         Txn.SelectFlowRequest.Builder reqBuilder, ColumnExpr[] exprs, KvPair kvPair) {
    return throwable -> {
      if (context.canRetryWithDelay()) {
        if (throwable instanceof JimException) {
          JimException exception = (JimException) throwable;
          if (exception.getCode() == ErrorCode.ER_SHARD_ROUTE_CHANGE) {
            return func.apply(context, reqBuilder, exprs, kvPair).onErrorResume(getErrHandler(context, func, reqBuilder, exprs, kvPair));
          } else if (exception.getCode() == ErrorCode.ER_SHARD_NOT_EXIST) {
            RangeRouteException routeException = (RangeRouteException) exception;
            if (existRange(context, routeException)) {
              return func.apply(context, reqBuilder, exprs, kvPair).onErrorResume(getErrHandler(context, func, reqBuilder, exprs, kvPair));
            }
            return getRouteFlux(context, routeException.key).flatMap(flag -> func.apply(context, reqBuilder, exprs, kvPair).onErrorResume(getErrHandler(context, func, reqBuilder, exprs, kvPair)));
          }
        }
      }
      LOG.warn("do on err resume immediate throw err:", throwable);
      return Flux.error(throwable);
    };
  }

  public static Function<Throwable, Flux<ValueAccessor[]>> getErrHandler(StoreCtx context, SelectFlowKeysFunc func,
                                                                         Txn.SelectFlowRequest.Builder reqBuilder,
                                                                         ColumnExpr[] exprs, List<ByteString> keys) {
    return throwable -> {
      if (context.canRetryWithDelay()) {
        if (throwable instanceof JimException) {
          JimException exception = (JimException) throwable;
          if (exception.getCode() == ErrorCode.ER_SHARD_ROUTE_CHANGE) {
            return func.apply(context, reqBuilder, exprs, keys).onErrorResume(getErrHandler(context, func, reqBuilder, exprs, keys));
          } else if (exception.getCode() == ErrorCode.ER_SHARD_NOT_EXIST) {
            RangeRouteException routeException = (RangeRouteException) exception;
            if (existRange(context, routeException)) {
              return func.apply(context, reqBuilder, exprs, keys).onErrorResume(getErrHandler(context, func, reqBuilder, exprs, keys));
            }
            return getRouteFlux(context, routeException.key).flatMap(flag -> func.apply(context, reqBuilder, exprs, keys)
                    .onErrorResume(getErrHandler(context, func, reqBuilder, exprs, keys)));
          }
        }
      }
      LOG.warn("do on err resume immediate throw err:", throwable);
      return Flux.error(throwable);
    };
  }

  public static Function<Throwable, Flux<ValueAccessor[]>> getErrHandler(StoreCtx context, SelectFlowRangeFunc func,
                                                                         Txn.SelectFlowRequest.Builder reqBuilder, ColumnExpr[] exprs, KvPair kvPair) {
    return throwable -> {
      if (context.canRetryWithDelay()) {
        if (throwable instanceof JimException) {
          JimException exception = (JimException) throwable;
          if (exception.getCode() == ErrorCode.ER_SHARD_ROUTE_CHANGE) {
            return func.apply(context, reqBuilder, exprs, kvPair).onErrorResume(getErrHandler(context, func, reqBuilder, exprs, kvPair));
          } else if (exception.getCode() == ErrorCode.ER_SHARD_NOT_EXIST) {
            RangeRouteException routeException = (RangeRouteException) exception;
            if (existRange(context, routeException)) {
              return func.apply(context, reqBuilder, exprs, kvPair).onErrorResume(getErrHandler(context, func, reqBuilder, exprs, kvPair));
            }
            return getRouteFlux(context, routeException.key).flatMap(flag -> func.apply(context, reqBuilder, exprs, kvPair).onErrorResume(getErrHandler(context, func, reqBuilder, exprs, kvPair)));
          }
        }
      }
      LOG.warn("do on err resume immediate throw err:", throwable);
      return Flux.error(throwable);
    };
  }

  public static Function<Throwable, Flux<List<ByteString>>> getErrHandler(StoreCtx context, ScanPKFunc func,
                                                                          Txn.ScanRequest.Builder reqBuilder, Index index) {
    return throwable -> {
      if (context.canRetryWithDelay()) {
        if (throwable instanceof JimException) {
          JimException exception = (JimException) throwable;
          if (exception.getCode() == ErrorCode.ER_SHARD_ROUTE_CHANGE) {
            return func.apply(context, reqBuilder, index).onErrorResume(getErrHandler(context, func, reqBuilder, index));
          } else if (exception.getCode() == ErrorCode.ER_SHARD_NOT_EXIST) {
            RangeRouteException routeException = (RangeRouteException) exception;
            if (existRange(context, routeException)) {
              return func.apply(context, reqBuilder, index).onErrorResume(getErrHandler(context, func, reqBuilder, index));
            }
            return getRouteFlux(context, routeException.key).flatMap(flag -> func.apply(context, reqBuilder, index).onErrorResume(getErrHandler(context, func, reqBuilder, index)));
          }
        }
      }
      LOG.warn("do on err resume immediate throw err:", throwable);
      return Flux.error(throwable);
    };
  }

  public static Function<Throwable, Flux<Long>> getErrHandler(StoreCtx context, ScanVerFunc func,
                                                              ByteString key, Index index) {
    return throwable -> {
      if (context.canRetryWithDelay()) {
        if (throwable instanceof JimException) {
          JimException exception = (JimException) throwable;
          if (exception.getCode() == ErrorCode.ER_SHARD_ROUTE_CHANGE) {
            return func.apply(context, key, index).onErrorResume(getErrHandler(context, func, key, index));
          } else if (exception.getCode() == ErrorCode.ER_SHARD_NOT_EXIST) {
            RangeRouteException routeException = (RangeRouteException) exception;
            if (existRange(context, routeException)) {
              return func.apply(context, key, index).onErrorResume(getErrHandler(context, func, key, index));
            }
            return getRouteFlux(context, routeException.key).flatMap(flag -> func.apply(context, key, index).onErrorResume(getErrHandler(context, func, key, index)));
          }
        }
      }
      LOG.warn("do on err resume immediate throw err:", throwable);
      return Flux.error(throwable);
    };
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

  private static Flux<Boolean> getRouteFlux(StoreCtx storeCtx, byte[] key) {
    return Flux.create(sink -> {
      Callback callback = new Callback() {
        @Override
        public void success(boolean value) {
          sink.next(value);
        }

        @Override
        public void failed(Throwable cause) {
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

  public static void recoverFromPrimary(FluxSink<ExecResult> sink, TxnConfig config, StoreCtx context) {
    RECOVER_FUNC.apply(context, config).onErrorResume(
            TxnHandler.getErrHandler(context, RECOVER_FUNC, config)).subscribe(
            new CommitSubscriber<>(rs -> sink.next(rs), err -> sink.error(err)));
  }

  private static Flux<ExecResult> recover(StoreCtx context, TxnConfig config) {
    Txn.DecideRequest.Builder reqBuilder = Util.buildTxnDecide4Primary(config, Txn.TxnStatus.ABORTED);
    return TxnHandler.recoverFromPrimary(context, reqBuilder, config.getSecKeys()).map(f -> AckExecResult.getInstance());
  }

  public static JimException convertTxnErr(Txn.TxnError txError) {
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

  /**
   * Txn commit subscriber
   *
   * @param <T>
   * @version V1.0
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
