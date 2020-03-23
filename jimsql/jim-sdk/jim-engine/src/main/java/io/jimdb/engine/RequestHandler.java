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

import static io.jimdb.engine.ErrorHandler.retrieveException;

import java.util.ArrayList;
import java.util.List;

import io.jimdb.common.exception.BaseException;
import io.jimdb.common.utils.lang.ByteUtil;
import io.jimdb.core.codec.Codec;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.RowValueAccessor;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.model.meta.RangeInfo;
import io.jimdb.core.model.result.impl.QueryExecResult;
import io.jimdb.engine.client.RequestContext;
import io.jimdb.engine.txn.Restorer;
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
public class RequestHandler {
  protected static final RowValueAccessor[] ROW_VALUE_ACCESSORS_EMPTY = new RowValueAccessor[0];
  protected static final ValueAccessor[][] ROW_VALUE_ACCESSORS_ARRAY_EMPTY = {QueryExecResult.EMPTY_ROW};
  protected static final boolean GATHER_TRACE = false;
  private static final Logger LOGGER = LoggerFactory.getLogger(RequestHandler.class);

  /**
   * TODO
   *
   * @param <Resp> TODO
   * @param <Req>  TODO
   */
  @FunctionalInterface
  interface RequestFunc<Resp, Req> {
    Flux<Resp> apply(ShardSender shardSender, StoreCtx storeCtx, Req reqBuilder, ByteString key, RangeInfo rangeInfo);
  }

  protected static ValueAccessor[] convertRowsToValueAccessors(ColumnExpr[] resultColumns, List<Txn.Row> rows, StoreCtx storeCtx) {
    if (rows == null || rows.isEmpty()) {
      return ROW_VALUE_ACCESSORS_EMPTY;
    }
    List<ValueAccessor> decodeList = new ArrayList<>();
    for (Txn.Row row : rows) {
      if (row != null && row.getValue() != null && !row.getValue().getFields().isEmpty()) {
        Txn.RowValue rowValue = row.getValue();
        if (LOGGER.isInfoEnabled()) {
          LOGGER.info("decode row: {}", ByteUtil.bytes2hex01(NettyByteString.asByteArray(rowValue.getFields())));
        }
        decodeList.add(Codec.decodeRowWithOpt(resultColumns, rowValue, row.getPks(), storeCtx.getTimeZone()));
      }
    }
    return decodeList.toArray(new ValueAccessor[decodeList.size()]);
  }

  public static Flux<Txn.TxnStatus> getTxnStatus(ShardSender shardSender, StoreCtx storeCtx, String txnId, ByteString primaryKey, boolean txnTimeout) {
    if (txnTimeout) {
      if (LOGGER.isInfoEnabled()) {
        LOGGER.info("found timed out txn, need to confirm txn[{}] status", txnId);
      }
      return Restorer.recoverFromSecondaryAsync(shardSender, storeCtx, txnId, primaryKey);
    }

    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("could not find timed out txn, need to confirm txn[{}] status", txnId);
    }
    Txn.GetLockInfoRequest.Builder request = Txn.GetLockInfoRequest.newBuilder().setTxnId(txnId).setKey(primaryKey);

    RangeInfo rangeInfo = storeCtx.getRoutingPolicy().getRangeInfoByKey(request.getKey().toByteArray());
    RequestContext context = new RequestContext(storeCtx, request.getKey(), rangeInfo, request, Api.RangeRequest.ReqCase.GET_LOCK_INFO);
    return shardSender.sendReq(context).map(response -> (Txn.GetLockInfoResponse) response).map(response -> {
      if (response.hasErr()) {
        if (response.getErr().getErrType() == Txn.TxnError.ErrType.NOT_FOUND) {
          return Txn.TxnStatus.TXN_INIT;
        }
        BaseException err = retrieveException(response.getErr());
        LOGGER.error("get txn  {} lock info error:{}", txnId, err.getMessage());
        throw err;
      }
      return response.getInfo().getStatus();
    });

  }
}
