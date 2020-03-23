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

import static io.jimdb.engine.ScanHandler.SCAN_VER_FUNC;

import io.jimdb.common.exception.BaseException;
import io.jimdb.core.Session;
import io.jimdb.core.codec.Codec;
import io.jimdb.core.codec.KvPair;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.model.meta.Index;
import io.jimdb.core.model.meta.Table;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.model.result.QueryResult;
import io.jimdb.core.model.result.impl.DMLExecResult;
import io.jimdb.core.plugin.store.Transaction;
import io.jimdb.core.values.LongValue;
import io.jimdb.core.values.Value;
import io.jimdb.meta.Router;
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
public class DeleteHandler extends RequestHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(DeleteHandler.class);

  public static Flux<ExecResult> delete(ShardSender shardSender, Router router, Session session, Table table, QueryResult rows) throws BaseException {
    if (rows == null || rows.size() == 0) {
      return Flux.just(DMLExecResult.EMPTY);
    }

    int resultRowLength = rows.size();
    StoreCtx storeCtx = StoreCtx.buildCtx(session, table, router);
    ColumnExpr[] selectColumns = rows.getColumns();
    Index[] indexes = table.getDeletableIndices();
    //Map<Integer, Integer> colMap = getMapping(selectColumns);
    Flux<Long>[] versionFlux = new Flux[1];

    rows.forEach(row -> {
      LongValue recordVersion = (LongValue) row.get(selectColumns.length);
      Value[] partOldValue = row.extractValues(table.getWritableColumns(), selectColumns);
      versionFlux[0] = deleteRow(shardSender, session.getTxn(), table, storeCtx, indexes, versionFlux[0], recordVersion, partOldValue);
    });

    ExecResult result = new DMLExecResult(resultRowLength);
    if (versionFlux[0] != null) {
      return versionFlux[0].map(v -> result);
    }
    return Flux.create(sink -> sink.next(result));
  }

  private static Flux<Long> deleteRow(ShardSender shardSender, Transaction transaction, Table table, StoreCtx storeCtx, Index[] indexes, Flux<Long> versionFlux,
                                      LongValue recordVersion, Value[] partOldValue) {
    ByteString recordKey = Codec.encodeKey(table.getId(), table.getPrimary(), partOldValue);

    for (Index index : indexes) {
      ByteString oldIdxKey = Codec.encodeIndexKey(index, partOldValue, recordKey, true);
      KvPair oldKvPair = new KvPair(oldIdxKey);

      if (index.isPrimary()) {
        transaction.addIntent(oldKvPair, Txn.OpType.DELETE, false, recordVersion.getValue(), table);
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("[delete]txn {} encode old index:[{}], version:{} ", transaction.getTxnId(),
              oldIdxKey, recordVersion.getValue());
        }
        continue;
      }

      //scan version
      Flux<Long> txnScanFlux = SCAN_VER_FUNC.apply(shardSender, storeCtx, oldIdxKey, index)
                                   .onErrorResume(ScanHandler.getErrHandler(shardSender, storeCtx, SCAN_VER_FUNC, oldIdxKey, index))
                                   .map(v -> onErr(transaction, table, oldIdxKey, oldKvPair, v));

      if (versionFlux == null) {
        versionFlux = txnScanFlux;
      } else {
        versionFlux = versionFlux.flatMap(v -> txnScanFlux);
      }
    }
    return versionFlux;
  }

  private static Long onErr(Transaction transaction, Table table, ByteString oldIdxKey, KvPair oldKvPair, Long v) {
    transaction.addIntent(oldKvPair, Txn.OpType.DELETE, false, v, table);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("[delete]txn {} encode old index:[{}], version:[{}]", transaction.getTxnId(),
          oldIdxKey, v);
    }
    return v;
  }


}
