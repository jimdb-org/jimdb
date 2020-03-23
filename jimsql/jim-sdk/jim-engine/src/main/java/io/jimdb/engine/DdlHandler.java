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

import static io.jimdb.engine.RangeSelector.SINGLE_RANGE_SELECT_FUNC;
import static io.jimdb.engine.ScanHandler.txnScanForExist;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.jimdb.common.exception.BaseException;
import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.core.Session;
import io.jimdb.core.codec.Codec;
import io.jimdb.core.codec.KvPair;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.model.meta.Column;
import io.jimdb.core.model.meta.Index;
import io.jimdb.core.model.meta.Table;
import io.jimdb.core.plugin.store.Transaction;
import io.jimdb.core.values.LongValue;
import io.jimdb.core.values.Value;
import io.jimdb.meta.Router;
import io.jimdb.pb.Exprpb;
import io.jimdb.pb.Kv;
import io.jimdb.pb.Processorpb;
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
public class DdlHandler extends RequestHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(DdlHandler.class);

  public static byte[] addIndex(ShardSender shardSender, Router router, Session session, Index index, byte[] startKey, byte[] endKey, int limit) {
    ColumnExpr[] columns = buildIndexColumns(index);
    ByteString lastKey = NettyByteString.wrap(endKey);
    Txn.SelectFlowRequest.Builder selectBuilder = buildIndexSelect(columns, NettyByteString.wrap(startKey), lastKey, limit);
    ValueAccessor[] values = singleRangeSelect(shardSender, router, session, index.getTable(), selectBuilder, columns).blockFirst();
    if (values != null && values.length > 0) {
      doAddIndex(shardSender, router, session, index, columns, values);
      lastKey = selectBuilder.getProcessors(0).getTableRead().getRange().getEndKey();
    }

    lastKey = Codec.nextKey(lastKey);
    return lastKey == null ? endKey : NettyByteString.asByteArray(lastKey);
  }

  private static Flux<ValueAccessor[]> singleRangeSelect(ShardSender shardSender, Router router, Session session,
                                                         Table table, Txn.SelectFlowRequest.Builder builder, ColumnExpr[] resultColumns) throws BaseException {
    StoreCtx storeCtx = StoreCtx.buildCtx(session, table, router);
    Kv.KeyRange keyRange = builder.getProcessors(0).getTableRead().getRange();
    KvPair kvPair = new KvPair(keyRange.getStartKey(), keyRange.getEndKey());
    return SINGLE_RANGE_SELECT_FUNC.apply(shardSender, storeCtx, builder, resultColumns, kvPair)
               .onErrorResume(RangeSelector.getErrHandler(shardSender, storeCtx, SINGLE_RANGE_SELECT_FUNC, builder, resultColumns, kvPair));
  }

  private static ColumnExpr[] buildIndexColumns(Index index) {
    final Column[] pkColumns = index.getTable().getPrimary();
    final Column[] idxColumns = index.getColumns();
    final ColumnExpr[] exprs = new ColumnExpr[pkColumns.length + idxColumns.length];

    int i = 0;
    for (Column column : pkColumns) {
      exprs[i++] = new ColumnExpr(Long.valueOf(i), column);
    }
    for (Column column : idxColumns) {
      exprs[i++] = new ColumnExpr(Long.valueOf(i), column);
    }
    return exprs;
  }

  private static Txn.SelectFlowRequest.Builder buildIndexSelect(ColumnExpr[] columns, ByteString startKey, ByteString endKey, int limit) {
    List<Exprpb.ColumnInfo> selCols = new ArrayList<>(columns.length);
    for (ColumnExpr column : columns) {
      Exprpb.ColumnInfo.Builder columnInfo = Exprpb.ColumnInfo.newBuilder()
                                                 .setId(column.getId().intValue())
                                                 .setTyp(column.getResultType().getType())
                                                 .setUnsigned(column.getResultType().getUnsigned());
      if (column.getReorgValue() != null) {
        columnInfo.setReorgValue(column.getReorgValue());
      }
      selCols.add(columnInfo.build());
    }

    List<Integer> outputOffsetList = new ArrayList<>(columns.length);
    for (int i = 0; i < columns.length; i++) {
      outputOffsetList.add(i);
    }

    Processorpb.Processor.Builder tableReader = Processorpb.Processor.newBuilder()
                                                    .setType(Processorpb.ProcessorType.TABLE_READ_TYPE)
                                                    .setTableRead(Processorpb.TableRead.newBuilder()
                                                                      .setType(Processorpb.KeyType.KEYS_RANGE_TYPE)
                                                                      .addAllColumns(selCols)
                                                                      .setRange(Kv.KeyRange.newBuilder().setStartKey(startKey).setEndKey(endKey))
                                                    );
    Processorpb.Processor.Builder limitReader = Processorpb.Processor.newBuilder()
                                                    .setType(Processorpb.ProcessorType.LIMIT_TYPE)
                                                    .setLimit(Processorpb.Limit.newBuilder()
                                                                  .setCount(limit));

    return Txn.SelectFlowRequest.newBuilder()
               .addProcessors(tableReader)
               .addProcessors(limitReader)
               .addAllOutputOffsets(outputOffsetList)
               .setGatherTrace(GATHER_TRACE);
  }

  private static void doAddIndex(ShardSender shardSender, Router router, Session session, Index index, ColumnExpr[] columnExprs, ValueAccessor[] values) {
    Table table = index.getTable();
    Transaction transaction = session.getTxn();
    //Map<Integer, Integer> colMap = getMapping(columnExprs);

    //TODO move this context to the caller
    StoreCtx storeCtx = StoreCtx.buildCtx(session, table, router);
    Map<ByteString, Boolean> indexDulpMap = new HashMap<>(values.length);

    for (ValueAccessor value : values) {
      Value[] partOldValue = value.extractValues(table.getWritableColumns(), columnExprs);
      ByteString rowKey = Codec.encodeKey(table.getId(), table.getPrimary(), partOldValue);
      KvPair rowKvPair = new KvPair(rowKey);
      KvPair idxKvPair = Codec.encodeIndexKV(index, partOldValue, rowKey);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("[addIndex]txn {} encode index [{}]", transaction.getTxnId(), idxKvPair);
      }

      Boolean flag = indexDulpMap.get(idxKvPair.getKey());
      if (flag != null && flag.booleanValue()) {
        LOGGER.error("add index {}.{} duplicate, {}", table.getName(), index.getName(),
            Arrays.toString(idxKvPair.getKey().toByteArray()));
        throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_DUP_ENTRY, Arrays.toString(idxKvPair.getKey().toByteArray()), "INDEX");
      } else {
        indexDulpMap.put(idxKvPair.getKey(), Boolean.TRUE);
      }
      Boolean exist = txnScanForExist(shardSender, storeCtx, idxKvPair, index).blockFirst();
      if (exist != null && exist.booleanValue()) {
        continue;
      }

      LongValue recordVersion = (LongValue) value.get(columnExprs.length);
      // add lock to row
      transaction.addIntent(rowKvPair, Txn.OpType.LOCK, false, recordVersion.getValue(), table);

      // add lock to index
      transaction.addIntent(idxKvPair, Txn.OpType.INSERT, true, 0, table);
    }
  }


}
